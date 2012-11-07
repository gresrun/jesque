/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.greghaines.jesque.worker;

import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.DATE_FORMAT;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.JAVA_DYNAMIC_QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.STARTED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;
import static net.greghaines.jesque.utils.ResqueConstants.WORKER;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;
import static net.greghaines.jesque.worker.JobExecutor.State.NEW;
import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.JobExecutor.State.SHUTDOWN;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_EXECUTE;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_FAILURE;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_PROCESS;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_SUCCESS;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_ERROR;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_START;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_STOP;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.VersionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Basic implementation of the Worker interface.
 * Obeys the contract of a Resque worker in Redis.
 * 
 * @author Greg Haines
 */
public class WorkerImpl implements Worker
{
	private static final Logger log = LoggerFactory.getLogger(WorkerImpl.class);
	private static final AtomicLong workerCounter = new AtomicLong(0);
	protected static final long emptyQueueSleepTime = 500; // 500 ms
	private static final long reconnectSleepTime = 5000; // 5s
	private static final int reconnectAttempts = 120; // Total time: 10min
	private static volatile boolean threadNameChangingEnabled = false; // set the thread name to the message for debugging

	/**
	 * @return true if worker threads names will change during normal operation
	 */
	public static boolean isThreadNameChangingEnabled()
	{
		return threadNameChangingEnabled;
	}

	/**
	 * Enable/disable worker thread renaming during normal operation. (Disabled by default)
	 * <p/>
	 * <strong>Warning: Enabling this feature is very expensive CPU-wise!</strong><br/>
	 * This feature is designed to assist in debugging worker state but should be 
	 * disabled in production environments for performance reasons.
	 * 
	 * @param enabled whether threads' names should change during normal operation
	 */
	public static void setThreadNameChangingEnabled(final boolean enabled)
	{
		threadNameChangingEnabled = enabled;
	}
	
	/**
	 * Verify that the given queues are all valid.
	 * 
	 * @param queues the given queues
	 */
	protected static void checkQueues(final Iterable<String> queues)
	{
		if (queues == null)
		{
			throw new IllegalArgumentException("queues must not be null");
		}
		for (final String queue : queues)
		{
			if (queue == null || "".equals(queue))
			{
				throw new IllegalArgumentException("queues' members must not be null: " + queues);
			}
		}
	}

	protected final Jedis jedis;
	protected final String namespace;
	protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<String>();
	private final ConcurrentMap<String,Class<?>> jobTypes = new ConcurrentHashMap<String,Class<?>>();
	private final String name;
	protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
	protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
	private final AtomicBoolean paused = new AtomicBoolean(false);
	private final long workerId = workerCounter.getAndIncrement();
	private final String threadNameBase =
		"Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
	private final AtomicReference<Thread> threadRef =
		new AtomicReference<Thread>(null);
	private final AtomicReference<ExceptionHandler> exceptionHandlerRef = 
		new AtomicReference<ExceptionHandler>(new DefaultExceptionHandler());

	/**
	 * Creates a new WorkerImpl, which creates it's own connection to 
	 * Redis using values from the config. The worker will only listen 
	 * to the supplied queues and only execute jobs that are in the 
	 * supplied job types.
	 * 
	 * @param config used to create a connection to Redis and the package 
	 * prefix for incoming jobs
	 * @param queues the list of queues to poll
	 * @param jobTypes the map of job names and types to execute
	 * @throws IllegalArgumentException if the config is null, 
	 * if the queues is null, or if the jobTypes is null or empty
	 */
	public WorkerImpl(final Config config, final Collection<String> queues, 
			final Map<String,? extends Class<?>> jobTypes)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		checkQueues(queues);
		checkJobTypes(jobTypes);
		this.namespace = config.getNamespace();
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getPassword() != null)
		{
			this.jedis.auth(config.getPassword());
		}
		this.jedis.select(config.getDatabase());
		this.name = createName();
		setQueues(queues);
		setJobTypes(jobTypes);
	}

	/**
	 * @return this worker's identifier
	 */
	public long getWorkerId()
	{
		return this.workerId;
	}

	/**
	 * Starts this worker.
	 * Registers the worker in Redis and begins polling the queues for jobs.
	 * Stop this worker by calling end() on any thread.
	 */
	public void run()
	{
		if (this.state.compareAndSet(NEW, RUNNING))
		{
			try
			{
				renameThread("RUNNING");
				this.threadRef.set(Thread.currentThread());
				this.jedis.sadd(key(WORKERS), this.name);
				this.jedis.set(key(WORKER, this.name, STARTED), 
					new SimpleDateFormat(DATE_FORMAT).format(new Date()));
				this.listenerDelegate.fireEvent(WORKER_START, 
					this, null, null, null, null, null);
				poll();
			}
			finally
			{
				renameThread("STOPPING");
				this.listenerDelegate.fireEvent(WORKER_STOP, 
					this, null, null, null, null, null);
				this.jedis.srem(key(WORKERS), this.name);
				this.jedis.del(
					key(WORKER, this.name), 
					key(WORKER, this.name, STARTED), 
					key(STAT, FAILED, this.name), 
					key(STAT, PROCESSED, this.name));
				this.jedis.quit();
				this.threadRef.set(null);
			}
		}
		else
		{
			if (RUNNING.equals(this.state.get()))
			{
				throw new IllegalStateException("This WorkerImpl is already running");
			}
			else
			{
				throw new IllegalStateException("This WorkerImpl is shutdown");
			}
		}
	}

	/**
	 * Shutdown this Worker.<br/>
	 * <b>The worker cannot be started again; create a new worker in this case.</b>
	 * 
	 * @param now if true, an effort will be made to stop any job in progress
	 */
	public void end(final boolean now)
	{
		this.state.set(SHUTDOWN);
		if (now)
		{
			final Thread workerThread = this.threadRef.get();
			if (workerThread != null)
			{
				workerThread.interrupt();
			}
		}
		togglePause(false); // Release any threads waiting in checkPaused()
	}

	public boolean isShutdown()
	{
		return SHUTDOWN.equals(this.state.get());
	}

	public boolean isPaused()
	{
		return this.paused.get();
	}

	public void togglePause(final boolean paused)
	{
		this.paused.set(paused);
		synchronized (this.paused)
		{
			this.paused.notifyAll();
		}
	}

	public String getName()
	{
		return this.name;
	}

	public void addListener(final WorkerListener listener)
	{
		this.listenerDelegate.addListener(listener);
	}

	public void addListener(final WorkerListener listener, final WorkerEvent... events)
	{
		this.listenerDelegate.addListener(listener, events);
	}

	public void removeListener(final WorkerListener listener)
	{
		this.listenerDelegate.removeListener(listener);
	}

	public void removeListener(final WorkerListener listener, final WorkerEvent... events)
	{
		this.listenerDelegate.removeListener(listener, events);
	}

	public void removeAllListeners()
	{
		this.listenerDelegate.removeAllListeners();
	}

	public void removeAllListeners(final WorkerEvent... events)
	{
		this.listenerDelegate.removeAllListeners(events);
	}

	public Collection<String> getQueues()
	{
		return Collections.unmodifiableCollection(this.queueNames);
	}

	public void addQueue(final String queueName)
	{
		if (queueName == null || "".equals(queueName))
		{
			throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
		}
		this.queueNames.add(queueName);
	}

	public void removeQueue(final String queueName, final boolean all)
	{
		if (queueName == null || "".equals(queueName))
		{
			throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
		}
		if (all)
		{ // Remove all instances
			boolean tryAgain = true;
			while (tryAgain)
			{
				tryAgain = this.queueNames.remove(queueName);
			}
		}
		else
		{ // Only remove one instance
			this.queueNames.remove(queueName);
		}
	}

	public void removeAllQueues()
	{
		this.queueNames.clear();
	}

	public void setQueues(final Collection<String> queues)
	{
		checkQueues(queues);
		this.queueNames.clear();
		this.queueNames.addAll((queues == ALL_QUEUES) // Using object equality on purpose
			? this.jedis.smembers(key(QUEUES)) // Like '*' in other clients
			: queues);
	}

	public Map<String,Class<?>> getJobTypes()
	{
		return Collections.unmodifiableMap(this.jobTypes);
	}

	public void addJobType(final String jobName, final Class<?> jobType)
	{
		if (jobName == null)
		{
			throw new IllegalArgumentException("jobName must not be null");
		}
		if (jobType == null)
		{
			throw new IllegalArgumentException("jobType must not be null");
		}
		if (!(Runnable.class.isAssignableFrom(jobType)) && !(Callable.class.isAssignableFrom(jobType)))
		{
			throw new IllegalArgumentException("jobType must implement either Runnable or Callable: " + jobType);
		}
		this.jobTypes.put(jobName, jobType);
	}

	public void removeJobType(final Class<?> jobType)
	{
		if (jobType == null)
		{
			throw new IllegalArgumentException("jobType must not be null");
		}
		this.jobTypes.values().remove(jobType);
	}
	
	public void removeJobName(final String jobName)
	{
		if (jobName == null)
		{
			throw new IllegalArgumentException("jobName must not be null");
		}
		this.jobTypes.remove(jobName);
	}

	public void setJobTypes(final Map<String,? extends Class<?>> jobTypes)
	{
		checkJobTypes(jobTypes);
		this.jobTypes.clear();
		for (final Entry<String,? extends Class<?>> entry : jobTypes.entrySet())
		{
			addJobType(entry.getKey(), entry.getValue());
		}
	}

	public ExceptionHandler getExceptionHandler()
	{
		return this.exceptionHandlerRef.get();
	}

	public void setExceptionHandler(final ExceptionHandler exceptionHandler)
	{
		if (exceptionHandler == null)
		{
			throw new IllegalArgumentException("exceptionHandler must not be null");
		}
		this.exceptionHandlerRef.set(exceptionHandler);
	}
	
	public void join(final long millis)
	throws InterruptedException
	{
		final Thread workerThread = this.threadRef.get();
		if (workerThread != null && workerThread.isAlive())
		{
			workerThread.join(millis);
		}
	}

	/**
	 * @return the number of times this Worker will attempt to reconnect to Redis before giving up
	 */
	protected int getReconnectAttempts()
	{
		return reconnectAttempts;
	}

	/**
	 * Polls the queues for jobs and executes them.
	 */
	protected void poll()
	{
		int missCount = 0;
		String curQueue = null;
		while (RUNNING.equals(this.state.get()))
		{
			try
			{
				if (threadNameChangingEnabled)
				{
					renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
				}
				curQueue = this.queueNames.poll(emptyQueueSleepTime, TimeUnit.MILLISECONDS);
				if (curQueue != null)
				{
					this.queueNames.add(curQueue); // Rotate the queues
					checkPaused();
					if (RUNNING.equals(this.state.get())) // Might have been waiting in poll()/checkPaused() for a while
					{
						this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
						final String payload = this.jedis.lpop(key(QUEUE, curQueue));
						if (payload != null)
						{
							final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
							process(job, curQueue);
							missCount = 0;
						}
						else if (++missCount >= this.queueNames.size() && RUNNING.equals(this.state.get()))
						{ // Keeps worker from busy-spinning on empty queues
							missCount = 0;
							Thread.sleep(emptyQueueSleepTime);
						}
					}
				}
			}
			catch (InterruptedException ie)
			{
				if (!isShutdown())
				{
					recoverFromException(curQueue, ie);
				}
			}
			catch (Exception e)
			{
				recoverFromException(curQueue, e);
			}
		}
	}

	/**
	 * Handle an exception that was thrown from inside {@link #poll()}
	 * 
	 * @param curQueue the name of the queue that was being processed when the exception was thrown
	 * @param e the exception that was thrown
	 */
	protected void recoverFromException(final String curQueue, final Exception e)
	{
		final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, e, curQueue);
		switch (recoveryStrategy)
		{
		case RECONNECT:
			log.info("Reconnecting to Redis in response to exception", e);
			final int reconAttempts = getReconnectAttempts();
			if (!JedisUtils.reconnect(this.jedis, reconAttempts, reconnectSleepTime))
			{
				log.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", e);
				end(false);
			}
			else
			{
				log.info("Reconnected to Redis");
			}
			break;
		case TERMINATE:
			log.warn("Terminating in response to exception", e);
			end(false);
			break;
		case PROCEED:
			this.listenerDelegate.fireEvent(WORKER_ERROR, this, curQueue, null, null, null, e);
			break;
		default:
			log.error("Unknown RecoveryStrategy: " + recoveryStrategy + 
				" while attempting to recover from the following exception; worker proceeding...", e);
			break;
		}
	}

	/**
	 * Checks to see if worker is paused. If so, wait until unpaused.
	 * 
	 * @throws IOException if there was an error creating the pause message
	 */
	protected void checkPaused()
	throws IOException
	{
		if (this.paused.get())
		{
			synchronized (this.paused)
			{
				if (this.paused.get())
				{
					this.jedis.set(key(WORKER, this.name), pauseMsg());
				}
				while (this.paused.get())
				{
					try { this.paused.wait(); } catch (InterruptedException ie){}
				}
				this.jedis.del(key(WORKER, this.name));
			}
		}
	}

	/**
	 * Materializes and executes the given job.
	 * 
	 * @param job the Job to process
	 * @param curQueue the queue the payload came from
	 */
	protected void process(final Job job, final String curQueue)
	{
		this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null);
		if (threadNameChangingEnabled)
		{
			renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
		}
		try
		{
			execute(job, curQueue, JesqueUtils.materializeJob(job, this.jobTypes));
		}
		catch (Exception e)
		{
			failure(e, job, curQueue);
		}
	}

	/**
	 * Executes the given job.
	 * 
	 * @param job the job to execute
	 * @param curQueue the queue the job came from 
	 * @param instance the materialized job
	 * @throws Exception if the instance is a {@link Callable} and throws an exception
	 */
	protected void execute(final Job job, final String curQueue, final Object instance)
	throws Exception
	{
		this.jedis.set(key(WORKER, this.name), statusMsg(curQueue, job));
		try
		{
			final Object result;
			if (instance instanceof WorkerAware)
			{
				((WorkerAware) instance).setWorker(this);
			}
			this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null);
			if (instance instanceof Callable)
			{
				result = ((Callable<?>) instance).call(); // The job is executing!
			}
			else if (instance instanceof Runnable)
			{
				((Runnable) instance).run(); // The job is executing!
				result = null;
			}
			else
			{ // Should never happen since we're testing the class earlier
				throw new ClassCastException("instance must be a Runnable or a Callable: " + 
					instance.getClass().getName() + " - " + instance);
			}
			success(job, instance, result, curQueue);
		}
		finally
		{
			this.jedis.del(key(WORKER, this.name));
		}
	}

	/**
	 * Update the status in Redis on success.
	 * 
	 * @param job the Job that succeeded
	 * @param runner the materialized Job
	 * @param curQueue the queue the Job came from
	 */
	protected void success(final Job job, final Object runner, final Object result, final String curQueue)
	{
		this.jedis.incr(key(STAT, PROCESSED));
		this.jedis.incr(key(STAT, PROCESSED, this.name));
		this.listenerDelegate.fireEvent(JOB_SUCCESS, this, curQueue, job, runner, result, null);
	}

	/**
	 * Update the status in Redis on failure
	 * 
	 * @param ex the Exception that occured
	 * @param job the Job that failed
	 * @param curQueue the queue the Job came from
	 */
	protected void failure(final Exception ex, final Job job, final String curQueue)
	{
		this.jedis.incr(key(STAT, FAILED));
		this.jedis.incr(key(STAT, FAILED, this.name));
		try
		{
			this.jedis.rpush(key(FAILED), failMsg(ex, curQueue, job));
		}
		catch (Exception e)
		{
			log.warn("Error during serialization of failure payload for exception=" + ex + " job=" + job, e);
		}
		this.listenerDelegate.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, ex);
	}

	/**
	 * Create and serialize a JobFailure.
	 * 
	 * @param ex the Exception that occured
	 * @param queue the queue the job came from
	 * @param job the Job that failed
	 * @return the JSON representation of a new JobFailure
	 * @throws IOException if there was an error serializing the JobFailure
	 */
	protected String failMsg(final Exception ex, final String queue, final Job job)
	throws IOException
	{
		final JobFailure f = new JobFailure();
		f.setFailedAt(new Date());
		f.setWorker(this.name);
		f.setQueue(queue);
		f.setPayload(job);
		f.setException(ex);
		return ObjectMapperFactory.get().writeValueAsString(f);
	}

	/**
	 * Create and serialize a WorkerStatus.
	 * 
	 * @param queue the queue the Job came from 
	 * @param job the Job currently being processed
	 * @return the JSON representation of a new WorkerStatus
	 * @throws IOException if there was an error serializing the WorkerStatus
	 */
	protected String statusMsg(final String queue, final Job job)
	throws IOException
	{
		final WorkerStatus s = new WorkerStatus();
		s.setRunAt(new Date());
		s.setQueue(queue);
		s.setPayload(job);
		return ObjectMapperFactory.get().writeValueAsString(s);
	}

	/**
	 * Create and serialize a WorkerStatus for a pause event.
	 * 
	 * @return the JSON representation of a new WorkerStatus
	 * @throws IOException if there was an error serializing the WorkerStatus
	 */
	protected String pauseMsg()
	throws IOException
	{
		final WorkerStatus s = new WorkerStatus();
		s.setRunAt(new Date());
		s.setPaused(isPaused());
		return ObjectMapperFactory.get().writeValueAsString(s);
	}

	/**
	 * Creates a unique name, suitable for use with Resque.
	 * 
	 * @return a unique name for this worker
	 */
	protected String createName()
	{
		final StringBuilder sb = new StringBuilder(128);
		try
		{
			sb.append(InetAddress.getLocalHost().getHostName()).append(COLON)
				.append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
				.append('-').append(this.workerId).append(COLON).append(JAVA_DYNAMIC_QUEUES);
			for (final String queueName : this.queueNames)
			{
				sb.append(',').append(queueName);
			}
		}
		catch (UnknownHostException uhe)
		{
			throw new RuntimeException(uhe);
		}
		return sb.toString();
	}

	/**
	 * Builds a namespaced Redis key with the given arguments.
	 * 
	 * @param parts the key parts to be joined
	 * @return an assembled String key
	 */
	protected String key(final String... parts)
	{
		return JesqueUtils.createKey(this.namespace, parts);
	}

	/**
	 * Rename the current thread with the given message.
	 * 
	 * @param msg the message to add to the thread name
	 */
	protected void renameThread(final String msg)
	{
		Thread.currentThread().setName(this.threadNameBase + msg);
	}

	/**
	 * Verify the given job types are all valid.
	 * 
	 * @param jobTypes the given job types
	 */
	protected void checkJobTypes(final Map<String,? extends Class<?>> jobTypes)
	{
		if (jobTypes == null)
		{
			throw new IllegalArgumentException("jobTypes must not be null");
		}
		for (final Entry<String,? extends Class<?>> entry : jobTypes.entrySet())
		{
			if (entry.getKey() == null)
			{
				throw new IllegalArgumentException("jobType's keys must not be null: " + jobTypes);
			}
			final Class<?> jobType = entry.getValue();
			if (jobType == null)
			{
				throw new IllegalArgumentException("jobType's values must not be null: " + jobTypes);
			}
			if (!(Runnable.class.isAssignableFrom(jobType)) && !(Callable.class.isAssignableFrom(jobType)))
			{
				throw new IllegalArgumentException("jobType's values must implement either Runnable or Callable: " + jobTypes);
			}
		}
	}

	@Override
	public String toString()
	{
		return this.namespace + COLON + WORKER + COLON + this.name;
	}
}
