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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.ResqueConstants;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ReflectionUtils;
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
public class WorkerImpl implements Worker, ResqueConstants
{
	private static final Logger log = LoggerFactory.getLogger(WorkerImpl.class);
	private static final AtomicLong workerCounter = new AtomicLong(0);
	private static final long emptyQueueSleepTime = 500; // 500 ms
	
	/**
	 * Verify that the given queues are all valid.
	 * 
	 * @param queues the given queues
	 */
	private static void checkQueues(final Iterable<String> queues)
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
	
	private final Jedis jedis;
	private final String namespace;
	private final String jobPackage;
	private final BlockingDeque<String> queueNames;
	private final Set<Class<?>> jobTypes;
	private final String name;
	private final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final AtomicBoolean paused = new AtomicBoolean(false);
	private final long workerId = workerCounter.getAndIncrement();
	private final String threadNameBase = "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
	private final AtomicReference<Thread> workerThreadRef = new AtomicReference<Thread>(null);
	
	/**
	 * Creates a new WorkerImpl, which creates it's own connection to Redis using values from the config.
	 * The worker will only listen to the supplied queues and only execute jobs that are in the supplied job types.
	 * 
	 * @param config used to create a connection to Redis and the package prefix for incoming jobs
	 * @param queues the list of queues to poll
	 * @param jobTypes the list of job types to execute
	 * @throws IllegalArgumentException if the config is null, if the queues is null, or if the jobTypes is null or empty
	 */
	public WorkerImpl(final Config config, final Collection<String> queues, final Collection<? extends Class<?>> jobTypes)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		checkQueues(queues);
		if (jobTypes == null || jobTypes.isEmpty())
		{
			throw new IllegalArgumentException("jobTypes must not be null or empty: " + jobTypes);
		}
		for (final Class<?> jobType : jobTypes)
		{
			if (jobType == null)
			{
				throw new IllegalArgumentException("jobType's members must not be null: " + jobTypes);
			}
		}
		this.namespace = config.getNamespace();
		this.jobPackage = config.getJobPackage();
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		this.jedis.select(config.getDatabase());
		this.queueNames = new LinkedBlockingDeque<String>((queues == ALL_QUEUES) // Using object equality on purpose
				? this.jedis.smembers(key(QUEUES)) // Like '*' in other clients
				: queues);
		this.jobTypes = new HashSet<Class<?>>(jobTypes);
		this.name = createName();
	}
	
	/**
	 * Starts this worker.
	 * Registers the worker in Redis and begins polling the queues for jobs.
	 * Stop this worker by calling end() on any thread.
	 */
	public void run()
	{
		if (this.running.compareAndSet(false, true))
		{
			try
			{
				this.workerThreadRef.set(Thread.currentThread());
				this.jedis.sadd(key(WORKERS), this.name);
				this.jedis.set(key(WORKER, this.name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
				this.listenerDelegate.fireEvent(WorkerEvent.WORKER_START, this, null, null, null, null, null);
				poll();
			}
			finally
			{
				this.listenerDelegate.fireEvent(WorkerEvent.WORKER_STOP, this, null, null, null, null, null);
				this.jedis.srem(key(WORKERS), this.name);
				this.jedis.del(
					key(WORKER, this.name), 
					key(WORKER, this.name, STARTED), 
					key(STAT, FAILED, this.name), 
					key(STAT, PROCESSED, this.name));
				this.jedis.quit();
				this.workerThreadRef.set(null);
			}
		}
		else
		{
			throw new IllegalStateException("This WorkerImpl is already started");
		}
	}
	
	public void end(final boolean now)
	{
		this.running.set(false);
		if (now)
		{
			final Thread workerThread = this.workerThreadRef.get();
			if (workerThread != null)
			{
				workerThread.interrupt();
			}
		}
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
	
	public void setQueues(final Collection<String> queues)
	{
		checkQueues(queues);
		this.queueNames.clear();
		this.queueNames.addAll((queues == ALL_QUEUES) // Using object equality on purpose
			? this.jedis.smembers(key(QUEUES)) // Like '*' in other clients
			: queues);
	}

	/**
	 * Polls the queues for jobs and executes them.
	 */
	private void poll()
	{
		int missCount = 0;
		String curQueue = null;
		while (this.running.get())
		{
			try
			{
				renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
				curQueue = this.queueNames.poll(emptyQueueSleepTime, TimeUnit.MILLISECONDS);
				if (curQueue != null)
				{
					this.queueNames.add(curQueue); // Rotate the queues
					checkPaused();
					if (this.running.get()) // Might have been waiting in poll()/checkPaused() for a while
					{
						this.listenerDelegate.fireEvent(WorkerEvent.WORKER_POLL, this, curQueue, null, null, null, null);
						final String payload = this.jedis.lpop(key(QUEUE, curQueue));
						if (payload != null)
						{
							final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
							process(job, curQueue);
							missCount = 0;
						}
						else if (++missCount == this.queueNames.size() && this.running.get())
						{ // Keeps worker from busy-spinning on empty queues
							missCount = 0;
							Thread.sleep(emptyQueueSleepTime);
						}
					}
				}
			}
			catch (Exception e)
			{
				this.listenerDelegate.fireEvent(WorkerEvent.WORKER_ERROR, this, curQueue, null, null, null, e);
			}
		}
	}

	/**
	 * Checks to see if worker is paused. If so, wait until unpaused.
	 */
	private void checkPaused()
	{
		if (this.paused.get())
		{
			synchronized (this.paused)
			{
				while (this.paused.get())
				{
					try { this.paused.wait(); } catch (InterruptedException ie){}
				}
			}
		}
	}

	/**
	 * Materializes and executes the given job.
	 * 
	 * @param job the Job to process
	 * @param curQueue the queue the payload came from
	 */
	private void process(final Job job, final String curQueue)
	{
		this.listenerDelegate.fireEvent(WorkerEvent.JOB_PROCESS, this, curQueue, job, null, null, null);
		renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
		try
		{
			final String fullClassName = (this.jobPackage.length() == 0) 
				? job.getClassName() 
				: this.jobPackage + "." + job.getClassName();
			final Class<?> clazz = ReflectionUtils.forName(fullClassName);
			if (!this.jobTypes.contains(clazz))
			{
				throw new UnpermittedJobException(clazz);
			}
			if (!Runnable.class.isAssignableFrom(clazz) && !Callable.class.isAssignableFrom(clazz))
			{
				throw new ClassCastException("jobs must be a Runnable or a Callable: " + 
					clazz.getName() + " - " + job);
			}
			execute(job, curQueue, ReflectionUtils.createObject(clazz, job.getArgs()));
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
	 * @throws Exception if the instance is a callable and throws an exception
	 */
	private void execute(final Job job, final String curQueue, final Object instance)
	throws Exception
	{
		this.jedis.set(key(WORKER, this.name), statusMsg(curQueue, job));
		try
		{
			final Object result;
			this.listenerDelegate.fireEvent(WorkerEvent.JOB_EXECUTE, this, curQueue, job, instance, null, null);
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
	private void success(final Job job, final Object runner, final Object result, final String curQueue)
	{
		this.jedis.incr(key(STAT, PROCESSED));
		this.jedis.incr(key(STAT, PROCESSED, this.name));
		this.listenerDelegate.fireEvent(WorkerEvent.JOB_SUCCESS, this, curQueue, job, runner, result, null);
	}

	/**
	 * Update the status in Redis on failure
	 * 
	 * @param ex the Exception that occured
	 * @param job the Job that failed
	 * @param curQueue the queue the Job came from
	 */
	private void failure(final Exception ex, final Job job, final String curQueue)
	{
		this.jedis.incr(key(STAT, FAILED));
		this.jedis.incr(key(STAT, FAILED, this.name));
		try
		{
			this.jedis.rpush(key(FAILED), failMsg(ex, job));
		}
		catch (Exception e)
		{
			log.error("Error during serialization of failure payload for exception=" + ex + " job=" + job, e);
		}
		this.listenerDelegate.fireEvent(WorkerEvent.JOB_FAILURE, this, curQueue, job, null, null, ex);
	}

	/**
	 * Create and serialize a JobFailure.
	 * 
	 * @param ex the Exception that occured
	 * @param job the Job that failed
	 * @return the JSON representation of a new JobFailure
	 * @throws IOException if there was an error serializing the JobFailure
	 */
	private String failMsg(final Exception ex, final Job job)
	throws IOException
	{
		final JobFailure f = new JobFailure();
		f.setFailedAt(new Date());
		f.setWorker(this.name);
		f.setPayload(job);
		f.setException(ex);
		final String failMsg = ObjectMapperFactory.get().writeValueAsString(f);
		log.warn("failMsg={}", failMsg);
		return failMsg;
	}
	
	/**
	 * Create and serialize a WorkerStatus.
	 * 
	 * @param queue the queue the Job came from 
	 * @param job the Job currently being processed
	 * @return the JSON representation of a new WorkerStatus
	 * @throws IOException if there was an error serializing the WorkerStatus
	 */
	private String statusMsg(final String queue, final Job job)
	throws IOException
	{
		final WorkerStatus s = new WorkerStatus();
		s.setRunAt(new Date());
		s.setQueue(queue);
		s.setPayload(job);
		final String statusMsg = ObjectMapperFactory.get().writeValueAsString(s);
		log.debug("statusMsg={}", statusMsg);
		return statusMsg;
	}

	/**
	 * Creates a unique name, suitable for use with Resque.
	 * 
	 * @return a unique name for this worker
	 */
	private String createName()
	{
		final StringBuilder sb = new StringBuilder();
		sb.append(this.namespace).append(':')
			.append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
			.append('-').append(this.workerId);
		for (final String queueName : this.queueNames)
		{
			sb.append(':').append(queueName);
		}
		return sb.toString();
	}
	
	/**
	 * Builds a namespaced Redis key with the given arguments.
	 * 
	 * @param parts the key parts to be joined
	 * @return an assembled String key
	 */
	private String key(final String... parts)
	{
		return JesqueUtils.createKey(this.namespace, parts);
	}
	
	/**
	 * Rename the current thread with the given message.
	 * 
	 * @param msg the message to add to the thread name
	 */
	private void renameThread(final String msg)
	{
		Thread.currentThread().setName(this.threadNameBase + msg);
	}
	
	@Override
	public String toString()
	{
		return this.name;
	}
}
