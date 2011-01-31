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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
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
public class WorkerImpl implements Worker
{
	private static final Logger log = LoggerFactory.getLogger(WorkerImpl.class);
	private static final AtomicLong workerCounter = new AtomicLong(0);
	
	private final Jedis jedis;
	private final String namespace;
	private final String jobPackage;
	private final Queue<String> queues;
	private final Set<Class<?>> jobTypes;
	private final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
	private final String name;
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final DateFormat df = new SimpleDateFormat(JesqueUtils.DATE_FORMAT);
	private final long workerId = workerCounter.getAndIncrement();
	private final String threadNameBase = "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
	private final StringBuilder threadNameSB = new StringBuilder(this.threadNameBase);
	
	/**
	 * Creates a new WorkerImpl, which creates it's own connection to Redis using values from the config.
	 * The worker will only listen to the supplied queues and only execute jobs that are in the supplied job types.
	 * 
	 * @param config used to create a connection to Redis and the package prefix for incoming jobs
	 * @param queues the list of queues to poll
	 * @param jobTypes the list of job types to execute
	 * @throws IllegalArgumentException if the config is null, if the queues is null or empty, or if the jobTypes is null or empty
	 */
	public WorkerImpl(final Config config, final Collection<String> queues, final Collection<? extends Class<?>> jobTypes)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		if (queues == null || queues.isEmpty())
		{
			throw new IllegalArgumentException("queues must not be null or empty: " + queues);
		}
		if (jobTypes == null || jobTypes.isEmpty())
		{
			throw new IllegalArgumentException("jobTypes must not be null or empty: " + jobTypes);
		}
		this.namespace = config.getNamespace();
		this.jobPackage = config.getJobPackage();
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getDatabase() != null)
		{
			this.jedis.select(config.getDatabase());
		}
		this.queues = checkQueues(queues);
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
				this.jedis.sadd(key("workers"), this.name);
				this.jedis.set(key("worker", this.name, "started"), this.df.format(new Date()));
				this.listenerDelegate.fireEvent(WorkerEvent.WORKER_START, this, null, null, null, null, null);
				poll();
			}
			finally
			{
				this.listenerDelegate.fireEvent(WorkerEvent.WORKER_STOP, this, null, null, null, null, null);
				this.jedis.srem(key("workers"), this.name);
				this.jedis.del(
					key("worker", this.name), 
					key("worker", this.name, "started"), 
					key("stat", "failed", this.name), 
					key("stat", "processed", this.name));
			}
		}
		else
		{
			throw new IllegalStateException("This WorkerImpl is already started");
		}
	}
	
	public void end()
	{
		this.running.set(false);
		synchronized (this.queues)
		{ // In case this.queues is empty and we're waiting for a queue
			this.queues.notifyAll();
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
		this.queues.add(queueName);
		synchronized (this.queues)
		{ // In case this.queues is empty and we're waiting for a queue
			this.queues.notifyAll();
		}
	}
	
	public void removeQueue(final String queueName, final boolean all)
	{
		if (all)
		{ // Remove all instances
			while (this.queues.remove(queueName)){}
		}
		else
		{ // Only remove one instance
			this.queues.remove(queueName);
		}
	}

	/**
	 * Ensures that the given queues are in the right format.
	 * Also, retrives the current list of queues if ALL_QUEUES is passed.
	 * 
	 * @param qs List of queues to subscribe to
	 * @return a copy of the passed-in list of queues or the current list of queues if ALL_QUEUES is passed.
	 */
	private Queue<String> checkQueues(final Collection<String> qs)
	{
		return new ConcurrentLinkedQueue<String>((qs == ALL_QUEUES) // Using object equality on purpose
			? this.jedis.smembers(key("queues")) // Like '*' in other clients
			: qs);
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
			if (this.queues.isEmpty())
			{
				synchronized (this.queues)
				{
					while (this.queues.isEmpty() && this.running.get())
					{
						try { this.queues.wait(); } catch (InterruptedException ie){}
					}
				}
			}
			if (this.running.get())
			{
				try
				{
					renameThread("Waiting for " + JesqueUtils.join(",", this.queues));
					curQueue = this.queues.remove();
					this.queues.add(curQueue); // Rotate the queue
					this.listenerDelegate.fireEvent(WorkerEvent.WORKER_POLL, this, curQueue, null, null, null, null);
					final String payload = this.jedis.lpop(key("queue", curQueue));
					if (payload != null)
					{
						perform(payload, curQueue);
						missCount = 0;
					}
					else if (++missCount == this.queues.size() && this.running.get())
					{ // Keeps us from busy-spinning on empty queues
						missCount = 0;
						JesqueUtils.sleepTight(500);
					}
				}
				catch (Exception e)
				{
					this.listenerDelegate.fireEvent(WorkerEvent.WORKER_ERROR, this, curQueue, null, null, null, e);
				}
			}
		}
	}

	/**
	 * Deserialzes, matierializes and executes the given payload.
	 * 
	 * @param payload the JSON representation of a Job
	 * @param curQueue the queue the payload came from
	 * @throws IOException if the payload could not be deserialized
	 */
	private void perform(final String payload, final String curQueue)
	throws IOException
	{
		final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
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
			final Object instance = ReflectionUtils.createObject(clazz, job.getArgs());
			this.jedis.set(key("worker", this.name), statusMsg(curQueue, job));
			try
			{
				Object result = null;
				this.listenerDelegate.fireEvent(WorkerEvent.JOB_EXECUTE, this, curQueue, job, instance, null, null);
				if (instance instanceof Callable)
				{
					result = ((Callable<?>) instance).call(); // The job is executing!
				}
				else if (instance instanceof Runnable)
				{
					((Runnable) instance).run(); // The job is executing!
				}
				else
				{
					throw new ClassCastException("instance is not a Runnable or a Callable: " + 
						instance.getClass().getName() + " - " + instance);
				}
				success(job, instance, result, curQueue);
			}
			finally
			{
				this.jedis.del(key("worker", this.name));
			}
		}
		catch (Exception e)
		{
			failure(e, payload, job, curQueue);
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
		this.jedis.incr(key("stat", "processed"));
		this.jedis.incr(key("stat", "processed", this.name));
		this.listenerDelegate.fireEvent(WorkerEvent.JOB_SUCCESS, this, curQueue, job, runner, result, null);
	}

	/**
	 * Update the status in Redis on failure
	 * 
	 * @param ex the Exception that occured
	 * @param payload the JSON representation of the Job
	 * @param job the Job that failed
	 * @param curQueue the queue the Job came from
	 */
	private void failure(final Exception ex, final String payload, final Job job, final String curQueue)
	{
		this.jedis.incr(key("stat", "failed"));
		this.jedis.incr(key("stat", "failed", this.name));
		try
		{
			this.jedis.rpush(key("failed"), failMsg(ex, job));
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
		f.setException(ex.getClass().getName());
		f.setError(ex.getMessage());
		f.setBacktrace(JesqueUtils.createStackTrace(ex));
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
		for (final String q : this.queues)
		{
			sb.append(':').append(q);
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
		this.threadNameSB.setLength(this.threadNameBase.length());
		Thread.currentThread().setName(this.threadNameSB.append(msg).toString());
	}
}
