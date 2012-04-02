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
package net.greghaines.jesque.client;

import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;

import redis.clients.jedis.Jedis;

/**
 * Common logic for Client implementations.
 * 
 * @author Greg Haines
 */
public abstract class AbstractClient implements Client
{
	private final String namespace;

	/**
	 * @param config used to get the namespace for key creation
	 */
	protected AbstractClient(final Config config)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		this.namespace = config.getNamespace();
	}

	/**
	 * @return the namespace this client will use
	 */
	protected String getNamespace()
	{
		return this.namespace;
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

	public void enqueue(final String queue, final Job job)
	{
		validateArguments(queue, job);
		try
		{
			doEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
		}
		catch (RuntimeException re)
		{
			throw re;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public void priorityEnqueue(final String queue, final Job job)
	{
		validateArguments(queue, job);
		try
		{
			doPriorityEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
		}
		catch (RuntimeException re)
		{
			throw re;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public boolean acquireLock(final String lockName, final String lockHolder, final Integer timeout)
	{
		if ((lockName == null) || "".equals(lockName))
		{
			throw new IllegalArgumentException("lockName must not be null or empty: " + lockName);
		}
		if ((lockHolder == null) || "".equals(lockHolder))
		{
			throw new IllegalArgumentException("lockHolder must not be null or empty: " + lockHolder);
		}
		if (timeout == null)
		{
			throw new IllegalArgumentException("job must not be null");
		}
		if (timeout < 1)
		{
			throw new IllegalArgumentException("timeout must be a positive number");
		}
		try
		{
			return doAcquireLock(lockName, lockHolder, timeout);
		}
		catch (RuntimeException re)
		{
			throw re;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Actually enqueue the serialized job.
	 * 
	 * @param queue the queue to add the Job to
	 * @param msg the serialized Job
	 * @throws Exception in case something goes wrong
	 */
	protected abstract void doEnqueue(String queue, String msg) throws Exception;

	/**
	 * Actually enqueue the serialized job with high priority.
	 * 
	 * @param queue the queue to add the Job to
	 * @param msg the serialized Job
	 * @throws Exception in case something goes wrong
	 */
	protected abstract void doPriorityEnqueue(String queue, String msg) throws Exception;

	/**
	 * Actually acquire the lock based upon the client acquisition model
	 * @param lockName
	 *            all calls to this method will contend for a unique lock with
	 *            the name of lockName
	 * @param timeout
	 *            millis until the lock will expire
	 * @param lockHolder
	 *            a unique string used to tell if you are the current holder of
	 *            a lock for both acquisition, and extension
	 * @return Whether or not the lock was acquired.
	 */
	protected abstract boolean doAcquireLock(final String lockName,
			final String lockHolder, final Integer timeout) throws Exception;

	/**
	 * Helper method that encapsulates the minimum logic for adding a job to a queue.
	 * 
	 * @param jedis the connection to Redis
	 * @param namespace the Resque namespace
	 * @param queue the Resque queue name
	 * @param jobJson the job serialized as JSON
	 */
	public static void doEnqueue(final Jedis jedis, final String namespace, 
			final String queue, final String jobJson)
	{
		jedis.sadd(JesqueUtils.createKey(namespace, QUEUES), queue);
		jedis.rpush(JesqueUtils.createKey(namespace, QUEUE, queue), jobJson);
	}

	/**
	 * Helper method that encapsulates the minimum logic for adding a high priority job to a queue.
	 * 
	 * @param jedis the connection to Redis
	 * @param namespace the Resque namespace
	 * @param queue the Resque queue name
	 * @param jobJson the job serialized as JSON
	 */
	public static void doPriorityEnqueue(final Jedis jedis, final String namespace, 
			final String queue, final String jobJson)
	{
		jedis.sadd(JesqueUtils.createKey(namespace, QUEUES), queue);
		jedis.lpush(JesqueUtils.createKey(namespace, QUEUE, queue), jobJson);
	}

	/**
	 * Helper method that encapsulates the logic to acquire a lock.
	 * @param jedis
	 *            the connection to Redis
	 * @param namespace
	 *            the Resque namespace
	 * @param lockName
	 *            all calls to this method will contend for a unique lock with
	 *            the name of lockName
	 * @param timeout
	 *            millis until the lock will expire
	 * @param lockHolder
	 *            a unique string used to tell if you are the current holder of
	 *            a lock for both acquisition, and extension
	 * @return Whether or not the lock was acquired.
	 */
	public static boolean doAcquireLock(final Jedis jedis,
			final String namespace, final String lockName,
			final String lockHolder, Integer timeout)
	{
		final String key = JesqueUtils.createKey(namespace, lockName);

		// if lock already exists, extend it
		String existingLockHolder = jedis.get(key);
		if ((existingLockHolder != null)
				&& existingLockHolder.equals(lockHolder))
		{
			if (jedis.expire(key, timeout) == 1)
			{
				existingLockHolder = jedis.get(key);
				if ((existingLockHolder != null)
						&& existingLockHolder.equals(lockHolder))
				{
					return true;
				}
			}
		}

		// check to see if the key exists and is expired for cleanup purposes
		if (jedis.exists(key) && (jedis.ttl(key) < 0))
		{ // it is expired, but it may be in the process of being created, so sleep and check again
			try
			{
				Thread.sleep(2000);
			}
			catch (InterruptedException ie){} // ignore interruptions
			if (jedis.ttl(key) < 0)
			{
				existingLockHolder = jedis.get(key);

				// if it is our lock mark the time to live
				if ((existingLockHolder != null)
						&& existingLockHolder.equals(lockHolder))
				{
					if (jedis.expire(key, timeout) == 1)
					{
						existingLockHolder = jedis.get(key);
						if ((existingLockHolder != null)
								&& existingLockHolder.equals(lockHolder))
						{
							return true;
						}
					}
				}
				else
				{ // the key is expired, whack it!
					jedis.del(key);
				}
			}
			else
			{ // someone else locked it while we were sleeping
				return false;
			}
		}

		// ignore the cleanup steps above, start with no assumptions

		// test creating the key
		if (jedis.setnx(key, lockHolder) == 1)
		{ // yay, created the lock, now set the expiration
			if (jedis.expire(key, timeout) == 1)
			{ // set the timeout
				existingLockHolder = jedis.get(key);
				if ((existingLockHolder != null)
						&& existingLockHolder.equals(lockHolder))
				{
					return true;
				}
			}
			else
			{ // don't know why it failed, but for now just report failed acquisition
				return false;
			}
		}
		// failed to create the lock
		return false;
	}

	private static void validateArguments(final String queue, final Job job)
	{
		if (queue == null || "".equals(queue))
		{
			throw new IllegalArgumentException("queue must not be null or empty: " + queue);
		}
		if (job == null)
		{
			throw new IllegalArgumentException("job must not be null");
		}
		if (!job.isValid())
		{
			throw new IllegalStateException("job is not valid: " + job);
		}
	}
}
