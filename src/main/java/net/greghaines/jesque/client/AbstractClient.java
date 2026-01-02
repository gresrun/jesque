/*
 * Copyright 2011 Greg Haines
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package net.greghaines.jesque.client;

import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.commands.JedisCommands;

/**
 * Common logic for Client implementations.
 *
 * @author Greg Haines
 * @author Animesh Kumar
 */
public abstract class AbstractClient implements Client {

  private final String namespace;

  /**
   * Constructor.
   *
   * @param config used to get the namespace for key creation
   */
  protected AbstractClient(final Config config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    this.namespace = config.getNamespace();
  }

  /**
   * @return the namespace this client will use
   */
  protected String getNamespace() {
    return this.namespace;
  }

  /**
   * Builds a namespaced Redis key with the given arguments.
   *
   * @param parts the key parts to be joined
   * @return an assembled String key
   */
  protected String key(final String... parts) {
    return JesqueUtils.createKey(this.namespace, parts);
  }

  /** {@inheritDoc} */
  @Override
  public void enqueue(final String queue, final Job job) {
    validateArguments(queue, job);
    try {
      doEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void batchEnqueue(String queue, List<Job> jobs) {
    if (jobs == null) {
      throw new IllegalArgumentException("job list must not be null");
    }
    validateQueue(queue);
    for (Job job : jobs) {
      validateJob(job);
    }
    List<String> serializedJobs = new ArrayList<>(jobs.size());
    try {
      for (Job job : jobs) {
        serializedJobs.add(ObjectMapperFactory.get().writeValueAsString(job));
      }
      doBatchEnqueue(queue, serializedJobs);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void priorityEnqueue(final String queue, final Job job) {
    validateArguments(queue, job);
    try {
      doPriorityEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean acquireLock(
      final String lockName, final String lockHolder, final Duration timeout) {
    if ((lockName == null) || "".equals(lockName)) {
      throw new IllegalArgumentException("lockName must not be null or empty: " + lockName);
    }
    if ((lockHolder == null) || "".equals(lockHolder)) {
      throw new IllegalArgumentException("lockHolder must not be null or empty: " + lockHolder);
    }
    if (timeout.toSeconds() < 1) {
      throw new IllegalArgumentException("timeout must be a at least one second: " + timeout);
    }
    try {
      return doAcquireLock(lockName, lockHolder, timeout);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
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
   * Actually enqueue the serialized jobs.
   *
   * @param queue the queue to add the Jobs to
   * @param msgs the serialized Jobs
   * @throws Exception in case something goes wrong
   */
  protected abstract void doBatchEnqueue(String queue, List<String> msgs) throws Exception;

  /**
   * Actually enqueue the serialized job with high priority.
   *
   * @param queue the queue to add the Job to
   * @param msg the serialized Job
   * @throws Exception in case something goes wrong
   */
  protected abstract void doPriorityEnqueue(String queue, String msg) throws Exception;

  /**
   * Actually acquire the lock based upon the client acquisition model.
   *
   * @param lockName the name of the lock to acquire
   * @param timeout how long until the lock will expire, truncated to seconds
   * @param lockHolder a unique string identifying the caller
   * @return true, if the lock was acquired, false otherwise
   * @throws Exception in case something goes wrong
   */
  protected abstract boolean doAcquireLock(
      final String lockName, final String lockHolder, final Duration timeout) throws Exception;

  /**
   * Helper method that encapsulates the minimum logic for adding a job to a queue.
   *
   * @param jedis the connection to Redis
   * @param namespace the Resque namespace
   * @param queue the Resque queue name
   * @param jobJson the job serialized as JSON
   */
  public static void doEnqueue(
      final JedisCommands jedis, final String namespace, final String queue, final String jobJson) {
    jedis.sadd(JesqueUtils.createKey(namespace, QUEUES), queue);
    jedis.rpush(JesqueUtils.createKey(namespace, QUEUE, queue), jobJson);
  }

  /**
   * Helper method that encapsulates the minimum logic for adding jobs to a queue.
   *
   * @param jedis the connection to Redis
   * @param namespace the Resque namespace
   * @param queue the Resque queue name
   * @param jobJsons a list of jobs serialized as JSON
   */
  public static void doBatchEnqueue(
      final JedisCommands jedis,
      final Supplier<AbstractPipeline> makePipeline,
      final String namespace,
      final String queue,
      final List<String> jobJsons) {
    AbstractPipeline pipelined = makePipeline.get(); // jedis.pipelined();
    pipelined.sadd(JesqueUtils.createKey(namespace, QUEUES), queue);
    for (String jobJson : jobJsons) {
      pipelined.rpush(JesqueUtils.createKey(namespace, QUEUE, queue), jobJson);
    }
    pipelined.sync();
  }

  /**
   * Helper method that encapsulates the minimum logic for adding a high priority job to a queue.
   *
   * @param jedis the connection to Redis
   * @param namespace the Resque namespace
   * @param queue the Resque queue name
   * @param jobJson the job serialized as JSON
   */
  public static void doPriorityEnqueue(
      final JedisCommands jedis, final String namespace, final String queue, final String jobJson) {
    jedis.sadd(JesqueUtils.createKey(namespace, QUEUES), queue);
    jedis.lpush(JesqueUtils.createKey(namespace, QUEUE, queue), jobJson);
  }

  /**
   * Helper method that encapsulates the logic to acquire a lock.
   *
   * @param jedis the connection to Redis
   * @param namespace the Resque namespace
   * @param lockName all calls to this method will contend for a unique lock with the name of
   *     lockName
   * @param timeout how long until the lock will expire
   * @param lockHolder a unique string used to tell if you are the current holder of a lock for both
   *     acquisition, and extension
   * @return Whether or not the lock was acquired.
   */
  public static boolean doAcquireLock(
      final JedisCommands jedis,
      final String namespace,
      final String lockName,
      final String lockHolder,
      final Duration timeout) {
    final String key = JesqueUtils.createKey(namespace, lockName);
    final long timeoutSeconds = timeout.toSeconds();
    // If lock already exists, extend it
    String existingLockHolder = jedis.get(key);
    if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
      if (jedis.expire(key, timeoutSeconds) == 1) {
        existingLockHolder = jedis.get(key);
        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
          return true;
        }
      }
    }
    // Check to see if the key exists and is expired for cleanup purposes
    if (jedis.exists(key) && (jedis.ttl(key) < 0)) {
      // It is expired, but it may be in the process of being created, so
      // sleep and check again
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ie) {
      } // Ignore interruptions
      if (jedis.ttl(key) < 0) {
        existingLockHolder = jedis.get(key);
        // If it is our lock mark the time to live
        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
          if (jedis.expire(key, timeoutSeconds) == 1) {
            existingLockHolder = jedis.get(key);
            if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
              return true;
            }
          }
        } else { // The key is expired, whack it!
          jedis.del(key);
        }
      } else { // Someone else locked it while we were sleeping
        return false;
      }
    }
    // Ignore the cleanup steps above, start with no assumptions test
    // creating the key
    if (jedis.setnx(key, lockHolder) == 1) {
      // Created the lock, now set the expiration
      if (jedis.expire(key, timeoutSeconds) == 1) { // Set the timeout
        existingLockHolder = jedis.get(key);
        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
          return true;
        }
      } else { // Don't know why it failed, but for now just report failed
        // acquisition
        return false;
      }
    }
    // Failed to create the lock
    return false;
  }

  public static void doDelayedEnqueue(
      final JedisCommands jedis,
      final String namespace,
      final String queue,
      final String jobJson,
      final Instant future) {
    final String key = JesqueUtils.createKey(namespace, QUEUE, queue);
    // Add task only if this queue is either delayed or unused
    if (JedisUtils.canUseAsDelayedQueue(jedis, key)) {
      jedis.zadd(key, future.toEpochMilli(), jobJson);
      jedis.sadd(JesqueUtils.createKey(namespace, QUEUES), queue);
    } else {
      throw new IllegalArgumentException(queue + " cannot be used as a delayed queue");
    }
  }

  protected abstract void doDelayedEnqueue(String queue, String msg, Instant future)
      throws Exception;

  /** {@inheritDoc} */
  @Override
  public void delayedEnqueue(final String queue, final Job job, final Instant future) {
    validateArguments(queue, job, future);
    try {
      doDelayedEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job), future);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void doRemoveDelayedEnqueue(
      final JedisCommands jedis, final String namespace, final String queue, final String jobJson) {
    final String key = JesqueUtils.createKey(namespace, QUEUE, queue);
    // remove task only if this queue is either delayed or unused
    if (JedisUtils.canUseAsDelayedQueue(jedis, key)) {
      jedis.zrem(key, jobJson);
    } else {
      throw new IllegalArgumentException(queue + " cannot be used as a delayed queue");
    }
  }

  protected abstract void doRemoveDelayedEnqueue(String queue, String msg) throws Exception;

  /** {@inheritDoc} */
  @Override
  public void removeDelayedEnqueue(final String queue, final Job job) {
    validateArguments(queue, job);
    try {
      doRemoveDelayedEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void doRecurringEnqueue(
      final JedisCommands jedis,
      final Supplier<AbstractTransaction> makeTransaction,
      final String namespace,
      final String queue,
      final String jobJson,
      final Instant future,
      final Duration frequency) {
    final String queueKey = JesqueUtils.createKey(namespace, QUEUE, queue);
    final String hashKey = JesqueUtils.createRecurringHashKey(queueKey);

    if (JedisUtils.canUseAsRecurringQueue(jedis, queueKey, hashKey)) {
      AbstractTransaction transaction = makeTransaction.get(); // jedis.multi();
      transaction.zadd(queueKey, future.toEpochMilli(), jobJson);
      transaction.hset(hashKey, jobJson, Long.toString(frequency.toMillis()));
      if (transaction.exec() == null) {
        throw new RuntimeException("cannot add " + jobJson + " to recurring queue " + queue);
      }
    } else {
      throw new IllegalArgumentException(queue + " cannot be used as a recurring queue");
    }
  }

  protected abstract void doRecurringEnqueue(
      String queue, String msg, Instant future, Duration frequency) throws Exception;

  /** {@inheritDoc} */
  @Override
  public void recurringEnqueue(String queue, Job job, Instant future, Duration frequency) {
    validateArguments(queue, job, future, frequency);
    try {
      doRecurringEnqueue(
          queue, ObjectMapperFactory.get().writeValueAsString(job), future, frequency);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void doRemoveRecurringEnqueue(
      final JedisCommands jedis,
      final Supplier<AbstractTransaction> makeTransaction,
      final String namespace,
      final String queue,
      final String jobJson) {
    final String queueKey = JesqueUtils.createKey(namespace, QUEUE, queue);
    final String hashKey = JesqueUtils.createRecurringHashKey(queueKey);

    if (JedisUtils.canUseAsRecurringQueue(jedis, queueKey, hashKey)) {
      AbstractTransaction transaction = makeTransaction.get(); // jedis.multi();
      transaction.hdel(hashKey, jobJson);
      transaction.zrem(queueKey, jobJson);
      if (transaction.exec() == null) {
        throw new RuntimeException("cannot remove " + jobJson + " from recurring queue " + queue);
      }
    } else {
      throw new IllegalArgumentException(queue + " cannot be used as a recurring queue");
    }
  }

  protected abstract void doRemoveRecurringEnqueue(String queue, String msg) throws Exception;

  @Override
  public void removeRecurringEnqueue(String queue, Job job) {
    validateArguments(queue, job);
    try {
      doRemoveRecurringEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void validateArguments(final String queue, final Job job) {
    validateQueue(queue);
    validateJob(job);
  }

  private static void validateJob(Job job) {
    if (job == null) {
      throw new IllegalArgumentException("job must not be null");
    }
    if (!job.isValid()) {
      throw new IllegalStateException("job is not valid: " + job);
    }
  }

  private static void validateQueue(String queue) {
    if (queue == null || "".equals(queue)) {
      throw new IllegalArgumentException("queue must not be null or empty: " + queue);
    }
  }

  private static void validateArguments(final String queue, final Job job, final Instant future) {
    validateArguments(queue, job);
    validateFuture(future);
  }

  private static void validateFuture(Instant future) {
    if (System.currentTimeMillis() > future.toEpochMilli()) {
      throw new IllegalArgumentException("future must be after current time: " + future);
    }
  }

  private static void validateArguments(
      final String queue, final Job job, final Instant future, final Duration frequency) {
    validateArguments(queue, job, future);
    validateFrequency(frequency);
  }

  private static void validateFrequency(Duration frequency) {
    if (frequency.toSeconds() < 1) {
      throw new IllegalArgumentException("frequency must be greater than one second: " + frequency);
    }
  }
}
