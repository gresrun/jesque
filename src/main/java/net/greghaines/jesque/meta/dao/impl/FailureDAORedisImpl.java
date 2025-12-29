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
package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.meta.dao.FailureDAO;
import net.greghaines.jesque.utils.JesqueUtils;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.commands.JedisCommands;

/**
 * Accesses failure information about Jesque/Resque from Redis.
 *
 * @author Greg Haines
 */
public class FailureDAORedisImpl implements FailureDAO {

  private final Config config;
  private final UnifiedJedis jedisPool;

  /**
   * Constructor.
   *
   * @param config the Jesque configuration
   * @param jedisPool the connection pool to Redis
   */
  public FailureDAORedisImpl(final Config config, final UnifiedJedis jedisPool) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (jedisPool == null) {
      throw new IllegalArgumentException("jedisPool must not be null");
    }
    this.config = config;
    this.jedisPool = jedisPool;
  }

  /** {@inheritDoc} */
  @Override
  public long getCount() {
    final String failedStr = this.jedisPool.get(key(STAT, FAILED));
    return (failedStr == null) ? 0L : Long.parseLong(failedStr);
  }

  /** {@inheritDoc} */
  @Override
  public long getFailQueueJobCount() {
    return this.jedisPool.llen(key(FAILED));
  }

  /** {@inheritDoc} */
  @Override
  public List<JobFailure> getFailures(final long offset, final long count) {
    final List<String> payloads = this.jedisPool.lrange(key(FAILED), offset, offset + count - 1);
    final List<JobFailure> failures = new ArrayList<JobFailure>(payloads.size());
    try {
      for (final String payload : payloads) {
        if (payload.charAt(0) == '{') { // Ignore non-JSON strings
          failures.add(ObjectMapperFactory.get().readValue(payload, JobFailure.class));
        }
      }
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return failures;
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    this.jedisPool.del(key(FAILED));
  }

  /** {@inheritDoc} */
  @Override
  public Date requeue(final long index) {
    Date retryDate = null;
    final List<JobFailure> failures = getFailures(index, 1);
    if (!failures.isEmpty()) {
      retryDate = new Date();
      final JobFailure failure = failures.get(0);
      failure.setRetriedAt(retryDate);
      try {
        this.jedisPool.lset(
            key(FAILED), index, ObjectMapperFactory.get().writeValueAsString(failure));
        enqueue(this.jedisPool, failure.getQueue(), failure.getPayload());
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return retryDate;
  }

  /** {@inheritDoc} */
  @Override
  public void remove(final long index) {
    final String failedKey = key(FAILED);
    final String randId = UUID.randomUUID().toString();
    this.jedisPool.lset(failedKey, index, randId);
    this.jedisPool.lrem(failedKey, 1, randId);
  }

  protected void enqueue(final JedisCommands jedis, final String queue, final Job job)
      throws IOException {
    if (queue == null || "".equals(queue)) {
      throw new IllegalArgumentException("queue must not be null or empty: " + queue);
    }
    if (job == null) {
      throw new IllegalArgumentException("job must not be null");
    }
    if (!job.isValid()) {
      throw new IllegalStateException("job is not valid: " + job);
    }
    final String msg = ObjectMapperFactory.get().writeValueAsString(job);
    jedis.sadd(key(QUEUES), queue);
    jedis.rpush(key(QUEUE, queue), msg);
  }

  /**
   * Builds a namespaced Redis key with the given arguments.
   *
   * @param parts the key parts to be joined
   * @return an assembled String key
   */
  private String key(final String... parts) {
    return JesqueUtils.createKey(this.config.getNamespace(), parts);
  }
}
