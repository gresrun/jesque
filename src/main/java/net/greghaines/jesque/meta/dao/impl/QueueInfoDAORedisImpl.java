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

import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.meta.QueueInfo;
import net.greghaines.jesque.meta.dao.QueueInfoDAO;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.resps.Tuple;

/**
 * QueueInfoDAORedisImpl gets queue information from Redis.
 *
 * @author Greg Haines
 * @author Animesh Kumar
 */
public class QueueInfoDAORedisImpl implements QueueInfoDAO {

  private final Config config;
  private final UnifiedJedis jedisPool;

  /**
   * Constructor.
   *
   * @param config the Jesque configuration
   * @param jedisPool the pool of Jedis connections
   */
  public QueueInfoDAORedisImpl(final Config config, final UnifiedJedis jedisPool) {
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
  public List<String> getQueueNames() {
    final List<String> queueNames = new ArrayList<String>(this.jedisPool.smembers(key(QUEUES)));
    Collections.sort(queueNames);
    return queueNames;
  }

  /** {@inheritDoc} */
  @Override
  public long getPendingCount() {
    final List<String> queueNames = getQueueNames();
    long pendingCount = 0L;
    for (final String queueName : queueNames) {
      pendingCount += size(this.jedisPool, queueName);
    }
    return pendingCount;
  }

  /** {@inheritDoc} */
  @Override
  public long getProcessedCount() {
    final String processedStr = this.jedisPool.get(key(STAT, PROCESSED));
    return (processedStr == null) ? 0L : Long.parseLong(processedStr);
  }

  /** {@inheritDoc} */
  @Override
  public List<QueueInfo> getQueueInfos() {
    final List<String> queueNames = getQueueNames();
    final List<QueueInfo> queueInfos = new ArrayList<>(queueNames.size());
    for (final String queueName : queueNames) {
      final QueueInfo queueInfo = new QueueInfo();
      queueInfo.setName(queueName);
      queueInfo.setSize(size(this.jedisPool, queueName));
      queueInfo.setDelayed(delayed(this.jedisPool, queueName));
      if (queueInfo.isDelayed()) {
        queueInfo.setPending(pending(this.jedisPool, queueName));
      }
      queueInfos.add(queueInfo);
    }
    Collections.sort(queueInfos);
    return queueInfos;
  }

  /** {@inheritDoc} */
  @Override
  public QueueInfo getQueueInfo(final String name, final long jobOffset, final long jobCount) {
    try {
      final QueueInfo queueInfo = new QueueInfo();
      queueInfo.setName(name);
      queueInfo.setSize(size(this.jedisPool, name));
      queueInfo.setDelayed(delayed(this.jedisPool, name));
      if (queueInfo.isDelayed()) {
        queueInfo.setPending(pending(this.jedisPool, name));
      }
      queueInfo.setJobs(getJobs(this.jedisPool, name, jobOffset, jobCount));
      return queueInfo;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean delayed(final UnifiedJedis jedis, final String queueName) {
    final String key = key(QUEUE, queueName);
    return JedisUtils.isDelayedQueue(jedis, key);
  }

  /** {@inheritDoc} */
  @Override
  public void removeQueue(final String name) {
    this.jedisPool.srem(key(QUEUES), name);
    this.jedisPool.del(key(QUEUE, name));
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

  /**
   * Size of a queue.
   *
   * @param jedis
   * @param queueName
   * @return
   */
  private long size(final UnifiedJedis jedis, final String queueName) {
    final String key = key(QUEUE, queueName);
    final long size;
    if (JedisUtils.isDelayedQueue(jedis, key)) { // If delayed queue, use ZCARD
      size = jedis.zcard(key);
    } else { // Else, use LLEN
      size = jedis.llen(key);
    }
    return size;
  }

  private long pending(final UnifiedJedis jedis, final String queueName) {
    final String key = key(QUEUE, queueName);
    return jedis.zcount(key, 0, System.currentTimeMillis());
  }

  /**
   * Get list of Jobs from a queue.
   *
   * @param jedis
   * @param queueName
   * @param jobOffset
   * @param jobCount
   * @return
   */
  private List<Job> getJobs(
      final UnifiedJedis jedis, final String queueName, final long jobOffset, final long jobCount)
      throws Exception {
    final String key = key(QUEUE, queueName);
    final List<Job> jobs = new ArrayList<>();
    if (JedisUtils.isDelayedQueue(jedis, key)) { // If delayed queue, use ZRANGEWITHSCORES
      final List<Tuple> elements = jedis.zrangeWithScores(key, jobOffset, jobOffset + jobCount - 1);
      for (final Tuple elementWithScore : elements) {
        final Job job =
            ObjectMapperFactory.get().readValue(elementWithScore.getElement(), Job.class);
        job.setRunAt(elementWithScore.getScore());
        jobs.add(job);
      }
    } else { // Else, use LRANGE
      final List<String> elements = jedis.lrange(key, jobOffset, jobOffset + jobCount - 1);
      for (final String element : elements) {
        jobs.add(ObjectMapperFactory.get().readValue(element, Job.class));
      }
    }
    return jobs;
  }
}
