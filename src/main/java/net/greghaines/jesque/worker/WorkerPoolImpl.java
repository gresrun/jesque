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
package net.greghaines.jesque.worker;

import static net.greghaines.jesque.worker.WorkerEvent.*;

import java.io.IOException;
import java.util.Collection;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.ScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/**
 * WorkerPoolImpl is an implementation of the Worker interface that uses a connection pool. Obeys
 * the contract of a Resque worker in Redis.
 */
public class WorkerPoolImpl extends AbstractWorker {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerPoolImpl.class);

  /**
   * @return true if worker threads names will change during normal operation
   */
  public static boolean isThreadNameChangingEnabled() {
    return AbstractWorker.isThreadNameChangingEnabled();
  }

  /**
   * Enable/disable worker thread renaming during normal operation. (Disabled by default)
   *
   * @param enabled whether threads' names should change during normal operation
   */
  public static void setThreadNameChangingEnabled(final boolean enabled) {
    AbstractWorker.setThreadNameChangingEnabled(enabled);
  }

  protected final UnifiedJedis jedisPool;

  /**
   * Creates a new WorkerImpl, with the given connection to Redis.<br>
   * The worker will only listen to the supplied queues and execute jobs that are provided by the
   * given job factory. Uses the DRAIN_WHILE_MESSAGES_EXISTS NextQueueStrategy.
   *
   * @param config used to create a connection to Redis and the package prefix for incoming jobs
   * @param queues the list of queues to poll
   * @param jobFactory the job factory that materializes the jobs
   * @param jedisPool the Redis connection pool
   * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
   */
  public WorkerPoolImpl(
      final Config config,
      final Collection<String> queues,
      final JobFactory jobFactory,
      final UnifiedJedis jedisPool) {
    this(config, queues, jobFactory, jedisPool, NextQueueStrategy.DRAIN_WHILE_MESSAGES_EXISTS);
  }

  /**
   * Creates a new WorkerImpl, with the given connection to Redis.<br>
   * The worker will only listen to the supplied queues and execute jobs that are provided by the
   * given job factory.
   *
   * @param config used to create a connection to Redis and the package prefix for incoming jobs
   * @param queues the list of queues to poll
   * @param jobFactory the job factory that materializes the jobs
   * @param jedisPool the Redis connection pool
   * @param nextQueueStrategy defines worker behavior once it has found messages in a queue
   * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
   */
  public WorkerPoolImpl(
      final Config config,
      final Collection<String> queues,
      final JobFactory jobFactory,
      final UnifiedJedis jedisPool,
      final NextQueueStrategy nextQueueStrategy) {
    super(config, queues, jobFactory, nextQueueStrategy, new DefaultPoolExceptionHandler());
    if (jedisPool == null) {
      throw new IllegalArgumentException("jedisPool must not be null");
    }
    this.jedisPool = jedisPool;
    setQueues(queues);
    this.name = createName();
  }

  @Override
  protected void recoverFromException(final String curQueue, final Exception ex) {
    final RecoveryStrategy recoveryStrategy =
        this.exceptionHandlerRef.get().onException(this, ex, curQueue);
    switch (recoveryStrategy) {
      case RECONNECT:
        if (ex instanceof JedisNoScriptException) {
          LOG.info("Got JedisNoScriptException while reconnecting, reloading Redis scripts");
          loadRedisScripts();
        } else {
          LOG.info("Waiting " + RECONNECT_SLEEP_TIME + "ms for pool to reconnect to redis", ex);
          try {
            Thread.sleep(RECONNECT_SLEEP_TIME);
          } catch (InterruptedException e) {
          }
        }
        break;
      case TERMINATE:
        LOG.warn("Terminating in response to exception", ex);
        end(false);
        break;
      case PROCEED:
        this.listenerDelegate.fireEvent(WORKER_ERROR, this, curQueue, null, null, null, ex);
        break;
      default:
        LOG.error(
            "Unknown RecoveryStrategy: "
                + recoveryStrategy
                + " while attempting to recover from the following exception; worker proceeding...",
            ex);
        break;
    }
  }

  @Override
  protected AbstractTransaction createTransaction() {
    return this.jedisPool.multi();
  }

  @Override
  protected UnifiedJedis getJedis() {
    return this.jedisPool;
  }

  @Override
  protected void loadRedisScripts() {
    try {
      super.loadRedisScripts();
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected String loadRedisScript(final String scriptName) throws IOException {
    return this.jedisPool.scriptLoad(ScriptUtils.readScript(scriptName));
  }
}
