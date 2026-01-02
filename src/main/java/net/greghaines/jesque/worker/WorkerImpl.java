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
import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.ScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.Jedis;

/**
 * WorkerImpl is an implementation of the Worker interface. Obeys the contract of a Resque worker in
 * Redis.
 */
public class WorkerImpl extends AbstractWorker {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerImpl.class);
  private static final int RECONNECT_ATTEMPTS = 120; // Total time: 10 min

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

  protected final Jedis jedis;

  /**
   * Creates a new WorkerImpl, which creates it's own connection to Redis using values from the
   * config.<br>
   * The worker will only listen to the supplied queues and execute jobs that are provided by the
   * given job factory.
   *
   * @param config used to create a connection to Redis and the package prefix for incoming jobs
   * @param queues the list of queues to poll
   * @param jobFactory the job factory that materializes the jobs
   * @throws IllegalArgumentException if either config, queues or jobFactory is null
   */
  public WorkerImpl(
      final Config config, final Collection<String> queues, final JobFactory jobFactory) {
    this(
        config,
        queues,
        jobFactory,
        new Jedis(config.getHostAndPort(), config.getJedisClientConfig()));
  }

  /**
   * Creates a new WorkerImpl, with the given connection to Redis.<br>
   * The worker will only listen to the supplied queues and execute jobs that are provided by the
   * given job factory. Uses the DRAIN_WHILE_MESSAGES_EXISTS NextQueueStrategy.
   *
   * @param config used to create a connection to Redis and the package prefix for incoming jobs
   * @param queues the list of queues to poll
   * @param jobFactory the job factory that materializes the jobs
   * @param jedis the connection to Redis
   * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
   */
  public WorkerImpl(
      final Config config,
      final Collection<String> queues,
      final JobFactory jobFactory,
      final Jedis jedis) {
    this(config, queues, jobFactory, jedis, NextQueueStrategy.DRAIN_WHILE_MESSAGES_EXISTS);
  }

  /**
   * Creates a new WorkerImpl, with the given connection to Redis.<br>
   * The worker will only listen to the supplied queues and execute jobs that are provided by the
   * given job factory.
   *
   * @param config used to create a connection to Redis and the package prefix for incoming jobs
   * @param queues the list of queues to poll
   * @param jobFactory the job factory that materializes the jobs
   * @param jedis the connection to Redis
   * @param nextQueueStrategy defines worker behavior once it has found messages in a queue
   * @throws IllegalArgumentException if either config, queues, jobFactory, jedis or
   *     nextQueueStrategy is null
   */
  public WorkerImpl(
      final Config config,
      final Collection<String> queues,
      final JobFactory jobFactory,
      final Jedis jedis,
      final NextQueueStrategy nextQueueStrategy) {
    super(config, queues, jobFactory, nextQueueStrategy, new DefaultExceptionHandler());
    if (jedis == null) {
      throw new IllegalArgumentException("jedis must not be null");
    }
    this.jedis = jedis;
    authenticateAndSelectDB();
    setQueues(queues);
    this.name = createName();
  }

  @Override
  protected void unregisterWorker() throws Exception {
    super.unregisterWorker();
    this.jedis.close();
  }

  /**
   * @return the number of times this Worker will attempt to reconnect to Redis before giving up
   */
  protected int getReconnectAttempts() {
    return RECONNECT_ATTEMPTS;
  }

  /**
   * Handle an exception that was thrown from inside {@link #poll()}.
   *
   * @param curQueue the name of the queue that was being processed when the exception was thrown
   * @param ex the exception that was thrown
   */
  @Override
  protected void recoverFromException(final String curQueue, final Exception ex) {
    final RecoveryStrategy recoveryStrategy =
        this.exceptionHandlerRef.get().onException(this, ex, curQueue);
    switch (recoveryStrategy) {
      case RECONNECT:
        LOG.info("Reconnecting to Redis in response to exception", ex);
        final int reconAttempts = getReconnectAttempts();
        if (!JedisUtils.reconnect(this.jedis, reconAttempts, RECONNECT_SLEEP_TIME)) {
          LOG.warn(
              "Terminating in response to exception after " + reconAttempts + " to reconnect", ex);
          end(false);
        } else {
          authenticateAndSelectDB();
          LOG.info("Reconnected to Redis");
          try {
            loadRedisScripts();
          } catch (IOException e) {
            LOG.error("Failed to reload Lua scripts after reconnect", e);
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

  private void authenticateAndSelectDB() {
    if (this.config.getJedisClientConfig().getPassword() != null) {
      this.jedis.auth(this.config.getJedisClientConfig().getPassword());
    }
    this.jedis.select(this.config.getJedisClientConfig().getDatabase());
  }

  @Override
  protected void success(
      final Job job, final Object runner, final Object result, final String curQueue) {
    // The job may have taken a long time;
    // make an effort to ensure the connection is OK
    JedisUtils.ensureJedisConnection(this.jedis);
    super.success(job, runner, result, curQueue);
  }

  @Override
  protected void failure(final Throwable thrwbl, final Job job, final String curQueue) {
    // The job may have taken a long time;
    // make an effort to ensure the connection is OK
    JedisUtils.ensureJedisConnection(this.jedis);
    super.failure(thrwbl, job, curQueue);
  }

  @Override
  protected AbstractTransaction createTransaction() {
    return this.jedis.multi();
  }

  @Override
  protected Jedis getJedis() {
    return this.jedis;
  }

  @Override
  protected String loadRedisScript(final String scriptName) throws IOException {
    return this.jedis.scriptLoad(ScriptUtils.readScript(scriptName));
  }
}
