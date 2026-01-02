/*
 * Copyright 2026 Greg Haines
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

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.worker.JobExecutor.State.*;
import static net.greghaines.jesque.worker.WorkerEvent.*;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisException;

/** AbstractWorker extracts common behavior for the concrete worker implementations. */
public abstract class AbstractWorker implements Worker {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractWorker.class);
  protected static final AtomicLong WORKER_COUNTER = new AtomicLong(0);
  protected static final long EMPTY_QUEUE_SLEEP_TIME = 500; // 500 ms
  protected static final long RECONNECT_SLEEP_TIME = 5000; // 5 sec
  protected static final String LPOPLPUSH_LUA = "/workerScripts/jesque_lpoplpush.lua";
  protected static final String POP_LUA = "/workerScripts/jesque_pop.lua";
  protected static final String POP_FROM_MULTIPLE_PRIO_QUEUES =
      "/workerScripts/fromMultiplePriorityQueues.lua";

  // Set the thread name to the message for debugging
  protected static volatile boolean threadNameChangingEnabled = false;

  /**
   * @return true if worker threads names will change during normal operation
   */
  public static boolean isThreadNameChangingEnabled() {
    return threadNameChangingEnabled;
  }

  /** Enable/disable worker thread renaming during normal operation. */
  public static void setThreadNameChangingEnabled(final boolean enabled) {
    threadNameChangingEnabled = enabled;
  }

  protected final Config config;
  protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<>();
  protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
  protected final AtomicReference<JobExecutor.State> state =
      new AtomicReference<>(JobExecutor.State.NEW);
  protected final AtomicBoolean paused = new AtomicBoolean(false);
  protected final AtomicBoolean processingJob = new AtomicBoolean(false);
  protected final AtomicReference<String> popScriptHash = new AtomicReference<>(null);
  protected final AtomicReference<String> lpoplpushScriptHash = new AtomicReference<>(null);
  protected final AtomicReference<String> multiPriorityQueuesScriptHash =
      new AtomicReference<>(null);
  protected final long workerId = WORKER_COUNTER.getAndIncrement();
  protected final String threadNameBase =
      "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
  protected final AtomicReference<Thread> threadRef = new AtomicReference<>(null);
  protected final AtomicReference<ExceptionHandler> exceptionHandlerRef;
  protected final AtomicReference<FailQueueStrategy> failQueueStrategyRef;
  protected final JobFactory jobFactory;
  protected final NextQueueStrategy nextQueueStrategy;

  protected String name;

  protected AbstractWorker(
      final Config config,
      final Collection<String> queues,
      final JobFactory jobFactory,
      final NextQueueStrategy nextQueueStrategy,
      final ExceptionHandler defaultExceptionHandler) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (jobFactory == null) {
      throw new IllegalArgumentException("jobFactory must not be null");
    }
    if (nextQueueStrategy == null) {
      throw new IllegalArgumentException("nextQueueStrategy must not be null");
    }
    checkQueues(queues);
    this.nextQueueStrategy = nextQueueStrategy;
    this.config = config;
    this.jobFactory = jobFactory;
    this.failQueueStrategyRef =
        new AtomicReference<>(new DefaultFailQueueStrategy(config.getNamespace()));
    this.exceptionHandlerRef = new AtomicReference<>(defaultExceptionHandler);
    // Subclasses must call setQueues(...) and set `name` after initializing the client
    // JEP 513 can fix this constructor chain issue in the future
  }

  /**
   * @return this worker's identifier
   */
  public long getWorkerId() {
    return this.workerId;
  }

  /**
   * Verify that the given queues are all valid.
   *
   * @param queues the given queues
   */
  protected static void checkQueues(final Iterable<String> queues) {
    if (queues == null) {
      throw new IllegalArgumentException("queues must not be null");
    }
    for (final String queue : queues) {
      if (queue == null || "".equals(queue)) {
        throw new IllegalArgumentException("queues' members must not be null: " + queues);
      }
    }
  }

  /**
   * Starts this worker. Registers the worker in Redis and begins polling the queues for jobs.<br>
   * Stop this worker by calling end() on any thread.
   */
  @Override
  public void run() {
    if (this.state.compareAndSet(JobExecutor.State.NEW, JobExecutor.State.RUNNING)) {
      try {
        renameThread("RUNNING");
        this.threadRef.set(Thread.currentThread());
        registerWorker();
        this.listenerDelegate.fireEvent(WORKER_START, this, null, null, null, null, null);
        loadRedisScripts();
        poll();
      } catch (Exception ex) {
        LOG.error("Uncaught exception in worker run-loop!", ex);
        this.listenerDelegate.fireEvent(WORKER_ERROR, this, null, null, null, null, ex);
      } finally {
        renameThread("STOPPING");
        this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
        try {
          unregisterWorker();
        } catch (Exception e) {
          LOG.warn("Error unregistering worker", e);
        }
        this.threadRef.set(null);
      }
    } else if (JobExecutor.State.RUNNING.equals(this.state.get())) {
      throw new IllegalStateException("This Worker is already running");
    } else {
      throw new IllegalStateException("This Worker is shutdown");
    }
  }

  /**
   * Shutdown this Worker.<br>
   * <b>The worker cannot be started again; create a new worker in this case.</b>
   *
   * @param now if true, an effort will be made to stop any job in progress
   */
  @Override
  public void end(final boolean now) {
    if (now) {
      this.state.set(SHUTDOWN_IMMEDIATE);
      final Thread workerThread = this.threadRef.get();
      if (workerThread != null) {
        workerThread.interrupt();
      }
    } else {
      this.state.set(SHUTDOWN);
    }
    togglePause(false); // Release any threads waiting in checkPaused()
  }

  protected void registerWorker() throws Exception {
    getJedis().sadd(key(WORKERS), this.name);
    getJedis()
        .set(key(WORKER, this.name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
  }

  protected void unregisterWorker() throws Exception {
    getJedis().srem(key(WORKERS), this.name);
    getJedis()
        .del(
            key(WORKER, this.name),
            key(WORKER, this.name, STARTED),
            key(STAT, FAILED, this.name),
            key(STAT, PROCESSED, this.name));
  }

  protected void loadRedisScripts() throws IOException {
    this.popScriptHash.set(loadRedisScript(POP_LUA));
    this.lpoplpushScriptHash.set(loadRedisScript(LPOPLPUSH_LUA));
    this.multiPriorityQueuesScriptHash.set(loadRedisScript(POP_FROM_MULTIPLE_PRIO_QUEUES));
  }

  protected abstract String loadRedisScript(String scriptName) throws IOException;

  /**
   * Remove a job from the given queue.
   *
   * @param curQueue the queue to remove a job from
   * @return a JSON string of a job or null if there was nothing to de-queue
   */
  protected String pop(final String curQueue) {
    final String key = key(QUEUE, curQueue);
    final String now = Long.toString(System.currentTimeMillis());
    final String inflightKey = key(INFLIGHT, this.name, curQueue);
    return switch (this.nextQueueStrategy) {
      case DRAIN_WHILE_MESSAGES_EXISTS ->
          (String)
              getJedis()
                  .evalsha(
                      this.popScriptHash.get(),
                      3,
                      key,
                      inflightKey,
                      JesqueUtils.createRecurringHashKey(key),
                      now);
      case RESET_TO_HIGHEST_PRIORITY ->
          (String)
              getJedis()
                  .evalsha(
                      this.multiPriorityQueuesScriptHash.get(),
                      3,
                      curQueue,
                      inflightKey,
                      config.getNamespace(),
                      now);
      default -> throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
    };
  }

  protected void removeInFlight(final String curQueue, boolean skipRequeue) {
    if (SHUTDOWN_IMMEDIATE.equals(this.state.get()) && !skipRequeue) {
      getJedis()
          .evalsha(
              this.lpoplpushScriptHash.get(),
              2,
              key(INFLIGHT, this.name, curQueue),
              key(QUEUE, curQueue));
    } else {
      getJedis().lpop(key(INFLIGHT, this.name, curQueue));
    }
  }

  /**
   * Handle an exception that was thrown from inside {@link #poll()}.
   *
   * @param curQueue the name of the queue that was being processed when the exception was thrown
   * @param ex the exception that was thrown
   */
  protected abstract void recoverFromException(final String curQueue, final Exception ex);

  @Override
  public boolean isShutdown() {
    return JobExecutor.State.SHUTDOWN.equals(this.state.get())
        || JobExecutor.State.SHUTDOWN_IMMEDIATE.equals(this.state.get());
  }

  @Override
  public boolean isPaused() {
    return this.paused.get();
  }

  @Override
  public boolean isProcessingJob() {
    return this.processingJob.get();
  }

  @Override
  public void togglePause(final boolean paused) {
    this.paused.set(paused);
    synchronized (this.paused) {
      this.paused.notifyAll();
    }
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public WorkerEventEmitter getWorkerEventEmitter() {
    return this.listenerDelegate;
  }

  @Override
  public Collection<String> getQueues() {
    return Collections.unmodifiableCollection(this.queueNames);
  }

  @Override
  public void addQueue(final String queueName) {
    if (queueName == null || "".equals(queueName)) {
      throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
    }
    this.queueNames.add(queueName);
  }

  @Override
  public void removeQueue(final String queueName, final boolean all) {
    if (queueName == null || "".equals(queueName)) {
      throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
    }
    if (all) { // Remove all instances
      boolean tryAgain = true;
      while (tryAgain) {
        tryAgain = this.queueNames.remove(queueName);
      }
    } else { // Only remove one instance
      this.queueNames.remove(queueName);
    }
  }

  @Override
  public void removeAllQueues() {
    this.queueNames.clear();
  }

  @Override
  public void setQueues(final Collection<String> queues) {
    checkQueues(queues);
    this.queueNames.clear();
    if (queues == ALL_QUEUES) { // Using object equality on purpose
      this.queueNames.addAll(getJedis().smembers(key(QUEUES)));
    } else {
      this.queueNames.addAll(queues);
    }
  }

  @Override
  public JobFactory getJobFactory() {
    return this.jobFactory;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    return this.exceptionHandlerRef.get();
  }

  @Override
  public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
    if (exceptionHandler == null) {
      throw new IllegalArgumentException("exceptionHandler must not be null");
    }
    this.exceptionHandlerRef.set(exceptionHandler);
  }

  public FailQueueStrategy getFailQueueStrategy() {
    return this.failQueueStrategyRef.get();
  }

  public void setFailQueueStrategy(final FailQueueStrategy failQueueStrategy) {
    if (failQueueStrategy == null) {
      throw new IllegalArgumentException("failQueueStrategy must not be null");
    }
    this.failQueueStrategyRef.set(failQueueStrategy);
  }

  @Override
  public void join(final long millis) throws InterruptedException {
    final Thread workerThread = this.threadRef.get();
    if (workerThread != null && workerThread.isAlive()) {
      workerThread.join(millis);
    }
  }

  protected boolean shouldSleep(final int missCount) {
    return (NextQueueStrategy.RESET_TO_HIGHEST_PRIORITY.equals(this.nextQueueStrategy)
        || (missCount >= this.queueNames.size()));
  }

  protected String getNextQueue() throws InterruptedException {
    return switch (this.nextQueueStrategy) {
      case DRAIN_WHILE_MESSAGES_EXISTS -> {
        final String nextPollQueue =
            this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
        if (nextPollQueue != null) {
          this.queueNames.add(nextPollQueue);
        }
        yield nextPollQueue;
      }
      case RESET_TO_HIGHEST_PRIORITY -> JesqueUtils.join(",", this.queueNames);
      default -> throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
    };
  }

  protected void poll() {
    int missCount = 0;
    String curQueue = null;
    while (JobExecutor.State.RUNNING.equals(this.state.get())) {
      try {
        if (threadNameChangingEnabled) {
          renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
        }
        curQueue = getNextQueue();
        if (curQueue != null) {
          checkPaused();
          if (JobExecutor.State.RUNNING.equals(this.state.get())) {
            this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
            final String payload = pop(curQueue);
            if (payload != null) {
              process(ObjectMapperFactory.get().readValue(payload, Job.class), curQueue);
              missCount = 0;
            } else {
              missCount++;
              if (shouldSleep(missCount) && JobExecutor.State.RUNNING.equals(this.state.get())) {
                missCount = 0;
                Thread.sleep(EMPTY_QUEUE_SLEEP_TIME);
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        if (!isShutdown()) {
          recoverFromException(curQueue, ie);
        }
      } catch (JsonParseException | JsonMappingException e) {
        removeInFlight(curQueue, true);
        recoverFromException(curQueue, e);
      } catch (Exception e) {
        recoverFromException(curQueue, e);
      }
    }
  }

  private void checkPaused() throws IOException {
    if (this.paused.get()) {
      synchronized (this.paused) {
        if (this.paused.get()) {
          getJedis().set(key(WORKER, name), pauseMsg());
        }
        while (this.paused.get()) {
          try {
            this.paused.wait();
          } catch (InterruptedException ie) {
            LOG.warn("Worker interrupted", ie);
          }
        }
        getJedis().del(key(WORKER, name));
      }
    }
  }

  /**
   * Materializes and executes the given job.
   *
   * @param job the Job to process
   * @param curQueue the queue the payload came from
   */
  protected void process(final Job job, final String curQueue) {
    boolean success = false;
    try {
      this.processingJob.set(true);
      if (threadNameChangingEnabled) {
        renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
      }
      this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null);
      getJedis().set(key(WORKER, name), statusMsg(curQueue, job));
      final Object instance = this.jobFactory.materializeJob(job);
      final Object result = execute(job, curQueue, instance);
      success(job, instance, result, curQueue);
      success = true;
    } catch (Throwable thrwbl) {
      failure(thrwbl, job, curQueue);
    } finally {
      removeInFlight(curQueue, success);
      getJedis().del(key(WORKER, name));
      this.processingJob.set(false);
    }
  }

  /**
   * Executes the given job.
   *
   * @param job the job to execute
   * @param curQueue the queue the job came from
   * @param instance the materialized job
   * @throws Exception if the instance is a {@link Callable} and throws an exception
   * @return result of the execution
   */
  protected Object execute(final Job job, final String curQueue, final Object instance)
      throws Exception {
    if (instance instanceof WorkerAware) {
      ((WorkerAware) instance).setWorker(this);
    }
    this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null);
    final Object result;
    if (instance instanceof Callable) {
      result = ((Callable<?>) instance).call();
    } else if (instance instanceof Runnable) {
      ((Runnable) instance).run();
      result = null;
    } else {
      throw new ClassCastException(
          "Instance must be a Runnable or a Callable: "
              + instance.getClass().getName()
              + " - "
              + instance);
    }
    return result;
  }

  /**
   * Update the status in Redis on success.
   *
   * @param job the Job that succeeded
   * @param runner the materialized Job
   * @param result the result of the successful execution of the Job
   * @param curQueue the queue the Job came from
   */
  protected void success(
      final Job job, final Object runner, final Object result, final String curQueue) {
    try {
      getJedis().incr(key(STAT, PROCESSED));
      getJedis().incr(key(STAT, PROCESSED, name));
    } catch (JedisException je) {
      LOG.warn("Error updating success stats for job=" + job, je);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.listenerDelegate.fireEvent(JOB_SUCCESS, this, curQueue, job, runner, result, null);
  }

  /**
   * Update the status in Redis on failure.
   *
   * @param thrwbl the Throwable that occurred
   * @param job the Job that failed
   * @param curQueue the queue the Job came from
   */
  protected void failure(final Throwable thrwbl, final Job job, final String curQueue) {
    try {
      getJedis().incr(key(STAT, FAILED));
      getJedis().incr(key(STAT, FAILED, this.name));
      final FailQueueStrategy strategy = this.failQueueStrategyRef.get();
      final String failQueueKey = strategy.getFailQueueKey(thrwbl, job, curQueue);
      if (failQueueKey != null) {
        final int failQueueMaxItems = strategy.getFailQueueMaxItems(curQueue);
        if (failQueueMaxItems > 0) {
          AbstractTransaction tx = createTransaction();
          tx.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
          tx.ltrim(failQueueKey, -failQueueMaxItems, -1);
          tx.exec();
        } else {
          getJedis().rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
        }
      }
    } catch (JedisException je) {
      LOG.warn("Error updating failure stats for throwable=" + thrwbl + " job=" + job, je);
    } catch (IOException ioe) {
      LOG.warn("Error serializing failure payload for throwable=" + thrwbl + " job=" + job, ioe);
    }
    this.listenerDelegate.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, thrwbl);
  }

  protected abstract AbstractTransaction createTransaction();

  protected abstract JedisCommands getJedis();

  /**
   * Create and serialize a JobFailure.
   *
   * @param thrwbl the Throwable that occurred
   * @param queue the queue the job came from
   * @param job the Job that failed
   * @return the JSON representation of a new JobFailure
   * @throws IOException if there was an error serializing the JobFailure
   */
  protected String failMsg(final Throwable thrwbl, final String queue, final Job job)
      throws IOException {
    final JobFailure failure = new JobFailure();
    failure.setFailedAt(new Date());
    failure.setWorker(this.name);
    failure.setQueue(queue);
    failure.setPayload(job);
    failure.setThrowable(thrwbl);
    return ObjectMapperFactory.get().writeValueAsString(failure);
  }

  /**
   * Create and serialize a WorkerStatus.
   *
   * @param queue the queue the Job came from
   * @param job the Job currently being processed
   * @return the JSON representation of a new WorkerStatus
   * @throws IOException if there was an error serializing the WorkerStatus
   */
  private String statusMsg(final String queue, final Job job) throws IOException {
    final WorkerStatus status = new WorkerStatus();
    status.setRunAt(new Date());
    status.setQueue(queue);
    status.setPayload(job);
    return ObjectMapperFactory.get().writeValueAsString(status);
  }

  /**
   * Create and serialize a paused WorkerStatus.
   *
   * @return the JSON representation of a new WorkerStatus
   * @throws IOException if there was an error serializing the WorkerStatus
   */
  private String pauseMsg() throws IOException {
    final WorkerStatus status = new WorkerStatus();
    status.setRunAt(new Date());
    status.setPaused(isPaused());
    return ObjectMapperFactory.get().writeValueAsString(status);
  }

  /**
   * Creates a unique name, suitable for use with Resque.
   *
   * @return a unique name for this worker
   */
  protected String createName() {
    final StringBuilder buf = new StringBuilder(128);
    try {
      buf.append(InetAddress.getLocalHost().getHostName())
          .append(COLON)
          .append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
          .append('-')
          .append(this.workerId)
          .append(COLON)
          .append(JAVA_DYNAMIC_QUEUES);
      for (final String queueName : this.queueNames) {
        buf.append(',').append(queueName);
      }
    } catch (UnknownHostException uhe) {
      throw new RuntimeException(uhe);
    }
    return buf.toString();
  }

  /**
   * Builds a namespaced Redis key with the given arguments.
   *
   * @param parts the key parts to be joined
   * @return an assembled String key
   */
  protected String key(final String... parts) {
    return JesqueUtils.createKey(config.getNamespace(), parts);
  }

  /**
   * Rename the current thread with the given message.
   *
   * @param msg the message to add to the thread name
   */
  protected void renameThread(final String msg) {
    Thread.currentThread().setName(this.threadNameBase + msg);
  }

  @Override
  public String toString() {
    return config.getNamespace() + COLON + WORKER + COLON + this.name;
  }
}
