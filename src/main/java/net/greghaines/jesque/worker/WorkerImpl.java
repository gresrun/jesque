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

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.worker.JobExecutor.State.*;
import static net.greghaines.jesque.worker.WorkerEvent.*;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ScriptUtils;
import net.greghaines.jesque.utils.VersionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

/**
 * WorkerImpl is an implementation of the Worker interface. Obeys the contract of a Resque worker in Redis.
 */
public class WorkerImpl implements Worker {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerImpl.class);
    private static final AtomicLong WORKER_COUNTER = new AtomicLong(0);
    protected static final long EMPTY_QUEUE_SLEEP_TIME = 500; // 500 ms
    protected static final long RECONNECT_SLEEP_TIME = 5000; // 5 sec
    protected static final int RECONNECT_ATTEMPTS = 120; // Total time: 10 min
    private static final String LPOPLPUSH_LUA = "/workerScripts/jesque_lpoplpush.lua";
    private static final String POP_LUA = "/workerScripts/jesque_pop.lua";
    private static final String POP_FROM_MULTIPLE_PRIO_QUEUES = "/workerScripts/fromMultiplePriorityQueues.lua";

    // Set the thread name to the message for debugging
    private static volatile boolean threadNameChangingEnabled = false;
    private final NextQueueStrategy nextQueueStrategy;

    /**
     * @return true if worker threads names will change during normal operation
     */
    public static boolean isThreadNameChangingEnabled() {
        return threadNameChangingEnabled;
    }

    /**
     * Enable/disable worker thread renaming during normal operation. (Disabled by default)
     * <p>
     * <strong>Warning: Enabling this feature is very expensive CPU-wise!</strong><br>
     * This feature is designed to assist in debugging worker state but should be disabled in production environments
     * for performance reasons.
     * </p>
     * 
     * @param enabled whether threads' names should change during normal operation
     */
    public static void setThreadNameChangingEnabled(final boolean enabled) {
        threadNameChangingEnabled = enabled;
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

    protected final Config config;
    protected final Jedis jedis;
    protected final String namespace;
    protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<String>();
    private final String name;
    protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate();
    protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean processingJob = new AtomicBoolean(false);
    private final AtomicReference<String> popScriptHash = new AtomicReference<>(null);
    private final AtomicReference<String> lpoplpushScriptHash = new AtomicReference<>(null);
    private final AtomicReference<String> multiPriorityQueuesScriptHash = new AtomicReference<>(null);
    private final long workerId = WORKER_COUNTER.getAndIncrement();
    private final String threadNameBase = "Worker-" + this.workerId + " Jesque-" + VersionUtils.getVersion() + ": ";
    private final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null);
    private final AtomicReference<ExceptionHandler> exceptionHandlerRef = new AtomicReference<ExceptionHandler>(
            new DefaultExceptionHandler());
    private final AtomicReference<FailQueueStrategy> failQueueStrategyRef;
    private final JobFactory jobFactory;

    /**
     * Creates a new WorkerImpl, which creates it's own connection to Redis using values from the config.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @throws IllegalArgumentException if either config, queues or jobFactory is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory) {
        this(config, queues, jobFactory, new Jedis(config.getHost(), config.getPort(), config.getTimeout()));
    }

    /**
     * Creates a new WorkerImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * Uses the DRAIN_WHILE_MESSAGES_EXISTS NextQueueStrategy.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedis the connection to Redis
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedis is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory,
            final Jedis jedis) {
        this(config, queues, jobFactory, jedis, NextQueueStrategy.DRAIN_WHILE_MESSAGES_EXISTS);
    }

    /**
     * Creates a new WorkerImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     * 
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedis the connection to Redis
     * @param nextQueueStrategy defines worker behavior once it has found messages in a queue
     * @throws IllegalArgumentException if either config, queues, jobFactory, jedis or nextQueueStrategy is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory,
            final Jedis jedis, final NextQueueStrategy nextQueueStrategy) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (jobFactory == null) {
            throw new IllegalArgumentException("jobFactory must not be null");
        }
        if (jedis == null) {
            throw new IllegalArgumentException("jedis must not be null");
        }
        if (nextQueueStrategy == null) {
            throw new IllegalArgumentException("nextQueueStrategy must not be null");
        }
        checkQueues(queues);
        this.nextQueueStrategy = nextQueueStrategy;
        this.config = config;
        this.jobFactory = jobFactory;
        this.namespace = config.getNamespace();
        this.jedis = jedis;
        this.failQueueStrategyRef = new AtomicReference<FailQueueStrategy>(
                new DefaultFailQueueStrategy(this.namespace));
        authenticateAndSelectDB();
        setQueues(queues);
        this.name = createName();
    }

    /**
     * @return this worker's identifier
     */
    public long getWorkerId() {
        return this.workerId;
    }

    /**
     * Starts this worker. Registers the worker in Redis and begins polling the queues for jobs.<br>
     * Stop this worker by calling end() on any thread.
     */
    @Override
    public void run() {
        if (this.state.compareAndSet(NEW, RUNNING)) {
            try {
                renameThread("RUNNING");
                this.threadRef.set(Thread.currentThread());
                this.jedis.sadd(key(WORKERS), this.name);
                this.jedis.set(key(WORKER, this.name, STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));
                this.listenerDelegate.fireEvent(WORKER_START, this, null, null, null, null, null);
                this.popScriptHash.set(this.jedis.scriptLoad(ScriptUtils.readScript(POP_LUA)));
                this.lpoplpushScriptHash.set(this.jedis.scriptLoad(ScriptUtils.readScript(LPOPLPUSH_LUA)));
                this.multiPriorityQueuesScriptHash
                        .set(this.jedis.scriptLoad(ScriptUtils.readScript(POP_FROM_MULTIPLE_PRIO_QUEUES)));
                poll();
            } catch (Exception ex) {
                LOG.error("Uncaught exception in worker run-loop!", ex);
                this.listenerDelegate.fireEvent(WORKER_ERROR, this, null, null, null, null, ex);
            } finally {
                renameThread("STOPPING");
                this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
                this.jedis.srem(key(WORKERS), this.name);
                this.jedis.del(key(WORKER, this.name), key(WORKER, this.name, STARTED), key(STAT, FAILED, this.name),
                        key(STAT, PROCESSED, this.name));
                this.jedis.quit();
                this.threadRef.set(null);
            }
        } else if (RUNNING.equals(this.state.get())) {
            throw new IllegalStateException("This WorkerImpl is already running");
        } else {
            throw new IllegalStateException("This WorkerImpl is shutdown");
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return SHUTDOWN.equals(this.state.get()) || SHUTDOWN_IMMEDIATE.equals(this.state.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPaused() {
        return this.paused.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isProcessingJob() {
        return this.processingJob.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void togglePause(final boolean paused) {
        this.paused.set(paused);
        synchronized (this.paused) {
            this.paused.notifyAll();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WorkerEventEmitter getWorkerEventEmitter() {
        return this.listenerDelegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getQueues() {
        return Collections.unmodifiableCollection(this.queueNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(final String queueName) {
        if (queueName == null || "".equals(queueName)) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName);
        }
        this.queueNames.add(queueName);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllQueues() {
        this.queueNames.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueues(final Collection<String> queues) {
        checkQueues(queues);
        this.queueNames.clear();
        this.queueNames.addAll((queues == ALL_QUEUES) // Using object equality on purpose
                ? this.jedis.smembers(key(QUEUES)) // Like '*' in other clients
                : queues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobFactory getJobFactory() {
        return this.jobFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionHandler getExceptionHandler() {
        return this.exceptionHandlerRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (exceptionHandler == null) {
            throw new IllegalArgumentException("exceptionHandler must not be null");
        }
        this.exceptionHandlerRef.set(exceptionHandler);
    }

    /**
     * @return the current FailQueueStrategy
     */
    public FailQueueStrategy getFailQueueStrategy() {
        return this.failQueueStrategyRef.get();
    }

    /**
     * @param failQueueStrategy the new FailQueueStrategy to use
     */
    public void setFailQueueStrategy(final FailQueueStrategy failQueueStrategy) {
        if (failQueueStrategy == null) {
            throw new IllegalArgumentException("failQueueStrategy must not be null");
        }
        this.failQueueStrategyRef.set(failQueueStrategy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void join(final long millis) throws InterruptedException {
        final Thread workerThread = this.threadRef.get();
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.join(millis);
        }
    }

    /**
     * @return the number of times this Worker will attempt to reconnect to Redis before giving up
     */
    protected int getReconnectAttempts() {
        return RECONNECT_ATTEMPTS;
    }

    /**
     * Polls the queues for jobs and executes them.
     */
    protected void poll() {
        int missCount = 0;
        String curQueue = null;
        while (RUNNING.equals(this.state.get())) {
            try {
                if (threadNameChangingEnabled) {
                    renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
                }
                curQueue = getNextQueue();
                if (curQueue != null) {
                    checkPaused();
                    // Might have been waiting in poll()/checkPaused() for a while
                    if (RUNNING.equals(this.state.get())) {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final String payload = pop(curQueue);
                        if (payload != null) {
                            process(ObjectMapperFactory.get().readValue(payload, Job.class), curQueue);
                            missCount = 0;
                        } else {
                            missCount++;
                            if (shouldSleep(missCount) && RUNNING.equals(this.state.get())) {
                                // Keeps worker from busy-spinning on empty queues
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
                // If the job JSON is not deserializable, we never want to submit it again...
                removeInFlight(curQueue);
                recoverFromException(curQueue, e);
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    private boolean shouldSleep(final int missCount) {
        return (NextQueueStrategy.RESET_TO_HIGHEST_PRIORITY.equals(this.nextQueueStrategy)
                || (missCount >= this.queueNames.size()));
    }

    protected String getNextQueue() throws InterruptedException {
        final String nextQueue;
        switch (this.nextQueueStrategy) {
        case DRAIN_WHILE_MESSAGES_EXISTS:
            final String nextPollQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
            if (nextPollQueue != null) {
                // Rotate the queues
                this.queueNames.add(nextPollQueue);
            }
            nextQueue = nextPollQueue;
            break;
        case RESET_TO_HIGHEST_PRIORITY:
            nextQueue = JesqueUtils.join(",", this.queueNames);
            break;
        default:
            throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
        }
        return nextQueue;
    }

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
        switch (nextQueueStrategy) {
            case DRAIN_WHILE_MESSAGES_EXISTS:
                return (String) this.jedis.evalsha(this.popScriptHash.get(), 3, key, inflightKey,
                        JesqueUtils.createRecurringHashKey(key), now);
            case RESET_TO_HIGHEST_PRIORITY:
                return (String) this.jedis.evalsha(this.multiPriorityQueuesScriptHash.get(), 2, curQueue, inflightKey, now);
            default:
                throw new RuntimeException("Unimplemented 'nextQueueStrategy'");
        }
    }

    /**
     * Handle an exception that was thrown from inside {@link #poll()}.
     * 
     * @param curQueue the name of the queue that was being processed when the exception was thrown
     * @param ex the exception that was thrown
     */
    protected void recoverFromException(final String curQueue, final Exception ex) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, ex, curQueue);
        switch (recoveryStrategy) {
        case RECONNECT:
            LOG.info("Reconnecting to Redis in response to exception", ex);
            final int reconAttempts = getReconnectAttempts();
            if (!JedisUtils.reconnect(this.jedis, reconAttempts, RECONNECT_SLEEP_TIME)) {
                LOG.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", ex);
                end(false);
            } else {
                authenticateAndSelectDB();
                LOG.info("Reconnected to Redis");
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
            LOG.error("Unknown RecoveryStrategy: " + recoveryStrategy
                    + " while attempting to recover from the following exception; worker proceeding...", ex);
            break;
        }
    }

    private void authenticateAndSelectDB() {
        if (this.config.getPassword() != null) {
            this.jedis.auth(this.config.getPassword());
        }
        this.jedis.select(this.config.getDatabase());
    }

    /**
     * Checks to see if worker is paused. If so, wait until unpaused.
     * 
     * @throws IOException if there was an error creating the pause message
     */
    protected void checkPaused() throws IOException {
        if (this.paused.get()) {
            synchronized (this.paused) {
                if (this.paused.get()) {
                    this.jedis.set(key(WORKER, this.name), pauseMsg());
                }
                while (this.paused.get()) {
                    try {
                        this.paused.wait();
                    } catch (InterruptedException ie) {
                        LOG.warn("Worker interrupted", ie);
                    }
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
    protected void process(final Job job, final String curQueue) {
        try {
            this.processingJob.set(true);
            if (threadNameChangingEnabled) {
                renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
            }
            this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null);
            this.jedis.set(key(WORKER, this.name), statusMsg(curQueue, job));
            final Object instance = this.jobFactory.materializeJob(job);
            final Object result = execute(job, curQueue, instance);
            success(job, instance, result, curQueue);
        } catch (Throwable thrwbl) {
            failure(thrwbl, job, curQueue);
        } finally {
            removeInFlight(curQueue);
            this.jedis.del(key(WORKER, this.name));
            this.processingJob.set(false);
        }
    }

    private void removeInFlight(final String curQueue) {
        if (SHUTDOWN_IMMEDIATE.equals(this.state.get())) {
            lpoplpush(key(INFLIGHT, this.name, curQueue), key(QUEUE, curQueue));
        } else {
            this.jedis.lpop(key(INFLIGHT, this.name, curQueue));
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
    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this);
        }
        this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null);
        final Object result;
        if (instance instanceof Callable) {
            result = ((Callable<?>) instance).call(); // The job is executing!
        } else if (instance instanceof Runnable) {
            ((Runnable) instance).run(); // The job is executing!
            result = null;
        } else { // Should never happen since we're testing the class earlier
            throw new ClassCastException(
                    "Instance must be a Runnable or a Callable: " + instance.getClass().getName() + " - " + instance);
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
    protected void success(final Job job, final Object runner, final Object result, final String curQueue) {
        // The job may have taken a long time; make an effort to ensure the
        // connection is OK
        JedisUtils.ensureJedisConnection(this.jedis);
        try {
            this.jedis.incr(key(STAT, PROCESSED));
            this.jedis.incr(key(STAT, PROCESSED, this.name));
        } catch (JedisException je) {
            LOG.warn("Error updating success stats for job=" + job, je);
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
        // The job may have taken a long time; make an effort to ensure the connection is OK
        JedisUtils.ensureJedisConnection(this.jedis);
        try {
            this.jedis.incr(key(STAT, FAILED));
            this.jedis.incr(key(STAT, FAILED, this.name));
            final FailQueueStrategy strategy = this.failQueueStrategyRef.get();
            final String failQueueKey = strategy.getFailQueueKey(thrwbl, job, curQueue);
            if (failQueueKey != null) {
                final int failQueueMaxItems = strategy.getFailQueueMaxItems(curQueue);
                if (failQueueMaxItems > 0) {
                    Long currentItems = this.jedis.llen(failQueueKey);
                    if (currentItems >= failQueueMaxItems) {
                        Transaction tx = this.jedis.multi();
                        tx.ltrim(failQueueKey, 1, -1);
                        tx.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                        tx.exec();
                    }
                } else {
                    this.jedis.rpush(failQueueKey, failMsg(thrwbl, curQueue, job));
                }
            }
        } catch (JedisException je) {
            LOG.warn("Error updating failure stats for throwable=" + thrwbl + " job=" + job, je);
        } catch (IOException ioe) {
            LOG.warn("Error serializing failure payload for throwable=" + thrwbl + " job=" + job, ioe);
        }
        this.listenerDelegate.fireEvent(JOB_FAILURE, this, curQueue, job, null, null, thrwbl);
    }

    /**
     * Create and serialize a JobFailure.
     * 
     * @param thrwbl the Throwable that occurred
     * @param queue the queue the job came from
     * @param job the Job that failed
     * @return the JSON representation of a new JobFailure
     * @throws IOException if there was an error serializing the JobFailure
     */
    protected String failMsg(final Throwable thrwbl, final String queue, final Job job) throws IOException {
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
    protected String statusMsg(final String queue, final Job job) throws IOException {
        final WorkerStatus status = new WorkerStatus();
        status.setRunAt(new Date());
        status.setQueue(queue);
        status.setPayload(job);
        return ObjectMapperFactory.get().writeValueAsString(status);
    }

    /**
     * Create and serialize a WorkerStatus for a pause event.
     * 
     * @return the JSON representation of a new WorkerStatus
     * @throws IOException if there was an error serializing the WorkerStatus
     */
    protected String pauseMsg() throws IOException {
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
            buf.append(InetAddress.getLocalHost().getHostName()).append(COLON)
                    .append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
                    .append('-').append(this.workerId).append(COLON).append(JAVA_DYNAMIC_QUEUES);
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
        return JesqueUtils.createKey(this.namespace, parts);
    }

    /**
     * Rename the current thread with the given message.
     * 
     * @param msg the message to add to the thread name
     */
    protected void renameThread(final String msg) {
        Thread.currentThread().setName(this.threadNameBase + msg);
    }

    protected String lpoplpush(final String from, final String to) {
        return (String) this.jedis.evalsha(this.lpoplpushScriptHash.get(), 2, from, to);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.namespace + COLON + WORKER + COLON + this.name;
    }
}
