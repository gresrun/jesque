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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.greghaines.jesque.ConfigFO;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisException;

/**
 * Extends WorkerImpl for failback.
 * 
 * @author Greg Haines
 * @author Animesh Kumar <smile.animesh@gmail.com>
 * @author Heebyung <heebyung@gmail.com>
 */
public class FailoverWorkerImpl extends WorkerImpl {
    
    private static final Logger LOG = LoggerFactory.getLogger(FailoverWorkerImpl.class);
    
    public int emptyFailoverQueueSleepTimeMs;
    
    /**
     * Verify that the given queues are all valid.
     * 
     * @param queues
     *            the given queues
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
     * Creates a new FailoverWorkerImpl, which creates it's own connection to Redis
     * using values from the config. The worker will only listen to the supplied
     * failover-queues.
     * 
     * @param config
     *            used to create a connection to Redis and the package prefix
     *            for incoming jobs
     * @param queues
     *            the list of failover-queues to poll
     */
    public FailoverWorkerImpl(final ConfigFO config, final Collection<String> queues) {
        super(config, queues, 
                new MapBasedJobFactory(JesqueUtils.map(
                        JesqueUtils.entry("EmptyAction", EmptyAction.class))));
        this.emptyFailoverQueueSleepTimeMs = config.getEmptyFailoverQueueSleepTimeSec()*1000;
    }
    
    public FailoverWorkerImpl(final ConfigFO config) {
        this(config, config.getFailoverQueues());
    }
    
    /**
     * Starts this worker. Registers the worker in Redis and begins polling the
     * failover-queues for failback. Stop this worker by calling end() on any thread.
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
                poll();
            } finally {
                renameThread("STOPPING");
                this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
                this.jedis.srem(key(WORKERS), this.name);
                this.jedis.del(key(WORKER, this.name), key(WORKER, this.name, STARTED), key(STAT, FAILED, this.name),
                        key(STAT, PROCESSED, this.name));
                this.jedis.quit();
                this.threadRef.set(null);
            }
        } else {
            if (RUNNING.equals(this.state.get())) {
                throw new IllegalStateException("This FailoverWorkerImpl is already running");
            } else {
                throw new IllegalStateException("This FailoverWorkerImpl is shutdown");
            }
        }
    }
    
    /**
     * Polls the failover-queues for failover-data, and failback them if failoverTimeout exceed.
     */
    protected void poll() {
        int missCount = 0;
        String curQueue = null;
        while (RUNNING.equals(this.state.get())) {
            try {
                if (threadNameChangingEnabled) {
                    renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
                }
                curQueue = this.queueNames.poll(emptyFailoverQueueSleepTimeMs, TimeUnit.MILLISECONDS);
                if (curQueue != null) {
                    this.queueNames.add(curQueue); // Rotate the queues
                    checkPaused(); 
                    // Might have been waiting in poll()/checkPaused() for a while
                    if (RUNNING.equals(this.state.get())) {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final Long result = failback(curQueue);
                        
                        if (result != null) {
                            missCount = 0;
                            if (result == 0) { 
                                success(curQueue);
                            } else {
                                failure(curQueue);
                            }
                        } else if (++missCount >= this.queueNames.size() && RUNNING.equals(this.state.get())) {
                            // Keeps worker from busy-spinning on empty queues
                            missCount = 0;
                            Thread.sleep(emptyFailoverQueueSleepTimeMs);
                        }
                    }
                }
            } catch (InterruptedException ie) {
                if (!isShutdown()) {
                    recoverFromException(curQueue, ie);
                }
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }

    /**
     * Handle an exception that was thrown from inside {@link #poll()}
     * 
     * @param curQueue
     *            the name of the queue that was being processed when the
     *            exception was thrown
     * @param e
     *            the exception that was thrown
     */
    protected void recoverFromException(final String curQueue, final Exception e) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, e, curQueue);
        switch (recoveryStrategy) {
        case RECONNECT:
            LOG.info("Reconnecting to Redis in response to exception", e);
            final int reconAttempts = getReconnectAttempts();
            if (!JedisUtils.reconnect(this.jedis, reconAttempts, RECONNECT_SLEEP_TIME)) {
                LOG.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", e);
                end(false);
            } else {
                authenticateAndSelectDB();
                LOG.info("Reconnected to Redis");
            }
            break;
        case TERMINATE:
            LOG.warn("Terminating in response to exception", e);
            end(false);
            break;
        case PROCEED:
            this.listenerDelegate.fireEvent(WORKER_ERROR, this, curQueue, null, null, null, e);
            break;
        default:
            LOG.error("Unknown RecoveryStrategy: " + recoveryStrategy
                    + " while attempting to recover from the following exception; worker proceeding...", e);
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
     * Update the status in Redis on success.
     * 
     * @param curQueue
     *            the queue the failover-data came from
     */    
    private void success(final String curQueue) {
        if (threadNameChangingEnabled) {
            renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
        }
        try {
            this.jedis.incr(key(STAT, PROCESSED));
            this.jedis.incr(key(STAT, PROCESSED, this.name));
        } catch (JedisException je) {
            LOG.warn("Error updating success stats", je);
        }
    }
    
    /**
     * Update the status in Redis on failure.
     * 
     * @param curQueue
     *            the queue the failover-data came from
     */    
    private void failure(final String curQueue) {
        if (threadNameChangingEnabled) {
            renameThread("Processing " + curQueue + " since " + System.currentTimeMillis());
        }
        try {
            this.jedis.incr(key(STAT, FAILED));
            this.jedis.incr(key(STAT, FAILED, this.name));
        } catch (JedisException je) {
            LOG.warn("Error updating failure stats", je);
        }
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
                    .append('-').append(this.workerId).append(COLON).append(FAILOVER_QUEUES);
            for (final String queueName : this.queueNames) {
                buf.append(',').append(queueName);
            }
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
        return buf.toString();
    }

    /**
     * Load lua-script to redis.
     * 
     */
    protected void scriptLoad() {
        keyToScriptSha.put(FAILBACK, this.jedis.scriptLoad(FAILBACK_SCRIPT));
    }
    
    /**
     * Performing failback using given failoverQueue.
     * 
     * @param failoverQueue
     *            the queue the failover-data came from
     */
    protected Long failback(String failoverQueue) {
        return (Long)this.jedis.evalsha(keyToScriptSha.get(FAILBACK), 2, 
                failoverQueue, 
                String.valueOf(System.currentTimeMillis()));     
    }

    public int getEmptyFailoverQueueSleepTimeMs() {
        return emptyFailoverQueueSleepTimeMs;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.namespace + COLON + WORKER + COLON + this.name;
    }
    
    /**
     * Empty job for passing null-check of jobFactory in WorkerImpl.
     * 
     */
    public static class EmptyAction implements Runnable {
        private static final Logger LOG = LoggerFactory.getLogger(EmptyAction.class);
        private final String s;

        public EmptyAction(String s) {
            this.s = s;
        }

        public void run() {
            LOG.debug("EmptyAction.run() {} ", this.s);
        }
    }
}
