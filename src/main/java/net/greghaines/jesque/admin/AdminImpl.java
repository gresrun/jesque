/*
 * Copyright 2012 Greg Haines
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
package net.greghaines.jesque.admin;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.JesqueUtils.set;
import static net.greghaines.jesque.utils.ResqueConstants.ADMIN_CHANNEL;
import static net.greghaines.jesque.utils.ResqueConstants.CHANNEL;
import static net.greghaines.jesque.worker.JobExecutor.State.NEW;
import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.JobExecutor.State.SHUTDOWN;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.admin.commands.PauseCommand;
import net.greghaines.jesque.admin.commands.ShutdownCommand;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.ConcurrentHashSet;
import net.greghaines.jesque.utils.ConcurrentSet;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import net.greghaines.jesque.worker.DefaultExceptionHandler;
import net.greghaines.jesque.worker.ExceptionHandler;
import net.greghaines.jesque.worker.JobFactory;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.RecoveryStrategy;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerAware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * AdminImpl receives administrative jobs for a worker.
 * 
 * @author Greg Haines
 */
public class AdminImpl implements Admin {
    
    private static final Logger LOG = LoggerFactory.getLogger(AdminImpl.class);
    private static final long RECONNECT_SLEEP_TIME = 5000; // 5s
    private static final int RECONNECT_ATTEMPTS = 120; // Total time: 10min

    protected final Jedis jedis;
    protected final String namespace;
    private final JobFactory jobFactory;
    private final ConcurrentSet<String> channels = new ConcurrentHashSet<String>();
    protected final PubSubListener jedisPubSub = new PubSubListener();
    protected final AtomicReference<Worker> workerRef = new AtomicReference<Worker>(null);
    protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
    private final AtomicBoolean processingJob = new AtomicBoolean(false);
    private final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null);
    private final AtomicReference<ExceptionHandler> exceptionHandlerRef = 
            new AtomicReference<ExceptionHandler>(new DefaultExceptionHandler());

    /**
     * Create a new AdminImpl which subscribes to {@link ResqueConstants#ADMIN_CHANNEL}, registers the 
     * {@link PauseCommand} and {@link ShutdownCommand} jobs, and creates a new Jedis connection.
     * @param config the Jesque configuration
     */
    public AdminImpl(final Config config) {
        this(config, set(ADMIN_CHANNEL), new MapBasedJobFactory(map(
                entry("PauseCommand", PauseCommand.class), 
                entry("ShutdownCommand", ShutdownCommand.class))));
    }

    /**
     * Create a new AdminImpl which creates a new Jedis connection.
     * @param config the Jesque configuration
     * @param channels the channels to subscribe to
     * @param jobFactory the job factory that materializes the jobs
     */
    public AdminImpl(final Config config, final Set<String> channels, final JobFactory jobFactory) {
        this(config, channels, jobFactory, new Jedis(config.getHost(), config.getPort(), config.getTimeout()));
    }

    /**
     * Create a new AdminImpl.
     * @param config the Jesque configuration
     * @param channels the channels to subscribe to
     * @param jobFactory the job factory that materializes the jobs
     * @param jedis the connection to Redis
     */
    public AdminImpl(final Config config, final Set<String> channels, final JobFactory jobFactory, final Jedis jedis) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (jobFactory == null) {
            throw new IllegalArgumentException("jobFactory must not be null");
        }
        if (jedis == null) {
            throw new IllegalArgumentException("jedis must not be null");
        }
        this.namespace = config.getNamespace();
        this.jedis = jedis;
        if (config.getPassword() != null) {
            this.jedis.auth(config.getPassword());
        }
        this.jedis.select(config.getDatabase());
        setChannels(channels);
        this.jobFactory = jobFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        if (this.state.compareAndSet(NEW, RUNNING)) {
            try {
                LOG.debug("AdminImpl starting up");
                this.threadRef.set(Thread.currentThread());
                while (!isShutdown()) {
                    this.jedis.subscribe(this.jedisPubSub, createFullChannels());
                }
            } finally {
                LOG.debug("AdminImpl shutting down");
                this.jedis.quit();
                this.threadRef.set(null);
            }
        } else {
            if (RUNNING.equals(this.state.get())) {
                throw new IllegalStateException("This AdminImpl is already running");
            } else {
                throw new IllegalStateException("This AdminImpl is shutdown");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getChannels() {
        return Collections.unmodifiableSet(this.channels);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setChannels(final Set<String> channels) {
        checkChannels(channels);
        this.channels.clear();
        this.channels.addAll(channels);
        if (this.jedisPubSub.isSubscribed()) {
            this.jedisPubSub.unsubscribe();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Worker getWorker() {
        return this.workerRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWorker(final Worker worker) {
        this.workerRef.set(worker);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end(final boolean now) {
        this.state.set(SHUTDOWN);
        this.jedisPubSub.unsubscribe();
        if (now) {
            final Thread workerThread = this.threadRef.get();
            if (workerThread != null) {
                workerThread.interrupt();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return SHUTDOWN.equals(this.state.get());
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
    public void join(final long millis) throws InterruptedException {
        final Thread workerThread = this.threadRef.get();
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.join(millis);
        }
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

    protected class PubSubListener extends JedisPubSub {
        /**
         * {@inheritDoc}
         */
        @Override
        public void onMessage(final String channel, final String message) {
            if (message != null) {
                try {
                    AdminImpl.this.processingJob.set(true);
                    final Job job = ObjectMapperFactory.get().readValue(message, Job.class);
                    execute(job, channel, AdminImpl.this.jobFactory.materializeJob(job));
                } catch (Exception e) {
                    recoverFromException(channel, e);
                } finally {
                    AdminImpl.this.processingJob.set(false);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPMessage(final String pattern, final String channel, final String message) {
        } // NOOP

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSubscribe(final String channel, final int subscribedChannels) {
        } // NOOP

        /**
         * {@inheritDoc}
         */
        @Override
        public void onUnsubscribe(final String channel, final int subscribedChannels) {
        } // NOOP

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPUnsubscribe(final String pattern, final int subscribedChannels) {
        } // NOOP

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPSubscribe(final String pattern, final int subscribedChannels) {
        } // NOOP
    }

    /**
     * Executes the given job.
     * 
     * @param job
     *            the job to execute
     * @param curQueue
     *            the queue the job came from
     * @param instance
     *            the materialized job
     * @return the result of the job execution
     * @throws Exception
     *             if the instance is a {@link Callable} and throws an exception
     */
    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        final Object result;
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this.workerRef.get());
        }
        if (instance instanceof Callable) {
            result = ((Callable<?>) instance).call(); // The job is executing!
        } else if (instance instanceof Runnable) {
            ((Runnable) instance).run(); // The job is executing!
            result = null;
        } else { // Should never happen since we're testing the class earlier
            throw new ClassCastException("instance must be a Runnable or a Callable: " + instance.getClass().getName() 
                    + " - " + instance);
        }
        return result;
    }

    /**
     * @return the number of times this Admin will attempt to reconnect to Redis
     *         before giving up
     */
    protected int getReconnectAttempts() {
        return RECONNECT_ATTEMPTS;
    }

    /**
     * Handle an exception that was thrown from inside
     * {@link PubSubListener#onMessage(String,String)}.
     * 
     * @param channel
     *            the name of the channel that was being processed when the
     *            exception was thrown
     * @param e
     *            the exception that was thrown
     */
    protected void recoverFromException(final String channel, final Exception e) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, e, channel);
        switch (recoveryStrategy) {
        case RECONNECT:
            LOG.info("Reconnecting to Redis in response to exception", e);
            final int reconAttempts = getReconnectAttempts();
            if (!JedisUtils.reconnect(this.jedis, reconAttempts, RECONNECT_SLEEP_TIME)) {
                LOG.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", e);
                end(false);
            } else {
                LOG.info("Reconnected to Redis");
            }
            break;
        case TERMINATE:
            LOG.warn("Terminating in response to exception", e);
            end(false);
            break;
        case PROCEED:
            break;
        default:
            LOG.error("Unknown RecoveryStrategy: " + recoveryStrategy 
                    + " while attempting to recover from the following exception; Admin proceeding...", e);
            break;
        }
    }

    /**
     * Verify that the given channels are all valid.
     * 
     * @param channels
     *            the given channels
     */
    protected static void checkChannels(final Iterable<String> channels) {
        if (channels == null) {
            throw new IllegalArgumentException("channels must not be null");
        }
        for (final String channel : channels) {
            if (channel == null || "".equals(channel)) {
                throw new IllegalArgumentException("channels' members must not be null: " + channels);
            }
        }
    }

    private String[] createFullChannels() {
        final String[] fullChannels = this.channels.toArray(new String[this.channels.size()]);
        int i = 0;
        for (final String channel : fullChannels) {
            fullChannels[i++] = JesqueUtils.createKey(this.namespace, CHANNEL, channel);
        }
        return fullChannels;
    }
}
