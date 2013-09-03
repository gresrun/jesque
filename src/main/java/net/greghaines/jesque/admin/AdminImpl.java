package net.greghaines.jesque.admin;

import static net.greghaines.jesque.worker.JobExecutor.State.NEW;
import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.JobExecutor.State.SHUTDOWN;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.JesqueUtils.set;
import static net.greghaines.jesque.utils.ResqueConstants.ADMIN_CHANNEL;
import static net.greghaines.jesque.utils.ResqueConstants.CHANNEL;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import net.greghaines.jesque.worker.DefaultExceptionHandler;
import net.greghaines.jesque.worker.ExceptionHandler;
import net.greghaines.jesque.worker.RecoveryStrategy;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerAware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class AdminImpl implements Admin {
    
    private static final Logger log = LoggerFactory.getLogger(AdminImpl.class);
    private static final long reconnectSleepTime = 5000; // 5s
    private static final int reconnectAttempts = 120; // Total time: 10min

    protected final Jedis jedis;
    protected final String namespace;
    private final ConcurrentMap<String, Class<?>> jobTypes = new ConcurrentHashMap<String, Class<?>>();
    private final ConcurrentSet<String> channels = new ConcurrentHashSet<String>();
    protected final PubSubListener jedisPubSub = new PubSubListener();
    protected final AtomicReference<Worker> workerRef = new AtomicReference<Worker>(null);
    protected final AtomicReference<State> state = new AtomicReference<State>(NEW);
    private final AtomicBoolean processingJob = new AtomicBoolean(false);
    private final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null);
    private final AtomicReference<ExceptionHandler> exceptionHandlerRef = new AtomicReference<ExceptionHandler>(new DefaultExceptionHandler());

    @SuppressWarnings("unchecked")
    public AdminImpl(final Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.namespace = config.getNamespace();
        this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        if (config.getPassword() != null) {
            this.jedis.auth(config.getPassword());
        }
        this.jedis.select(config.getDatabase());
        setChannels(set(ADMIN_CHANNEL));
        setJobTypes(map(entry("PauseCommand", PauseCommand.class), entry("ShutdownCommand", ShutdownCommand.class)));
    }

    public AdminImpl(final Config config, final Set<String> channels, final Map<String, ? extends Class<?>> jobTypes) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.namespace = config.getNamespace();
        this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        if (config.getPassword() != null) {
            this.jedis.auth(config.getPassword());
        }
        this.jedis.select(config.getDatabase());
        setChannels(channels);
        setJobTypes(jobTypes);
    }

    public void run() {
        if (this.state.compareAndSet(NEW, RUNNING)) {
            try {
                log.debug("AdminImpl starting up");
                this.threadRef.set(Thread.currentThread());
                while (!this.isShutdown()) {
                    this.jedis.subscribe(this.jedisPubSub, createFullChannels());
                }
            } finally {
                log.debug("AdminImpl shutting down");
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

    public Set<String> getChannels() {
        return Collections.unmodifiableSet(this.channels);
    }

    public void setChannels(final Set<String> channels) {
        checkChannels(channels);
        this.channels.clear();
        this.channels.addAll(channels);
        if (this.jedisPubSub.isSubscribed()) {
            this.jedisPubSub.unsubscribe();
        }
    }

    public Worker getWorker() {
        return this.workerRef.get();
    }

    public void setWorker(final Worker worker) {
        this.workerRef.set(worker);
    }

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

    public boolean isShutdown() {
        return SHUTDOWN.equals(this.state.get());
    }

    public boolean isProcessingJob() {
        return this.processingJob.get();
    }

    public void join(final long millis) throws InterruptedException {
        final Thread workerThread = this.threadRef.get();
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.join(millis);
        }
    }

    public Map<String, Class<?>> getJobTypes() {
        return Collections.unmodifiableMap(this.jobTypes);
    }

    public void addJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null");
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null");
        }
        if (!(Runnable.class.isAssignableFrom(jobType)) && !(Callable.class.isAssignableFrom(jobType))) {
            throw new IllegalArgumentException("jobType must implement either Runnable or Callable: " + jobType);
        }
        this.jobTypes.put(jobName, jobType);
    }

    public void removeJobType(final Class<?> jobType) {
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null");
        }
        this.jobTypes.values().remove(jobType);
    }

    public void removeJobName(final String jobName) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null");
        }
        this.jobTypes.remove(jobName);
    }

    public void setJobTypes(final Map<String, ? extends Class<?>> jobTypes) {
        checkJobTypes(jobTypes);
        this.jobTypes.clear();
        for (final Entry<String, ? extends Class<?>> entry : jobTypes.entrySet()) {
            addJobType(entry.getKey(), entry.getValue());
        }
    }

    public ExceptionHandler getExceptionHandler() {
        return this.exceptionHandlerRef.get();
    }

    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (exceptionHandler == null) {
            throw new IllegalArgumentException("exceptionHandler must not be null");
        }
        this.exceptionHandlerRef.set(exceptionHandler);
    }

    protected class PubSubListener extends JedisPubSub {
        
        public void onMessage(final String channel, final String message) {
            if (message != null) {
                try {
                    AdminImpl.this.processingJob.set(true);
                    final Job job = ObjectMapperFactory.get().readValue(message, Job.class);
                    execute(job, channel, JesqueUtils.materializeJob(job, AdminImpl.this.jobTypes));
                } catch (Exception e) {
                    recoverFromException(channel, e);
                } finally {
                    AdminImpl.this.processingJob.set(false);
                }
            }
        }

        public void onPMessage(final String pattern, final String channel, final String message) {
        } // NOOP

        public void onSubscribe(final String channel, final int subscribedChannels) {
        } // NOOP

        public void onUnsubscribe(final String channel, final int subscribedChannels) {
        } // NOOP

        public void onPUnsubscribe(final String pattern, final int subscribedChannels) {
        } // NOOP

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
            throw new ClassCastException("instance must be a Runnable or a Callable: " + instance.getClass().getName() + " - " + instance);
        }
        return result;
    }

    /**
     * @return the number of times this Admin will attempt to reconnect to Redis
     *         before giving up
     */
    protected int getReconnectAttempts() {
        return reconnectAttempts;
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
            log.info("Reconnecting to Redis in response to exception", e);
            final int reconAttempts = getReconnectAttempts();
            if (!JedisUtils.reconnect(this.jedis, reconAttempts, reconnectSleepTime)) {
                log.warn("Terminating in response to exception after " + reconAttempts + " to reconnect", e);
                end(false);
            } else {
                log.info("Reconnected to Redis");
            }
            break;
        case TERMINATE:
            log.warn("Terminating in response to exception", e);
            end(false);
            break;
        case PROCEED:
            break;
        default:
            log.error("Unknown RecoveryStrategy: " + recoveryStrategy + " while attempting to recover from the following exception; Admin proceeding...", e);
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

    /**
     * Verify the given job types are all valid.
     * 
     * @param jobTypes
     *            the given job types
     */
    protected void checkJobTypes(final Map<String, ? extends Class<?>> jobTypes) {
        if (jobTypes == null) {
            throw new IllegalArgumentException("jobTypes must not be null");
        }
        for (final Entry<String, ? extends Class<?>> entry : jobTypes.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("jobType's keys must not be null: " + jobTypes);
            }
            final Class<?> jobType = entry.getValue();
            if (jobType == null) {
                throw new IllegalArgumentException("jobType's values must not be null: " + jobTypes);
            }
            if (!(Runnable.class.isAssignableFrom(jobType)) && !(Callable.class.isAssignableFrom(jobType))) {
                throw new IllegalArgumentException("jobType's values must implement either Runnable or Callable: " + jobTypes);
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
