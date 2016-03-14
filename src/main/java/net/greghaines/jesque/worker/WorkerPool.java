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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkerPool creates a fixed number of identical <code>Workers</code>, each on a separate <code>Thread</code>.
 */
public class WorkerPool implements Worker, WorkerPoolMXBean {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);
    private static final String JMX_WORKER_POOL_NAME = "net.greghaines.jesque:type=WorkerPool";

    private final List<Worker> workers;
    private final List<Thread> threads;
    private final WorkerEventEmitter eventEmitter;

    private boolean isRegisterMbeans;

    /**
     * Create a WorkerPool with the given number of Workers, the default <code>ThreadFactory</code>
     * and disable JMX monitoring.
     *
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers the number of Workers to create
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers) {
        this(workerFactory, numWorkers, false);
    }

    /**
     * Create a WorkerPool with the given number of Workers, the default <code>ThreadFactory</code>
     * and whether to monitor by JMX or not.
     *
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers the number of Workers to create
     * @param isRegisterMbeans whether to monitor by JMX or not.
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
            final boolean isRegisterMbeans) {
        this(workerFactory, numWorkers, Executors.defaultThreadFactory(), isRegisterMbeans);
    }

    /**
     * Create a WorkerPool with the given number of Workers, the given <code>ThreadFactory</code>
     * and disable JMX monitoring.
     *
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers the number of Workers to create
     * @param threadFactory the factory to create pre-configured Threads
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
            final ThreadFactory threadFactory) {
        this(workerFactory, numWorkers, threadFactory, false);
    }

    /**
     * Create a WorkerPool with the given number of Workers, the given <code>ThreadFactory</code>
     * and whether to monitor by JMX or not.
     *
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers the number of Workers to create
     * @param threadFactory the factory to create pre-configured Threads
     * @param isRegisterMbeans whether to monitor by JMX or not.
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
            final ThreadFactory threadFactory, final boolean isRegisterMbeans) {
        this.workers = new ArrayList<Worker>(numWorkers);
        this.threads = new ArrayList<Thread>(numWorkers);
        this.eventEmitter = new WorkerPoolEventEmitter(this.workers);
        for (int i = 0; i < numWorkers; i++) {
            try {
                final Worker worker = workerFactory.call();
                this.workers.add(worker);
                this.threads.add(threadFactory.newThread(worker));
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        this.isRegisterMbeans = isRegisterMbeans;
        registerMBeans();
    }

    /**
     * Shutdown this pool and wait millis time per thread or until all threads are finished if millis is 0.
     * @param now if true, an effort will be made to stop any jobs in progress
     * @param millis the time to wait in milliseconds for the threads to join; a timeout of 0 means to wait forever.
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown.
     */
    public void endAndJoin(final boolean now, final long millis) throws InterruptedException {
        end(now);
        join(millis);
    }

    /**
     * Join to internal threads and wait millis time per thread or until all
     * threads are finished if millis is 0.
     * 
     * @param millis the time to wait in milliseconds for the threads to join; a timeout of 0 means to wait forever.
     * @throws InterruptedException if any thread has interrupted the current thread. 
     * The interrupted status of the current thread is cleared when this exception is thrown.
     */
    @Override
    public void join(final long millis) throws InterruptedException {
        for (final Thread thread : this.threads) {
            thread.join(millis);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        final StringBuilder sb = new StringBuilder(128 * this.threads.size());
        String prefix = "";
        for (final Worker worker : this.workers) {
            sb.append(prefix).append(worker.getName());
            prefix = " | ";
        }
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WorkerEventEmitter getWorkerEventEmitter() {
        return this.eventEmitter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        for (final Thread thread : this.threads) {
            thread.start();
        }
        Thread.yield();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end(final boolean now) {
        for (final Worker worker : this.workers) {
            worker.end(now);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return this.workers.get(0).isShutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPaused() {
        return this.workers.get(0).isPaused();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void togglePause(final boolean paused) {
        for (final Worker worker : this.workers) {
            worker.togglePause(paused);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isProcessingJob() {
        boolean processingJob = false;
        for (final Worker worker : this.workers) {
            processingJob |= worker.isProcessingJob();
            if (processingJob) {
                break;
            }
        }
        return processingJob;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobFactory getJobFactory() {
        return this.workers.get(0).getJobFactory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getQueues() {
        return this.workers.get(0).getQueues();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(final String queueName) {
        for (final Worker worker : this.workers) {
            worker.addQueue(queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(final String queueName, final boolean all) {
        for (final Worker worker : this.workers) {
            worker.removeQueue(queueName, all);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllQueues() {
        for (final Worker worker : this.workers) {
            worker.removeAllQueues();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueues(final Collection<String> queues) {
        for (final Worker worker : this.workers) {
            worker.setQueues(queues);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionHandler getExceptionHandler() {
        return this.workers.get(0).getExceptionHandler();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        for (final Worker worker : this.workers) {
            worker.setExceptionHandler(exceptionHandler);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTotalWorkers() {
        return this.workers.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getActiveWorkers() {
        int activeWorkers = 0;
        for (final Worker worker : this.workers) {
            if (worker.isProcessingJob()) {
                activeWorkers++;
            }
        }
        return activeWorkers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getIdleWorkers() {
        return getTotalWorkers() - getActiveWorkers();
    }

    /**
     * Enable JMX monitoring.
     */
    public void registerMBeans() {
        if (!this.isRegisterMbeans) {
            return;
        }

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        final ObjectName beanWorkerPoolName;
        try {
            beanWorkerPoolName = new ObjectName(JMX_WORKER_POOL_NAME);
            if (!mBeanServer.isRegistered(beanWorkerPoolName)) {
                mBeanServer.registerMBean(this, beanWorkerPoolName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to register management beans.", e);
        }
    }

    /**
     * Disable JMX monitoring.
     */
    public void unregisterMBeans() {
        if (!this.isRegisterMbeans) {
            return;
        }

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        final ObjectName beanWorkerPoolName;
        try {
            beanWorkerPoolName = new ObjectName(JMX_WORKER_POOL_NAME);
            if (mBeanServer.isRegistered(beanWorkerPoolName)) {
                mBeanServer.unregisterMBean(beanWorkerPoolName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister management beans.", e);
        }
    }

    /**
     * @return whether to monitor by JMX or not
     */
    public boolean isRegisterMbeans() {
        return this.isRegisterMbeans;
    }

    /**
     * Set whether to monitor by JMX or not
     *
     * @param isRegisterMbeans whether to monitor by JMX or not
     */
    public void setRegisterMbeans(final boolean isRegisterMbeans) {
        this.isRegisterMbeans = isRegisterMbeans;
    }

    private static class WorkerPoolEventEmitter implements WorkerEventEmitter {
        
        private final List<Worker> workers;
        
        /**
         * Constructor.
         * @param workers the workers to manage
         */
        public WorkerPoolEventEmitter(final List<Worker> workers) {
            this.workers = workers;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addListener(final WorkerListener listener) {
            for (final Worker worker : this.workers) {
                worker.getWorkerEventEmitter().addListener(listener);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addListener(final WorkerListener listener, final WorkerEvent... events) {
            for (final Worker worker : this.workers) {
                worker.getWorkerEventEmitter().addListener(listener, events);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void removeListener(final WorkerListener listener) {
            for (final Worker worker : this.workers) {
                worker.getWorkerEventEmitter().removeListener(listener);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void removeListener(final WorkerListener listener, final WorkerEvent... events) {
            for (final Worker worker : this.workers) {
                worker.getWorkerEventEmitter().removeListener(listener, events);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void removeAllListeners() {
            for (final Worker worker : this.workers) {
                worker.getWorkerEventEmitter().removeAllListeners();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void removeAllListeners(final WorkerEvent... events) {
            for (final Worker worker : this.workers) {
                worker.getWorkerEventEmitter().removeAllListeners(events);
            }
        }
    }
}
