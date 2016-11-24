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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * WorkerPool creates a fixed number of identical <code>Workers</code>, each on a separate <code>Thread</code>.
 */
public class WorkerPool implements Worker {
    
    private final List<Worker> workers;
    private final List<Thread> threads;
    private final WorkerEventEmitter eventEmitter;

    /**
     * Create a WorkerPool with the given number of Workers and the default <code>ThreadFactory</code>.
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers the number of Workers to create
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers) {
        this(workerFactory, numWorkers, Executors.defaultThreadFactory());
    }

    /**
     * Create a WorkerPool with the given number of Workers and the given <code>ThreadFactory</code>.
     * @param workerFactory a Callable that returns an implementation of Worker
     * @param numWorkers the number of Workers to create
     * @param threadFactory the factory to create pre-configured Threads
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
            final ThreadFactory threadFactory) {
        this.workers = new ArrayList<>(numWorkers);
        this.threads = new ArrayList<>(numWorkers);
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
     * @return the number of total workers
     */
    public int getWorkerCount() {
        return this.workers.size();
    }

    /**
     * @return the number of workers processing jobs
     */
    public int getActiveWorkerCount() {
        int activeWorkers = 0;
        for (final Worker worker : this.workers) {
            if (worker.isProcessingJob()) {
                activeWorkers++;
            }
        }
        return activeWorkers;
    }

    /**
     * @return the number of workers not processing jobs
     */
    public int getIdleWorkerCount() {
        return getWorkerCount() - getActiveWorkerCount();
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
