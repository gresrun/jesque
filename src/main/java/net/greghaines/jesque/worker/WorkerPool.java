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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Creates a fixed number of identical <code>Workers</code>, each on a separate
 * <code>Thread</code>.
 * 
 * @author Greg Haines
 */
public class WorkerPool implements Worker {
    
    private final List<Worker> workers;
    private final List<Thread> threads;

    /**
     * Create a WorkerPool with the given number of Workers and the default
     * <code>ThreadFactory</code>.
     * 
     * @param workerFactory
     *            a Callable that returns an implementation of Worker
     * @param numWorkers
     *            the number of Workers to create
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers) {
        this(workerFactory, numWorkers, Executors.defaultThreadFactory());
    }

    /**
     * Create a WorkerPool with the given number of Workers and the given
     * <code>ThreadFactory</code>.
     * 
     * @param workerFactory
     *            a Callable that returns an implementation of Worker
     * @param numWorkers
     *            the number of Workers to create
     * @param threadFactory
     *            the factory to create pre-configured Threads
     */
    public WorkerPool(final Callable<? extends Worker> workerFactory, final int numWorkers,
            final ThreadFactory threadFactory) {
        this.workers = new ArrayList<Worker>(numWorkers);
        this.threads = new ArrayList<Thread>(numWorkers);
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
     * Shutdown this pool and wait millis time per thread or until all threads
     * are finished if millis is 0.
     * 
     * @param now
     *            if true, an effort will be made to stop any jobs in progress
     * @param millis
     *            the time to wait in milliseconds for the threads to join; a
     *            timeout of 0 means to wait forever.
     * @throws InterruptedException
     *             if any thread has interrupted the current thread. The
     *             interrupted status of the current thread is cleared when this
     *             exception is thrown.
     */
    public void endAndJoin(final boolean now, final long millis) throws InterruptedException {
        end(now);
        join(millis);
    }

    /**
     * Join to internal threads and wait millis time per thread or until all
     * threads are finished if millis is 0.
     * 
     * @param millis
     *            the time to wait in milliseconds for the threads to join; a
     *            timeout of 0 means to wait forever.
     * @throws InterruptedException
     *             if any thread has interrupted the current thread. The
     *             interrupted status of the current thread is cleared when this
     *             exception is thrown.
     */
    public void join(final long millis) throws InterruptedException {
        for (final Thread thread : this.threads) {
            thread.join(millis);
        }
    }

    public String getName() {
        final StringBuilder sb = new StringBuilder(128 * this.threads.size());
        String prefix = "";
        for (final Worker worker : this.workers) {
            sb.append(prefix).append(worker.getName());
            prefix = " | ";
        }
        return sb.toString();
    }

    public void run() {
        for (final Thread thread : this.threads) {
            thread.start();
        }
        Thread.yield();
    }

    public void addListener(final WorkerListener listener) {
        for (final Worker worker : this.workers) {
            worker.addListener(listener);
        }
    }

    public void addListener(final WorkerListener listener, final WorkerEvent... events) {
        for (final Worker worker : this.workers) {
            worker.addListener(listener, events);
        }
    }

    public void removeListener(final WorkerListener listener) {
        for (final Worker worker : this.workers) {
            worker.removeListener(listener);
        }
    }

    public void removeListener(final WorkerListener listener, final WorkerEvent... events) {
        for (final Worker worker : this.workers) {
            worker.removeListener(listener, events);
        }
    }

    public void removeAllListeners() {
        for (final Worker worker : this.workers) {
            worker.removeAllListeners();
        }
    }

    public void removeAllListeners(final WorkerEvent... events) {
        for (final Worker worker : this.workers) {
            worker.removeAllListeners(events);
        }
    }

    public void end(final boolean now) {
        for (final Worker worker : this.workers) {
            worker.end(now);
        }
    }

    public boolean isShutdown() {
        return this.workers.get(0).isShutdown();
    }

    public boolean isPaused() {
        return this.workers.get(0).isPaused();
    }

    public void togglePause(final boolean paused) {
        for (final Worker worker : this.workers) {
            worker.togglePause(paused);
        }
    }

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

    public Collection<String> getQueues() {
        return this.workers.get(0).getQueues();
    }

    public void addQueue(final String queueName) {
        for (final Worker worker : this.workers) {
            worker.addQueue(queueName);
        }
    }

    public void removeQueue(final String queueName, final boolean all) {
        for (final Worker worker : this.workers) {
            worker.removeQueue(queueName, all);
        }
    }

    public void removeAllQueues() {
        for (final Worker worker : this.workers) {
            worker.removeAllQueues();
        }
    }

    public void setQueues(final Collection<String> queues) {
        for (final Worker worker : this.workers) {
            worker.setQueues(queues);
        }
    }

    public Map<String, Class<?>> getJobTypes() {
        return this.workers.get(0).getJobTypes();
    }

    public void addJobType(final String jobName, final Class<?> jobType) {
        for (final Worker worker : this.workers) {
            worker.addJobType(jobName, jobType);
        }
    }

    public void removeJobType(final Class<?> jobType) {
        for (final Worker worker : this.workers) {
            worker.removeJobType(jobType);
        }
    }

    public void removeJobName(final String jobName) {
        for (final Worker worker : this.workers) {
            worker.removeJobName(jobName);
        }
    }

    public void setJobTypes(final Map<String, ? extends Class<?>> jobTypes) {
        for (final Worker worker : this.workers) {
            worker.setJobTypes(jobTypes);
        }
    }

    public ExceptionHandler getExceptionHandler() {
        return this.workers.get(0).getExceptionHandler();
    }

    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        for (final Worker worker : this.workers) {
            worker.setExceptionHandler(exceptionHandler);
        }
    }
}
