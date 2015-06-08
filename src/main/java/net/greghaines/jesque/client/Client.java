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
package net.greghaines.jesque.client;

import net.greghaines.jesque.Job;

/**
 * A Client allows Jobs to be enqueued for execution by Workers.
 * 
 * @author Greg Haines
 * @author Animesh Kumar
 */
public interface Client {
    
    /**
     * Queues a job in a given queue to be run.
     * 
     * @param queue
     *            the queue to add the Job to
     * @param job
     *            the job to be enqueued
     * @throws IllegalArgumentException
     *             if the queue is null or empty or if the job is null
     */
    void enqueue(String queue, Job job);

    /**
     * Queues a job with high priority in a given queue to be run.
     * 
     * @param queue
     *            the queue to add the Job to
     * @param job
     *            the job to be enqueued
     * @throws IllegalArgumentException
     *             if the queue is null or empty or if the job is null
     */
    void priorityEnqueue(String queue, Job job);

    /**
     * Quits the connection to the Redis server.
     */
    void end();

    /**
     * Acquire a non-blocking distributed lock. Calling this method again renews
     * the lock.
     * 
     * @param lockName
     *            the name of the lock to acquire
     * @param timeout
     *            number of seconds until the lock will expire
     * @param lockHolder
     *            a unique string identifying the caller
     * @return true, if the lock was acquired, false otherwise
     */
    boolean acquireLock(String lockName, String lockHolder, int timeout);

    /**
     * Queues a job in a given queue to be run in the future.
     * 
     * @param queue
     *            the queue to add the Job to
     * @param job
     *            the job to be enqueued
     * @param future
     *            timestamp when the job will run
     * @throws IllegalArgumentException
     *             if the queue is null or empty, if the job is null or if the
     *             timestamp is not in the future
     */
    void delayedEnqueue(String queue, Job job, long future);

    /**
     * Removes a queued future job.
     *
     * @param queue
     *            the queue to remove the Job from
     * @param job
     *            the job to be removed
     * @throws IllegalArgumentException
     *             if the queue is null or empty, if the job is null
     */
    void removeDelayedEnqueue(String queue, Job job);

    /**
     * Queues a job to be in the future and recur
     *
     * @param queue
     *          the queue to add the Job too
     * @param job
     *          the job to be enqueued
     * @param future
     *          timestamp when the job will run
     * @param frequency
     *          frequency in millis how often the job will run
     * @throws IllegalArgumentException
     *          if the queue is null or empty, if the job is null
     */
    void recurringEnqueue(String queue, Job job, long future, long frequency);

    /**
     * Removes a queued recurring job.
     *
     * @param queue
     *            the queue to remove the Job from
     * @param job
     *            the job to be removed
     * @throws IllegalArgumentException
     *             if the queue is null or empty, if the job is null
     */
    void removeRecurringEnqueue(String queue, Job job);
}
