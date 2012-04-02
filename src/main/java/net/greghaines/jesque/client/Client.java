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
 */
public interface Client
{
	/**
	 * Queues a job in a given queue to be run.
	 * 
	 * @param queue the queue to add the Job to
	 * @param job the job to be enqueued
	 * @throws IllegalArgumentException if the queue is null or empty or if the job is null
	 */
	void enqueue(String queue, Job job);

	/**
	 * Queues a job with high priority in a given queue to be run.
	 * 
	 * @param queue the queue to add the Job to
	 * @param job the job to be enqueued
	 * @throws IllegalArgumentException if the queue is null or empty or if the job is null
	 */
	void priorityEnqueue(String queue, Job job);

	/**
	 * Quits the connection to the Redis server.
	 */
	void end();

	/**
	 * A non blocking lock utilizing redis to create a lock that can be utilized by distributed servers.
	 *      Call this method again to renew your lock before it expires if you wish to guarantee holding the lock.
	 * 
	 * @param lockName all calls to this method will contend for a unique lock with the name of lockName
	 * @param timeout millis until the lock will expire
	 * @param lockHolder a unique string used to tell if you are the current holder of a lock for both acquisition, and extension
	 * @return Whether or not the lock was acquired.
	 */
	boolean acquireLock(String lockName, String lockHolder, Integer timeout);
}
