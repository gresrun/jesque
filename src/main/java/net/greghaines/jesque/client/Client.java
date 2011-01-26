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
	 * Quits the connection to the Redis server.
	 */
	void end();
}
