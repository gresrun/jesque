package net.greghaines.jesque.admin;

import net.greghaines.jesque.Job;

/**
 * An AdminClient publishes jobs to channels.
 * 
 * @author Greg Haines
 */
public interface AdminClient
{
	/**
	 * Publishes a job on a channel.
	 * 
	 * @param channel the channel to publish the job to
	 * @param job job the job to be published
	 * @throws IllegalArgumentException if the queue is null or empty or if the job is null
	 */
	void publish(String channel, Job job);

	/**
	 * Quits the connection to the Redis server.
	 */
	void end();
}
