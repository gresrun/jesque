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
	 * Send a shutdown command on the {@link ResqueConstants.ADMIN_CHANNEL} channel.
	 * 
	 * @param now if true, an effort will be made to stop any job in progress
	 */
	void shutdownWorkers(boolean now);
	
	/**
	 * Send a shutdown command on the given channel.
	 * 
	 * @param channel the channel to publish the pause command to
	 * @param now if true, an effort will be made to stop any job in progress
	 */
	void shutdownWorkers(String channel, boolean now);
	
	/**
	 * Send a pause command on the {@link ResqueConstants.ADMIN_CHANNEL} channel.
	 * 
	 * @param paused if true, the workers will not process any new jobs; 
	 * if false, the workers will process new jobs
	 */
	void togglePausedWorkers(boolean paused);
	
	/**
	 * Send a pause command on the given channel.
	 * 
	 * @param channel the channel to publish the pause command to
	 * @param paused if true, the workers will not process any new jobs; 
	 * if false, the workers will process new jobs
	 */
	void togglePausedWorkers(String channel, boolean paused);
	
	/**
	 * Publishes a job on the {@link ResqueConstants.ADMIN_CHANNEL} channel.
	 * 
	 * @param job job the job to be published
	 * @throws IllegalArgumentException if the job is null
	 */
	void publish(Job job);
	
	/**
	 * Publishes a job on the given channel.
	 * 
	 * @param channel the channel to publish the job to
	 * @param job job the job to be published
	 * @throws IllegalArgumentException if the channel is null or empty or if the job is null
	 */
	void publish(String channel, Job job);

	/**
	 * Quits the connection to the Redis server.
	 */
	void end();
}
