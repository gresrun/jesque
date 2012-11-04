package net.greghaines.jesque.worker;

import java.util.Map;

public interface JobExecutor
{
	public enum State
	{
		/**
		 * The JobExecutor has not started running.
		 */
		NEW,
		/**
		 * The JobExecutor is currently running.
		 */
		RUNNING,
		/**
		 * The JobExecutor has shutdown.
		 */
		SHUTDOWN;
	}
	
	/**
	 * The allowed job names and types that this JobExecutor will execute.
	 * 
	 * @return an unmodifiable view of the allowed job names and types
	 */
	Map<String,Class<?>> getJobTypes();

	/**
	 * Allow the given job type to be executed.
	 * 
	 * @param jobName the job name as seen
	 * @param jobType the job type to allow
	 */
	void addJobType(String jobName, Class<?> jobType);

	/**
	 * Disallow the job type from being executed.
	 * 
	 * @param jobType the job type to disallow
	 */
	void removeJobType(Class<?> jobType);
	
	/**
	 * Disallow the job name from being executed.
	 * 
	 * @param jobName the job name to disallow
	 */
	void removeJobName(String jobName);

	/**
	 * Clear any current allowed job types and use the given set.
	 * 
	 * @param jobTypes the job types to allow
	 */
	void setJobTypes(Map<String,? extends Class<?>> jobTypes);
	
	/**
	 * The current exception handler.
	 * 
	 * @return the current exception handler.
	 */
	ExceptionHandler getExceptionHandler();
	
	/**
	 * Set this JobExecutor's exception handler to the given handler.
	 * 
	 * @param exceptionHandler the exception handler to use
	 */
	void setExceptionHandler(ExceptionHandler exceptionHandler);
	
	/**
	 * Shutdown this JobExecutor.
	 * 
	 * @param now if true, an effort will be made to stop any job in progress
	 */
	void end(boolean now);

	/**
	 * Returns whether this JobExecutor is either shutdown or in the process of shutting down.
	 * 
	 * @return whether this JobExecutor is either shutdown or in the process of shutting down.
	 */
	boolean isShutdown();

	/**
	 *  Wait for this JobExecutor to complete. A timeout of 0 means to wait forever.
	 * <p/>
	 * This method will only return after a thread has called {@link #end(boolean)}.
	 * 
	 * @param millis the time to wait in milliseconds
	 * @throws InterruptedException if any thread has interrupted the current thread. 
	 * The interrupted status of the current thread is cleared when this exception is thrown.
	 */
	void join(long millis) throws InterruptedException;
}
