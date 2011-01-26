package net.greghaines.jesque.worker;

/**
 * The possible WorkerEvents that a WorkerListener may register for.
 * 
 * @author Greg Haines
 */
public enum WorkerEvent
{
	/**
	 * The Worker is polling the queue.
	 */
	POLL,
	/**
	 * The Worker is processing a Job.
	 */
	JOB_PROCESS,
	/**
	 * The Worker is about to execute a materialized Job.
	 */
	JOB_EXECUTE,
	/**
	 * The Worker successfully executed a materialized Job.
	 */
	JOB_SUCCESS,
	/**
	 * The Worker caught an Exception during the execution of a materialized Job.
	 */
	JOB_FAILURE,
	/**
	 * The Worker caught an Exception during normal operation.
	 */
	ERROR;
}
