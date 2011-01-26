package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

/**
 * A WorkerListener can register with a Worker to be notified of WorkerEvents.
 * 
 * @author Greg Haines
 * @see WorkerEvent
 */
public interface WorkerListener
{
	/**
	 * This method is called by the Worker upon the occurence of a registered WorkerEvent.
	 * 
	 * @param event the WorkerEvent that occured
	 * @param worker the Worker that the event occured in
	 * @param queue the queue the Worker is processing
	 * @param job the Job related to the event (only set for JOB_PROCESS, JOB_EXECUTE, JOB_SUCCESS, and JOB_FAILURE events)
	 * @param runner the materialized object that the Job specified (only set for JOB_EXECUTE and JOB_SUCCESS events)
	 * @param ex the Exception that caused the event (only set for JOB_FAILURE and ERROR events)
	 */
	void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Runnable runner, Exception ex);
}
