package net.greghaines.jesque.worker;

/**
 * A WorkerEventEmitter allows WorkerListeners to register for WorkerEvents.
 * 
 * @author Greg Haines
 */
public interface WorkerEventEmitter
{
	/**
	 * Register a WorkerListener for all WorkerEvents.
	 * 
	 * @param listener the WorkerListener to register
	 */
	void addListener(WorkerListener listener);
	/**
	 * Register a WorkerListener for the specified WorkerEvents.
	 * 
	 * @param listener the WorkerListener to register
	 * @param events the WorkerEvents to be notified of
	 */
	void addListener(WorkerListener listener, WorkerEvent... events);
	/**
	 * Unregister a WorkerListener for all WorkerEvents.
	 * 
	 * @param listener the WorkerListener to unregister
	 */
	void removeListener(WorkerListener listener);
	/**
	 * Unregister a WorkerListener for the specified WorkerEvents.
	 * 
	 * @param listener the WorkerListener to unregister
	 * @param events the WorkerEvents to no longer be notified of
	 */
	void removeListener(WorkerListener listener, WorkerEvent... events);
	/**
	 * Unregister all WorkerListeners for all WorkerEvents.
	 */
	void removeAllListeners();
	/**
	 * Unregister all WorkerListeners for the specified WorkerEvents.
	 * 
	 * @param events the WorkerEvents to no longer be notified of
	 */
	void removeAllListeners(WorkerEvent... events);
}
