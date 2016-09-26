package net.greghaines.jesque.worker;

/**
 * Force execution of job to be silently skipped. The job will be re-enqueued after a delay.
 *
 * This exception may be thrown by {@link WorkerListener}s when handling the event {@link WorkerEvent#JOB_PROCESS} or
 * during the execution of a job.
 */
public class DontPerformException extends RuntimeException {
    /**
     * Default constructor.
     */
    public DontPerformException() {
        super();
    }

    /**
     * Constructor with message.
     *
     * @param message Message
     */
    public DontPerformException(String message) {
        super(message);
    }
}
