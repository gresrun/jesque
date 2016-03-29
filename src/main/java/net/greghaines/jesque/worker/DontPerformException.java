package net.greghaines.jesque.worker;

/**
 * Force the execution of a job to be silently skipped.
 *
 * This exception may be thrown by {@link WorkerListener}s when handling the event {@link WorkerEvent#JOB_PROCESS}.
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
