package net.greghaines.jesque.worker;

/**
 * Force the execution of a job to be silently skipped.
 *
 * This exception may be thrown by {@link WorkerListener}s when handling
 * {@link WorkerEvent#JOB_PROCESS} or {@link WorkerEvent#JOB_EXECUTE}.
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
