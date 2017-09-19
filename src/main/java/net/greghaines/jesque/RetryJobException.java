package net.greghaines.jesque;

/**
 * Created by dimav
 * on 14/09/17 22:23.
 */
public class RetryJobException extends RuntimeException{
    private final long delay;

    public RetryJobException(final String message, long delay) {
        super(message);
        this.delay = delay;
    }

    public long getDelay() {
        return delay;
    }
}
