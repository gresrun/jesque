package net.greghaines.jesque;

import java.util.concurrent.Callable;

/**
 * An action that sleeps for the given number of milliseconds and may throw an exception.
 *
 * @author Florian Langenhahn
 */
public class SleepWithExceptionAction implements Callable<Void> {

    private final int millis;

    /**
     * Construct a sleep action.
     *
     * @param millis The number of milliseconds to sleep.
     */
    public SleepWithExceptionAction(int millis) {
        this.millis = millis;
    }

    @Override
    public Void call() throws Exception {
        Thread.sleep(millis);
        return null;
    }
}
