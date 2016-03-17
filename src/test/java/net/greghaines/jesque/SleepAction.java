package net.greghaines.jesque;

import java.util.concurrent.Callable;

/**
 * An action that sleeps for the given number of milliseconds.
 *
 * @author Daniël de Kok
 */
public class SleepAction implements Callable<Void> {
    private final int millis;

    /**
     * Construct a sleep action.
     *
     * @param millis The number of milliseconds to sleep.
     */
    public SleepAction(int millis) {
        this.millis = millis;
    }
    @Override
    public Void call() throws Exception {
        Thread.sleep(millis);
        return null;
    }
}
