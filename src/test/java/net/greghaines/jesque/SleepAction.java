package net.greghaines.jesque;

/**
 * An action that sleeps for the given number of milliseconds.
 *
 * @author Daniël de Kok
 */
public class SleepAction implements Runnable {
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
    public void run() {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }
}
