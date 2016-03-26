package net.greghaines.jesque;

import net.greghaines.jesque.worker.DontPerformException;

/**
 * An action that just throws a {@link DontPerformException}.
 */
public class DontPerformAction implements Runnable {
    /**
     * Flag to detect whether a job has been executed.
     */
    private static volatile boolean executed = false;

    /**
     * Constructor. Resets execution flag.
     */
    public DontPerformAction() {
        executed = false;
    }

    @Override
    public void run() {
        executed = true;
        throw new DontPerformException("test");
    }

    /**
     * Wait for a job to get executed.
     *
     * @throws InterruptedException
     */
    public static void waitForExecution() throws InterruptedException {
        while (!executed) {
            Thread.sleep(100);
        }
    }
}
