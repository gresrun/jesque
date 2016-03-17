package net.greghaines.jesque.utils;

/**
 * Helper for sleeping.
 */
public class Sleep {
    /**
     * Sleep.
     *
     * @param ms Sleep time in milliseconds.
     */
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
