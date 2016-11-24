package net.greghaines.jesque.worker;

/**
 * Strategies to determine the next queue a Worker will poll.
 * @author Ofir Naor (ofirnk)
 */
public enum NextQueueStrategy {
    /**
     * Drains messages as long as current queue is not empty
     */
    DRAIN_WHILE_MESSAGES_EXISTS,

    /**
     * Resets to check the first queue, then second queue, etc. after each message is processed.
     */
    RESET_TO_HIGHEST_PRIORITY
}