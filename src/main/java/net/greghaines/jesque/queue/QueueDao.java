package net.greghaines.jesque.queue;

import net.greghaines.jesque.Job;

/**
 * Queue DAO.
 */
public interface QueueDao {
   /**
    * Enqueue job.
    *
    * @param queue
    *           the Resque queue name
    * @param job
    *           Job
    */
   void enqueue(final String queue, final Job job) throws Exception;

   /**
    * Priority enqueue job.
    *
    * @param queue
    *           the Resque queue name
    * @param job
    *           Job
    */
   void priorityEnqueue(final String queue, final Job job) throws Exception;

   void delayedEnqueue(final String queue, final Job job, final long future) throws Exception;

   void removeDelayedEnqueue(final String queue, final Job job) throws Exception;

   void recurringEnqueue(final String queue, final Job job, final long future, final long frequency) throws Exception;

   void removeRecurringEnqueue(final String queue, final Job job) throws Exception;

   /**
    * Remove a job from the given queue.
    *
    * @param queue the queue to remove a job from
    * @return job of a job or null if there was nothing to de-queue
    */
   Job dequeue(final String workerName, final String queue) throws Exception;

   void removeInflight(final String workerName, final String queue) throws Exception;

   void restoreInflight(final String workerName, final String queue) throws Exception;
}
