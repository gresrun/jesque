package net.greghaines.jesque.queue;

/**
 * Queue DAO.
 */
public interface QueueDao {
   /**
    * Enqueue job.
    *
    * @param queue
    *           the Resque queue name
    * @param jobJson
    *           the job serialized as JSON
    */
   void enqueue(final String queue, final String jobJson) throws Exception;

   /**
    * Priority enqueue job.
    *
    * @param queue
    *           the Resque queue name
    * @param jobJson
    *           the job serialized as JSON
    */
   void priorityEnqueue(final String queue, final String jobJson) throws Exception;

   void delayedEnqueue(final String queue, final String jobJson, final long future) throws Exception;

   void removeDelayedEnqueue(final String queue, final String jobJson) throws Exception;

   void recurringEnqueue(final String queue, final String jobJson, final long future, final long frequency) throws Exception;

   void removeRecurringEnqueue(final String queue, final String jobJson) throws Exception;

   /**
    * Helper method that encapsulates the logic to acquire a lock.
    *
    * @param lockName
    *            all calls to this method will contend for a unique lock with
    *            the name of lockName
    * @param timeout
    *            seconds until the lock will expire
    * @param lockHolder
    *            a unique string used to tell if you are the current holder of
    *            a lock for both acquisition, and extension
    * @return Whether or not the lock was acquired.
    */
   boolean acquireLock(final String lockName, final String lockHolder, final int timeout) throws Exception;
}
