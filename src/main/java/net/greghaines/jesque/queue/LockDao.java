package net.greghaines.jesque.queue;

/**
 * Lock DAO.
 */
public interface LockDao {
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
