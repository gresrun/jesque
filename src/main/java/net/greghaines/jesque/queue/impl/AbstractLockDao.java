package net.greghaines.jesque.queue.impl;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.queue.LockDao;
import net.greghaines.jesque.queue.QueueDao;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import net.greghaines.jesque.utils.Sleep;
import redis.clients.jedis.Jedis;

import static net.greghaines.jesque.utils.JesqueUtils.createKey;

/**
 * Base implementation of {@link QueueDao}.
 */
public abstract class AbstractLockDao implements LockDao {
    /**
     * Config.
     */
    protected final Config config;

    /**
     * Constructor.
     *
     * @param config Config.
     */
    public AbstractLockDao(Config config) {
        this.config = config;
    }

    @Override
    public boolean acquireLock(final String lockName, final String lockHolder, final int timeout) throws Exception {
        return doWithJedis(new PoolWork<Jedis, Boolean>() {
            @Override
            public Boolean doWork(Jedis jedis) throws Exception {
                final String key = createKey(config.getNamespace(), lockName);
                // If lock already exists, extend it
                String existingLockHolder = jedis.get(key);
                if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                    if (jedis.expire(key, timeout) == 1) {
                        existingLockHolder = jedis.get(key);
                        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                            return true;
                        }
                    }
                }
                // Check to see if the key exists and is expired for cleanup purposes
                if (jedis.exists(key) && (jedis.ttl(key) < 0)) {
                    // It is expired, but it may be in the process of being created, so
                    // sleep and check again
                    Sleep.sleep(2000);
                    if (jedis.ttl(key) < 0) {
                        existingLockHolder = jedis.get(key);
                        // If it is our lock mark the time to live
                        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                            if (jedis.expire(key, timeout) == 1) {
                                existingLockHolder = jedis.get(key);
                                if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                                    return true;
                                }
                            }
                        } else { // The key is expired, whack it!
                            jedis.del(key);
                        }
                    } else { // Someone else locked it while we were sleeping
                        return false;
                    }
                }
                // Ignore the cleanup steps above, start with no assumptions test
                // creating the key
                if (jedis.setnx(key, lockHolder) == 1) {
                    // Created the lock, now set the expiration
                    if (jedis.expire(key, timeout) == 1) { // Set the timeout
                        existingLockHolder = jedis.get(key);
                        if ((existingLockHolder != null) && existingLockHolder.equals(lockHolder)) {
                            return true;
                        }
                    } else { // Don't know why it failed, but for now just report failed
                        // acquisition
                        return false;
                    }
                }
                // Failed to create the lock
                return false;
            }
        });
    }

    protected abstract <V> V doWithJedis(PoolWork<Jedis, V> work) throws Exception;
}
