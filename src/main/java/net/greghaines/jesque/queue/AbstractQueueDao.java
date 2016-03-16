package net.greghaines.jesque.queue;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.utils.ScriptUtils.readScript;

/**
 * Base implementation of {@link QueueDao}.
 */
public abstract class AbstractQueueDao implements QueueDao {
    /**
     * Config.
     */
    protected final Config config;

    /**
     * Constructor.
     * 
     * @param config Config.
     */
    public AbstractQueueDao(Config config) {
        this.config = config;
    }

    @Override
    public void enqueue(final String queue, final String jobJson) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                jedis.sadd(queues(), queue);
                jedis.rpush(queue(queue), jobJson);
                return null;
            }
        });
    }

    @Override
    public void priorityEnqueue(final String queue, final String jobJson) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                jedis.sadd(queues(), queue);
                jedis.lpush(queue(queue), jobJson);
                return null;
            }
        });
    }

    @Override
    public void delayedEnqueue(final String queue, final String jobJson, final long future) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                final String key = queue(queue);
                // Add task only if this queue is either delayed or unused
                if (JedisUtils.canUseAsDelayedQueue(jedis, key)) {
                    jedis.zadd(key, future, jobJson);
                    jedis.sadd(queues(), queue);
                } else {
                    throw new IllegalArgumentException(queue + " cannot be used as a delayed queue");
                }
                return null;
            }
        });
    }

    @Override
    public void removeDelayedEnqueue(final String queue, final String jobJson) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                final String key = queue(queue);
                // remove task only if this queue is either delayed or unused
                if (JedisUtils.canUseAsDelayedQueue(jedis, key)) {
                    jedis.zrem(key, jobJson);
                } else {
                    throw new IllegalArgumentException(queue + " cannot be used as a delayed queue");
                }
                return null;
            }
        });
    }

    @Override
    public void recurringEnqueue(final String queue, final String jobJson, final long future, final long frequency) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                final String queueKey = queue(queue);
                final String hashKey = JesqueUtils.createRecurringHashKey(queueKey);

                if (JedisUtils.canUseAsRecurringQueue(jedis, queueKey, hashKey)) {
                    Transaction transaction = jedis.multi();
                    transaction.zadd(queueKey, future, jobJson);
                    transaction.hset(hashKey, jobJson, String.valueOf(frequency));
                    if (transaction.exec() == null) {
                        throw new RuntimeException("cannot add " + jobJson + " to recurring queue " + queue);
                    }
                } else {
                    throw new IllegalArgumentException(queue + " cannot be used as a recurring queue");
                }
                return null;
            }
        });
    }

    @Override
    public void removeRecurringEnqueue(final String queue, final String jobJson) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                final String queueKey = queue(queue);
                final String hashKey = JesqueUtils.createRecurringHashKey(queueKey);

                if (JedisUtils.canUseAsRecurringQueue(jedis, queueKey, hashKey)) {
                    Transaction transaction = jedis.multi();
                    transaction.hdel(hashKey, jobJson);
                    transaction.zrem(queueKey, jobJson);
                    if (transaction.exec() == null) {
                        throw new RuntimeException("cannot remove " + jobJson + " from recurring queue " + queue);
                    }
                } else {
                    throw new IllegalArgumentException(queue + " cannot be used as a recurring queue");
                }
                return null;
            }
        });
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
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                    } // Ignore interruptions
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

    @Override
    public String dequeue(final String workerName, final String queue) throws Exception {
        return doWithJedis(new PoolWork<Jedis, String>() {
            @Override
            public String doWork(Jedis jedis) throws Exception {
                String popScriptHash = jedis.scriptLoad(readScript("/workerScripts/jesque_pop.lua"));
                String popKey = queue(queue);
                String pushInflight = inflight(workerName, queue);
                return (String) jedis.evalsha(popScriptHash, 3, popKey, pushInflight,
                        JesqueUtils.createRecurringHashKey(popKey), Long.toString(System.currentTimeMillis()));
            }
        });
    }

    @Override
    public void removeInflight(final String workerName, final String queue) throws Exception {
        doWithJedis(new PoolWork<Jedis, Void>() {
            @Override
            public Void doWork(Jedis jedis) throws Exception {
                String popInflight = inflight(workerName, queue);
                jedis.lpop(popInflight);
                return null;
            }
        });
    }

    @Override
    public void restoreInflight(final String workerName, final String queue) throws Exception {
        doWithJedis(new PoolWork<Jedis, String>() {
            @Override
            public String doWork(Jedis jedis) throws Exception {
                String lpoplpushScriptHash = jedis.scriptLoad(readScript("/workerScripts/jesque_lpoplpush.lua"));
                String popInflight = inflight(workerName, queue);
                String pushKey = queue(queue);
                return (String) jedis.evalsha(lpoplpushScriptHash, 2, popInflight, pushKey);
            }
        });
    }

    /**
     * Key of queues.
     */
    private String queues() {
        return createKey(config.getNamespace(), QUEUES);
    }

    /**
     * Key of queue.
     */
    private String queue(String queue) {
        return createKey(config.getNamespace(), QUEUE, queue);
    }

    /**
     * Key of inflight queue.
     */
    private String inflight(String workerName, String queue) {
        return createKey(config.getNamespace(), INFLIGHT, workerName, queue);
    }

    protected abstract <V> V doWithJedis(PoolWork<Jedis, V> work) throws Exception;
}
