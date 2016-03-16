package net.greghaines.jesque.queue.impl;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.queue.QueueDao;
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
