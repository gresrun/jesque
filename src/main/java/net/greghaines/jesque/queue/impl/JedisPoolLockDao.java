package net.greghaines.jesque.queue.impl;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.queue.LockDao;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * Jedis pool based {@link LockDao}.
 */
public class JedisPoolLockDao extends AbstractLockDao {
    /**
     * Jedis pool.
     */
    private final Pool<Jedis> jedisPool;

    /**
     * Constructor.
     *
     * @param config Config.
     * @param jedisPool Jedis pool.
     */
    public JedisPoolLockDao(Config config, Pool<Jedis> jedisPool) {
        super(config);
        this.jedisPool = jedisPool;
    }

    @Override
    protected <V> V doWithJedis(PoolWork<Jedis, V> work) throws Exception {
        return PoolUtils.doWorkInPool(jedisPool, work);
    }
}