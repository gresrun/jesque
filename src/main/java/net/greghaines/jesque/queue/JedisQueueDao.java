package net.greghaines.jesque.queue;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;

import java.io.Closeable;

/**
 * Jedis based {@link QueueDao}.
 */
public class JedisQueueDao extends AbstractQueueDao implements Closeable {
    /**
     * Jedis.
     */
    private final Jedis jedis;

    /**
     * Always check connection before using it?.
     */
    private final boolean checkConnectionBeforeUse;

    /**
     * Constructor.
     *
     * @param config Config.
     */
    public JedisQueueDao(Config config, boolean checkConnectionBeforeUse) {
        super(config);
        this.checkConnectionBeforeUse = checkConnectionBeforeUse;
        this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        authenticateAndSelectDB();
    }

    @Override
    protected <V> V doWithJedis(PoolWork<Jedis, V> work) throws Exception {
        if (this.checkConnectionBeforeUse) {
            ensureJedisConnection();
        }
        return work.doWork(jedis);
    }

    /**
     * Check connection before using it.
     */
    public void ensureJedisConnection() {
        if (!JedisUtils.ensureJedisConnection(this.jedis)) {
            authenticateAndSelectDB();
        }
    }

    /**
     * Connect to Redis.
     */
    private void authenticateAndSelectDB() {
        if (this.config.getPassword() != null) {
            this.jedis.auth(this.config.getPassword());
        }
        this.jedis.select(this.config.getDatabase());
    }

    @Override
    public void close() {
        jedis.close();
    }
}
