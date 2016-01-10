package net.greghaines.jesque.utils;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.ResqueConstants.DELAYED_QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE_TYPES;
import static net.greghaines.jesque.utils.ResqueConstants.REGULAR_QUEUE;

public class TestJedisUtils {

    private static final Config CONFIG = new ConfigBuilder().build();
    private static final String TEST_KEY = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(CONFIG);
    }

    @Test
    public void testEnsureJedisConnection_Success() {
        final Jedis jedis = createJedis(CONFIG);
        Assert.assertTrue(JedisUtils.ensureJedisConnection(jedis));
        Assert.assertTrue(JedisUtils.testJedisConnection(jedis));
    }

    @Test
    public void testEnsureJedisConnection_Fail() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.disconnect();
        Assert.assertFalse(JedisUtils.ensureJedisConnection(jedis));
        Assert.assertTrue(JedisUtils.testJedisConnection(jedis));
    }

    @Test
    public void testTestJedisConnection_Success() {
        Assert.assertTrue(JedisUtils.testJedisConnection(createJedis(CONFIG)));
    }

    @Test
    public void testTestJedisConnection_Fail() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.disconnect();
        Assert.assertFalse(JedisUtils.testJedisConnection(jedis));
    }

    @Test
    public void testReconnect_Success() {
        Assert.assertTrue(JedisUtils.reconnect(createJedis(CONFIG), 1, 1));
    }

    @Test
    public void testIsRegularQueue_Success() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.hset(QUEUE_TYPES, TEST_KEY, REGULAR_QUEUE);
        Assert.assertTrue(JedisUtils.isRegularQueue(jedis, TEST_KEY));
    }

    @Test
    public void testIsRegularQueue_Failure() {
        Assert.assertFalse(JedisUtils.isRegularQueue(createJedis(CONFIG), TEST_KEY));
    }

    @Test
    public void testIsDelayedQueue_Success() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.hset(QUEUE_TYPES, TEST_KEY, DELAYED_QUEUE);
        Assert.assertTrue(JedisUtils.isDelayedQueue(jedis, TEST_KEY));
    }

    @Test
    public void testIsDelayedQueue_Failure() {
        Assert.assertFalse(JedisUtils.isDelayedQueue(createJedis(CONFIG), TEST_KEY));
    }

    @Test
    public void testIsKeyUsed_Success() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.set(TEST_KEY, "bar");
        Assert.assertTrue(JedisUtils.isKeyUsed(jedis, TEST_KEY));
    }

    @Test
    public void testIsKeyUsed_Failure() {
        Assert.assertFalse(JedisUtils.isKeyUsed(createJedis(CONFIG), "foo"));
    }

    @Test
    public void testCanUseAsDelayedQueue_Success_ZSet() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.zadd(TEST_KEY, 1.0, "bar");
        Assert.assertTrue(JedisUtils.canUseAsDelayedQueue(jedis, TEST_KEY));
    }

    @Test
    public void testCanUseAsDelayedQueue_Success_None() {
        Jedis jedis = createJedis(CONFIG);
        jedis.hset(QUEUE_TYPES, TEST_KEY, REGULAR_QUEUE);
        Assert.assertTrue(JedisUtils.canUseAsDelayedQueue(jedis, TEST_KEY));
    }

    @Test
    public void testCanUseAsDelayedQueue_Failure() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.lpush(TEST_KEY, "bar");
        jedis.hset(QUEUE_TYPES, TEST_KEY, REGULAR_QUEUE);
        Assert.assertFalse(JedisUtils.canUseAsDelayedQueue(jedis, TEST_KEY));
    }

    @Test
    public void testCanUseAsRegularQueue_Failure() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.zadd(TEST_KEY, 2.0, "bar");
        jedis.hset(QUEUE_TYPES, TEST_KEY, DELAYED_QUEUE);
        Assert.assertFalse(JedisUtils.canUseAsRegularQueue(jedis, TEST_KEY));
    }

    @Test
    public void testCanUseAsRegularQueue_Success_None() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.hset(QUEUE_TYPES, TEST_KEY, DELAYED_QUEUE);
        Assert.assertTrue(JedisUtils.canUseAsRegularQueue(jedis, TEST_KEY));
    }
}
