package net.greghaines.jesque.utils;

import static net.greghaines.jesque.TestUtils.createJedis;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.TestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

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
        jedis.lpush(TEST_KEY, "bar");
        Assert.assertTrue(JedisUtils.isRegularQueue(jedis, TEST_KEY));
    }

    @Test
    public void testIsRegularQueue_Failure() {
        Assert.assertFalse(JedisUtils.isRegularQueue(createJedis(CONFIG), TEST_KEY));
    }

    @Test
    public void testIsDelayedQueue_Success() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.zadd(TEST_KEY, 1.0, "bar");
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
        Assert.assertTrue(JedisUtils.canUseAsDelayedQueue(createJedis(CONFIG), TEST_KEY));
    }

    @Test
    public void testCanUseAsDelayedQueue_Failure() {
        final Jedis jedis = createJedis(CONFIG);
        jedis.set(TEST_KEY, "bar");
        Assert.assertFalse(JedisUtils.canUseAsDelayedQueue(jedis, TEST_KEY));
    }
}
