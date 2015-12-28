package net.greghaines.jesque.utils;

import java.util.Collections;
import java.util.HashSet;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

public class TestPoolUtils {
    
    private Mockery mockCtx;
    private Jedis resource;
    private Pool<Jedis> pool;
    private PoolWork<Jedis, String> work;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        this.pool = this.mockCtx.mock(Pool.class);
        this.work = this.mockCtx.mock(PoolWork.class);
        this.resource = this.mockCtx.mock(Jedis.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDoWorkInPool_NullPool() throws Exception {
        PoolUtils.doWorkInPool(null, this.work);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDoWorkInPool_NullWork() throws Exception {
        PoolUtils.doWorkInPool(this.pool, null);
    }

    @Test
    public void testDoWorkInPool() throws Exception {
        final String result = "bar";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(returnValue(result));
            oneOf(resource).close();
        }});
        Assert.assertEquals(result, PoolUtils.doWorkInPool(this.pool, this.work));
    }

    @Test
    public void testDoWorkInPoolNicely() throws Exception {
        final String result = "bar";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(returnValue(result));
            oneOf(resource).close();
        }});
        Assert.assertEquals(result, PoolUtils.doWorkInPoolNicely(this.pool, this.work));
    }

    @Test(expected = RuntimeException.class)
    public void testDoWorkInPoolNicely_ThrowRuntimeEx() throws Exception {
        final RuntimeException rte = new RuntimeException("foo");
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(throwException(rte));
            oneOf(resource).close();
        }});
        PoolUtils.doWorkInPoolNicely(this.pool, this.work);
    }

    @Test(expected = RuntimeException.class)
    public void testDoWorkInPoolNicely_ThrowEx() throws Exception {
        final Exception ex = new Exception("foo");
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(throwException(ex));
            oneOf(resource).close();
        }});
        PoolUtils.doWorkInPoolNicely(this.pool, this.work);
    }
    
    @Test
    public void testGetDefaultPoolConfig() {
        Assert.assertNotNull(PoolUtils.getDefaultPoolConfig());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateJedisPool_NullConfig() {
        PoolUtils.createJedisPool(null);
    }
    
    @Test
    public void testCreateJedisPool() {
        final Config config = new ConfigBuilder().build();
        final Pool<Jedis> pool = PoolUtils.createJedisPool(config);
        Assert.assertNotNull(pool);
        Assert.assertTrue(pool instanceof JedisPool);
    }

    /**
     * This will need a sentinel running with the following config
     *
     * sentinel monitor mymaster 127.0.0.1 6379 1
     * sentinel down-after-milliseconds mymaster 6000
     * sentinel failover-timeout mymaster 180000
     * sentinel parallel-syncs mymaster 1
     *
     * You should also have a redis-server running to act as the master [mymaster]
     */
    @Test
    @Ignore("Will only work with sentinel running and travis-ci sentinel support is sketchy at best")
    public void testCreateJedisSentinelPool() {
        final Config config = new ConfigBuilder().withMasterName("mymaster")
                .withSentinels(new HashSet<>(Collections.singletonList("localhost:26379"))).build();
        final Pool<Jedis> pool = PoolUtils.createJedisPool(config);
        Assert.assertNotNull(pool);
        Assert.assertTrue(pool instanceof JedisSentinelPool);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateJedisPool_NullPoolConfig() {
        final Config config = new ConfigBuilder().build();
        PoolUtils.createJedisPool(config, null);
    }
}
