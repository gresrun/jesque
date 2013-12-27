package net.greghaines.jesque.utils;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.util.Pool;

public class TestPoolUtils {
    
    private Mockery mockCtx;
    private Pool<String> pool;
    private PoolWork<String,String> work;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        this.pool = this.mockCtx.mock(Pool.class);
        this.work = this.mockCtx.mock(PoolWork.class);
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
        final String resource = "foo";
        final String result = "bar";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(returnValue(result));
            oneOf(pool).returnResource(resource);
        }});
        Assert.assertEquals(result, PoolUtils.doWorkInPool(this.pool, this.work));
    }

    @Test
    public void testDoWorkInPoolNicely() throws Exception {
        final String resource = "foo";
        final String result = "bar";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(returnValue(result));
            oneOf(pool).returnResource(resource);
        }});
        Assert.assertEquals(result, PoolUtils.doWorkInPoolNicely(this.pool, this.work));
    }

    @Test(expected = RuntimeException.class)
    public void testDoWorkInPoolNicely_ThrowRuntimeEx() throws Exception {
        final String resource = "foo";
        final RuntimeException rte = new RuntimeException("foo");
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(throwException(rte));
            oneOf(pool).returnResource(resource);
        }});
        PoolUtils.doWorkInPoolNicely(this.pool, this.work);
    }

    @Test(expected = RuntimeException.class)
    public void testDoWorkInPoolNicely_ThrowEx() throws Exception {
        final String resource = "foo";
        final Exception ex = new Exception("foo");
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(resource));
            oneOf(work).doWork(resource); will(throwException(ex));
            oneOf(pool).returnResource(resource);
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
        Assert.assertNotNull(PoolUtils.createJedisPool(config));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateJedisPool_NullPoolConfig() {
        final Config config = new ConfigBuilder().build();
        PoolUtils.createJedisPool(config, null);
    }
}
