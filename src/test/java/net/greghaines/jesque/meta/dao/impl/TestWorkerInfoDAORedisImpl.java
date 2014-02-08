package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.ConfigBuilder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class TestWorkerInfoDAORedisImpl {
    
    private static final String WORKERS_KEY = DEFAULT_NAMESPACE + COLON + WORKERS;

    private Mockery mockCtx;
    private Pool<Jedis> pool;
    private Jedis jedis;
    private WorkerInfoDAORedisImpl workerInfoDAO;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        this.mockCtx.setThreadingPolicy(new Synchroniser());
        this.pool = this.mockCtx.mock(Pool.class);
        this.jedis = this.mockCtx.mock(Jedis.class);
        this.workerInfoDAO = new WorkerInfoDAORedisImpl(new ConfigBuilder().build(), this.pool);
    }
    
    @After
    public void tearDown() {
        this.mockCtx.assertIsSatisfied();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new WorkerInfoDAORedisImpl(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullPool() {
        final Config config = new ConfigBuilder().build();
        new WorkerInfoDAORedisImpl(config, null);
    }
    
    @Test
    public void testGetWorkerCount() {
        final long workerCount = 12;
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).scard(WORKERS_KEY); will(returnValue(workerCount));
            oneOf(pool).returnResource(jedis);
        }});
        final long count = this.workerInfoDAO.getWorkerCount();
        Assert.assertEquals(workerCount, count);
    }
    
    @Test
    public void testRemoveWorker() {
        final String workerName = "foo";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).srem(WORKERS_KEY, workerName); will(returnValue(1L));
            oneOf(jedis).del("resque:worker:foo", "resque:worker:foo:started", 
                    "resque:stat:failed:foo", "resque:stat:processed:foo");
            oneOf(pool).returnResource(jedis);
        }});
        this.workerInfoDAO.removeWorker(workerName);
    }
}
