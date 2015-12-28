package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.ConfigBuilder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.STARTED;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.meta.WorkerInfo;
import net.greghaines.jesque.utils.CompositeDateFormat;

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
            oneOf(jedis).close();
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
            oneOf(jedis).close();
        }});
        this.workerInfoDAO.removeWorker(workerName);
    }
    
    @Test
    public void testIsWorkerInState_NullState() throws IOException {
        Assert.assertTrue(this.workerInfoDAO.isWorkerInState("foo", null, this.jedis));
    }
    
    @Test
    public void testIsWorkerInState_IdleState() throws IOException {
        final String workerName = "foo";
        final String statusPayload = null;
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
        }});
        Assert.assertTrue(this.workerInfoDAO.isWorkerInState(workerName, 
                WorkerInfo.State.IDLE, this.jedis));
    }
    
    @Test
    public void testIsWorkerInState_PausedState_NoStatus() throws IOException {
        final String workerName = "foo";
        final String statusPayload = null;
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
        }});
        Assert.assertTrue(this.workerInfoDAO.isWorkerInState(workerName, 
                WorkerInfo.State.PAUSED, this.jedis));
    }
    
    @Test
    public void testIsWorkerInState_PausedState_Status() throws IOException {
        final String workerName = "foo";
        final String statusPayload = "{\"paused\":false}";
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
        }});
        Assert.assertFalse(this.workerInfoDAO.isWorkerInState(workerName, 
                WorkerInfo.State.PAUSED, this.jedis));
    }
    
    @Test
    public void testIsWorkerInState_WorkingState_NoStatus() throws IOException {
        final String workerName = "foo";
        final String statusPayload = null;
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
        }});
        Assert.assertFalse(this.workerInfoDAO.isWorkerInState(workerName, 
                WorkerInfo.State.WORKING, this.jedis));
    }
    
    @Test
    public void testIsWorkerInState_WorkingState_Status() throws IOException {
        final String workerName = "foo";
        final String statusPayload = "{\"paused\":false}";
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
        }});
        Assert.assertTrue(this.workerInfoDAO.isWorkerInState(workerName, 
                WorkerInfo.State.WORKING, this.jedis));
    }
    
    @Test(expected = ParseException.class)
    public void testCreateWorker_MalformedName() throws ParseException, IOException {
        this.workerInfoDAO.createWorker("foo", this.jedis);
    }
    
    @Test
    public void testCreateWorker_Idle() throws ParseException, IOException {
        final String workerName = "foo:123:bar,baz,qux";
        final String statusPayload = null;
        final String startedStr = "2014-02-09T23:22:54.412-0400";
        final String failedStr = "2";
        final String processedStr = "6";
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
            oneOf(jedis).get("resque:worker:" + workerName + COLON + STARTED); will(returnValue(startedStr));
            oneOf(jedis).get("resque:stat:failed:" + workerName); will(returnValue(failedStr));
            oneOf(jedis).get("resque:stat:processed:" + workerName); will(returnValue(processedStr));
        }});
        final WorkerInfo workerInfo = this.workerInfoDAO.createWorker(workerName, this.jedis);
        Assert.assertEquals(workerName, workerInfo.getName());
        Assert.assertEquals("123", workerInfo.getPid());
        Assert.assertNotNull(workerInfo.getQueues());
        Assert.assertEquals(3, workerInfo.getQueues().size());
        Assert.assertTrue(workerInfo.getQueues().containsAll(Arrays.asList("bar", "baz", "qux")));
        Assert.assertEquals(WorkerInfo.State.IDLE, workerInfo.getState());
        Assert.assertNull(workerInfo.getStatus());
        Assert.assertEquals(new CompositeDateFormat().parse(startedStr), workerInfo.getStarted());
        Assert.assertEquals((Long)2L, workerInfo.getFailed());
        Assert.assertEquals((Long)6L, workerInfo.getProcessed());
    }
    
    @Test
    public void testCreateWorker_Paused() throws ParseException, IOException {
        final String workerName = "foo:123:bar,baz,qux";
        final String statusPayload = "{\"paused\":true}";
        final String startedStr = null;
        final String failedStr = null;
        final String processedStr = null;
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
            oneOf(jedis).get("resque:worker:" + workerName + COLON + STARTED); will(returnValue(startedStr));
            oneOf(jedis).get("resque:stat:failed:" + workerName); will(returnValue(failedStr));
            oneOf(jedis).get("resque:stat:processed:" + workerName); will(returnValue(processedStr));
        }});
        final WorkerInfo workerInfo = this.workerInfoDAO.createWorker(workerName, this.jedis);
        Assert.assertEquals(workerName, workerInfo.getName());
        Assert.assertEquals("123", workerInfo.getPid());
        Assert.assertNotNull(workerInfo.getQueues());
        Assert.assertEquals(3, workerInfo.getQueues().size());
        Assert.assertTrue(workerInfo.getQueues().containsAll(Arrays.asList("bar", "baz", "qux")));
        Assert.assertEquals(WorkerInfo.State.PAUSED, workerInfo.getState());
        Assert.assertNotNull(workerInfo.getStatus());
        Assert.assertTrue(workerInfo.getStatus().isPaused());
        Assert.assertNull(workerInfo.getStarted());
        Assert.assertEquals((Long)0L, workerInfo.getFailed());
        Assert.assertEquals((Long)0L, workerInfo.getProcessed());
    }
    
    @Test
    public void testCreateWorker_Working() throws ParseException, IOException {
        final String workerName = "foo:123:bar,baz,qux";
        final String statusPayload = "{\"paused\":false}";
        final String startedStr = null;
        final String failedStr = null;
        final String processedStr = null;
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).get("resque:worker:" + workerName); will(returnValue(statusPayload));
            oneOf(jedis).get("resque:worker:" + workerName + COLON + STARTED); will(returnValue(startedStr));
            oneOf(jedis).get("resque:stat:failed:" + workerName); will(returnValue(failedStr));
            oneOf(jedis).get("resque:stat:processed:" + workerName); will(returnValue(processedStr));
        }});
        final WorkerInfo workerInfo = this.workerInfoDAO.createWorker(workerName, this.jedis);
        Assert.assertEquals(workerName, workerInfo.getName());
        Assert.assertEquals("123", workerInfo.getPid());
        Assert.assertNotNull(workerInfo.getQueues());
        Assert.assertEquals(3, workerInfo.getQueues().size());
        Assert.assertTrue(workerInfo.getQueues().containsAll(Arrays.asList("bar", "baz", "qux")));
        Assert.assertEquals(WorkerInfo.State.WORKING, workerInfo.getState());
        Assert.assertNotNull(workerInfo.getStatus());
        Assert.assertFalse(workerInfo.getStatus().isPaused());
        Assert.assertNull(workerInfo.getStarted());
        Assert.assertEquals((Long)0L, workerInfo.getFailed());
        Assert.assertEquals((Long)0L, workerInfo.getProcessed());
    }
}
