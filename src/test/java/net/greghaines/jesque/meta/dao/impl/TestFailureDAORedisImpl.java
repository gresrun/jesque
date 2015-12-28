package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.ConfigBuilder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.json.ObjectMapperFactory;

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

import com.fasterxml.jackson.core.JsonProcessingException;

public class TestFailureDAORedisImpl {
    
    private static final String FAILED_KEY = DEFAULT_NAMESPACE + COLON + FAILED;
    private static final String QUEUES_KEY = DEFAULT_NAMESPACE + COLON + QUEUES;
    
    private Mockery mockCtx;
    private Pool<Jedis> pool;
    private Jedis jedis;
    private FailureDAORedisImpl failureDAO;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        this.mockCtx.setThreadingPolicy(new Synchroniser());
        this.pool = this.mockCtx.mock(Pool.class);
        this.jedis = this.mockCtx.mock(Jedis.class);
        this.failureDAO = new FailureDAORedisImpl(new ConfigBuilder().build(), this.pool);
    }
    
    @After
    public void tearDown() {
        this.mockCtx.assertIsSatisfied();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new FailureDAORedisImpl(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullPool() {
        final Config config = new ConfigBuilder().build();
        new FailureDAORedisImpl(config, null);
    }
    
    @Test
    public void testGetCount() {
        final long failCount = 12;
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).llen(FAILED_KEY); will(returnValue(failCount));
            oneOf(jedis).close();
        }});
        final long count = this.failureDAO.getCount();
        Assert.assertEquals(failCount, count);
    }
    
    @Test
    public void testClear() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).del(FAILED_KEY);
            oneOf(jedis).close();
        }});
        this.failureDAO.clear();
    }
    
    @Test
    public void testGetFailures() throws JsonProcessingException {
        final long offset = 4;
        final long count = 2;
        final List<JobFailure> origFailures = new ArrayList<JobFailure>(2);
        final JobFailure fail1 = new JobFailure();
        fail1.setError("foo");
        final JobFailure fail2 = new JobFailure();
        fail2.setError("bar");
        origFailures.add(fail1);
        origFailures.add(fail2);
        final List<String> origJsons = new ArrayList<String>(origFailures.size());
        for (final JobFailure fail : origFailures) {
            origJsons.add(ObjectMapperFactory.get().writeValueAsString(fail));
        }
        origJsons.add(UUID.randomUUID().toString());
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).lrange(FAILED_KEY, offset, offset + count - 1);
            will(returnValue(origJsons));
            oneOf(jedis).close();
        }});
        final List<JobFailure> fails = this.failureDAO.getFailures(offset, count);
        Assert.assertNotNull(fails);
        Assert.assertEquals(origFailures.size(), fails.size());
        Assert.assertTrue(fails.containsAll(origFailures));
    }
    
    @Test
    public void testRemove() throws JsonProcessingException {
        final long index = 8;
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).lset(with(equal(FAILED_KEY)), with(equal(index)), with(any(String.class)));
            oneOf(jedis).lrem(with(equal(FAILED_KEY)), with(equal(1L)), with(any(String.class)));
            oneOf(jedis).close();
        }});
        this.failureDAO.remove(index);
    }
    
    @Test
    public void testEnqueue() throws IOException {
        final String queue = "queue1";
        final Job job = new Job("foo");
        final String jobJson = ObjectMapperFactory.get().writeValueAsString(job);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).sadd(QUEUES_KEY, queue);
            oneOf(jedis).rpush("resque:queue:" + queue, jobJson);
        }});
        this.failureDAO.enqueue(this.jedis, queue, job);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEnqueue_NullQueue() throws IOException {
        this.failureDAO.enqueue(this.jedis, null, new Job("foo"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEnqueue_EmptyQueue() throws IOException {
        this.failureDAO.enqueue(this.jedis, "", new Job("foo"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEnqueue_NullJob() throws IOException {
        this.failureDAO.enqueue(this.jedis, "foo", null);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testEnqueue_InvalidJob() throws IOException {
        this.failureDAO.enqueue(this.jedis, "foo", new Job());
    }
    
    @Test
    public void testRequeue() throws JsonProcessingException {
        final long index = 4;
        final long count = 1;
        final String queue = "queue1";
        final List<JobFailure> origFailures = new ArrayList<JobFailure>(1);
        final Job job = new Job("foo");
        final JobFailure fail1 = new JobFailure();
        fail1.setError("foo");
        fail1.setPayload(job);
        fail1.setQueue(queue);
        origFailures.add(fail1);
        final List<String> origJsons = new ArrayList<String>(origFailures.size());
        for (final JobFailure fail : origFailures) {
            origJsons.add(ObjectMapperFactory.get().writeValueAsString(fail));
        }
        final String jobJson = ObjectMapperFactory.get().writeValueAsString(job);
        this.mockCtx.checking(new Expectations(){{
            exactly(2).of(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).lrange(FAILED_KEY, index, index + count - 1);
            will(returnValue(origJsons));
            oneOf(jedis).lset(with(equal(FAILED_KEY)), with(equal(index)), with(any(String.class)));
            oneOf(jedis).sadd(QUEUES_KEY, queue);
            oneOf(jedis).rpush("resque:queue:" + queue, jobJson);
            exactly(2).of(jedis).close();
        }});
        final Date requeuedAt = this.failureDAO.requeue(index);
        Assert.assertNotNull(requeuedAt);
        Assert.assertTrue(System.currentTimeMillis() >= requeuedAt.getTime());
    }
}
