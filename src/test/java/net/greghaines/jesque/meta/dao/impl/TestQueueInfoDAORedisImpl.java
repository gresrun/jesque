package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.ConfigBuilder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.meta.KeyType;
import net.greghaines.jesque.meta.QueueInfo;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class TestQueueInfoDAORedisImpl {
    
    private static final String QUEUES_KEY = DEFAULT_NAMESPACE + COLON + QUEUES;
    
    private Mockery mockCtx;
    private Pool<Jedis> pool;
    private Jedis jedis;
    private QueueInfoDAORedisImpl qInfoDAO;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        this.mockCtx.setThreadingPolicy(new Synchroniser());
        this.pool = this.mockCtx.mock(Pool.class);
        this.jedis = this.mockCtx.mock(Jedis.class);
        this.qInfoDAO = new QueueInfoDAORedisImpl(new ConfigBuilder().build(), this.pool);
    }
    
    @After
    public void tearDown() {
        this.mockCtx.assertIsSatisfied();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new QueueInfoDAORedisImpl(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullPool() {
        final Config config = new ConfigBuilder().build();
        new QueueInfoDAORedisImpl(config, null);
    }
    
    @Test
    public void testGetQueueNames() {
        final Set<String> queueSet = new HashSet<String>(Arrays.asList("queue1", "queue2"));
        final List<String> origQueueNames = new ArrayList<String>(queueSet);
        Collections.sort(origQueueNames);
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).smembers(QUEUES_KEY); will(returnValue(queueSet));
            oneOf(jedis).close();
        }});
        final List<String> queueNames = this.qInfoDAO.getQueueNames();
        Assert.assertNotNull(queueNames);
        Assert.assertEquals(queueSet.size(), queueNames.size());
        Assert.assertTrue(queueNames.containsAll(queueSet));
        Assert.assertEquals(origQueueNames, queueNames);
    }
    
    @Test
    public void testGetPendingCount() {
        final Map<String,Long> queueCountMap = new HashMap<String,Long>(2);
        queueCountMap.put("queue1", 3L);
        queueCountMap.put("queue2", 5L);
        final Map<String,String> queueTypeMap = new HashMap<String,String>(2);
        queueTypeMap.put("queue1", KeyType.LIST.toString());
        queueTypeMap.put("queue2", KeyType.ZSET.toString());
        this.mockCtx.checking(new Expectations(){{
            exactly(2).of(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).smembers(QUEUES_KEY); will(returnValue(queueCountMap.keySet()));
            for (final Entry<String,String> e : queueTypeMap.entrySet()) {
                final String queueKey = "resque:queue:" + e.getKey();
                oneOf(jedis).type(queueKey); will(returnValue(e.getValue()));
                if (KeyType.ZSET.toString().equals(e.getValue())) {
                    oneOf(jedis).zcard(queueKey); will(returnValue(queueCountMap.get(e.getKey())));
                } else {
                    oneOf(jedis).llen(queueKey); will(returnValue(queueCountMap.get(e.getKey())));
                }
            }
            exactly(2).of(jedis).close();
        }});
        final long pendingCount = this.qInfoDAO.getPendingCount();
        Assert.assertEquals(8, pendingCount);
    }
    
    @Test
    public void testGetProcessedCount() {
        final Long count = 5L;
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).get("resque:stat:processed"); will(returnValue(Long.toString(count)));
            oneOf(jedis).close();
        }});
        final long processedCount = this.qInfoDAO.getProcessedCount();
        Assert.assertEquals(count, (Long)processedCount);
    }
    
    @Test
    public void testGetProcessedCount_Null() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).get("resque:stat:processed"); will(returnValue(null));
            oneOf(jedis).close();
        }});
        final long processedCount = this.qInfoDAO.getProcessedCount();
        Assert.assertEquals(0L, processedCount);
    }
    
    @Test
    public void testRemoveQueue() {
        final String queue = "queue1";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).srem(QUEUES_KEY, queue);
            oneOf(jedis).del("resque:queue:" + queue);
            oneOf(jedis).close();
        }});
        this.qInfoDAO.removeQueue(queue);
    }
    
    @Test
    public void testGetQueueInfos() {
        final Map<String,Long> queueCountMap = new HashMap<String,Long>(2);
        queueCountMap.put("queue1", 3L);
        queueCountMap.put("queue2", 5L);
        final Map<String,String> queueTypeMap = new HashMap<String,String>(2);
        queueTypeMap.put("queue1", KeyType.LIST.toString());
        queueTypeMap.put("queue2", KeyType.ZSET.toString());
        this.mockCtx.checking(new Expectations(){{
            exactly(2).of(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).smembers(QUEUES_KEY); will(returnValue(queueCountMap.keySet()));
            for (final Entry<String,String> e : queueTypeMap.entrySet()) {
                final String queueKey = "resque:queue:" + e.getKey();
                exactly(2).of(jedis).type(queueKey); will(returnValue(e.getValue()));
                if (KeyType.ZSET.toString().equals(e.getValue())) {
                    oneOf(jedis).zcard(queueKey); will(returnValue(queueCountMap.get(e.getKey())));
                } else {
                    oneOf(jedis).llen(queueKey); will(returnValue(queueCountMap.get(e.getKey())));
                }
            }
            exactly(2).of(jedis).close();
        }});
        final List<QueueInfo> queueInfos = this.qInfoDAO.getQueueInfos();
        Assert.assertNotNull(queueInfos);
        Assert.assertEquals(queueCountMap.size(), queueInfos.size());
        for (final QueueInfo queueInfo : queueInfos) {
            Assert.assertTrue(queueCountMap.containsKey(queueInfo.getName()));
            Assert.assertEquals(queueCountMap.get(queueInfo.getName()), queueInfo.getSize());
        }
    }
    
    @Test
    public void testGetQueueInfo_List() throws JsonProcessingException {
        final String name = "queue1";
        final String queueKey = "resque:queue:" + name;
        final long jobOffset = 1;
        final long jobCount = 2;
        final long size = 4;
        final Collection<String> payloads = new ArrayList<String>();
        payloads.add(ObjectMapperFactory.get().writeValueAsString(new Job("foo")));
        payloads.add(ObjectMapperFactory.get().writeValueAsString(new Job("bar")));
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            exactly(3).of(jedis).type(queueKey); will(returnValue(KeyType.LIST.toString()));
            oneOf(jedis).llen(queueKey); will(returnValue(size));
            oneOf(jedis).lrange(queueKey, jobOffset, jobOffset + jobCount - 1); will(returnValue(payloads));
            oneOf(jedis).close();
        }});
        final QueueInfo queueInfo = this.qInfoDAO.getQueueInfo(name, jobOffset, jobCount);
        Assert.assertNotNull(queueInfo);
        Assert.assertEquals(name, queueInfo.getName());
        Assert.assertEquals((Long) size, queueInfo.getSize());
        final List<Job> jobs = queueInfo.getJobs();
        Assert.assertNotNull(jobs);
        Assert.assertEquals(payloads.size(), jobs.size());
    }
    
    @Test
    public void testGetQueueInfo_ZSet() throws JsonProcessingException {
        final String name = "queue1";
        final String queueKey = "resque:queue:" + name;
        final long jobOffset = 1;
        final long jobCount = 2;
        final long size = 4;
        final Collection<String> payloads = new HashSet<String>();
        payloads.add(ObjectMapperFactory.get().writeValueAsString(new Job("foo")));
        payloads.add(ObjectMapperFactory.get().writeValueAsString(new Job("bar")));
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            exactly(3).of(jedis).type(queueKey); will(returnValue(KeyType.ZSET.toString()));
            oneOf(jedis).zcard(queueKey); will(returnValue(size));
            oneOf(jedis).zrange(queueKey, jobOffset, jobOffset + jobCount - 1); will(returnValue(payloads));
            oneOf(jedis).close();
        }});
        final QueueInfo queueInfo = this.qInfoDAO.getQueueInfo(name, jobOffset, jobCount);
        Assert.assertNotNull(queueInfo);
        Assert.assertEquals(name, queueInfo.getName());
        Assert.assertEquals((Long) size, queueInfo.getSize());
        final List<Job> jobs = queueInfo.getJobs();
        Assert.assertNotNull(jobs);
        Assert.assertEquals(payloads.size(), jobs.size());
    }
}
