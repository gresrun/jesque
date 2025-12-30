package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.Config.Builder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.meta.KeyType;
import net.greghaines.jesque.meta.QueueInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.resps.Tuple;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestQueueInfoDAORedisImpl {

  private static final String QUEUES_KEY = DEFAULT_NAMESPACE + COLON + QUEUES;

  @Mock private UnifiedJedis jedisPool;
  private QueueInfoDAORedisImpl qInfoDAO;

  @Before
  public void setUp() {
    this.qInfoDAO = new QueueInfoDAORedisImpl(Config.getDefaultConfig(), this.jedisPool);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullConfig() {
    new QueueInfoDAORedisImpl(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullPool() {
    final Config config = Config.getDefaultConfig();
    new QueueInfoDAORedisImpl(config, null);
  }

  @Test
  public void testGetQueueNames() {
    final Set<String> queueSet = new HashSet<String>(Arrays.asList("queue1", "queue2"));
    final List<String> origQueueNames = new ArrayList<String>(queueSet);
    Collections.sort(origQueueNames);
    when(this.jedisPool.smembers(QUEUES_KEY)).thenReturn(queueSet);
    final List<String> queueNames = this.qInfoDAO.getQueueNames();
    Assert.assertNotNull(queueNames);
    Assert.assertEquals(queueSet.size(), queueNames.size());
    Assert.assertTrue(queueNames.containsAll(queueSet));
    Assert.assertEquals(origQueueNames, queueNames);
  }

  @Test
  public void testGetPendingCount() {
    final Map<String, Long> queueCountMap = new HashMap<String, Long>(2);
    queueCountMap.put("queue1", 3L);
    queueCountMap.put("queue2", 5L);
    final Map<String, String> queueTypeMap = new HashMap<String, String>(2);
    queueTypeMap.put("queue1", KeyType.LIST.toString());
    queueTypeMap.put("queue2", KeyType.ZSET.toString());
    when(this.jedisPool.smembers(QUEUES_KEY)).thenReturn(queueCountMap.keySet());
    for (final Entry<String, String> e : queueTypeMap.entrySet()) {
      final String queueKey = "resque:queue:" + e.getKey();
      when(this.jedisPool.type(queueKey)).thenReturn(e.getValue());
      if (KeyType.ZSET.toString().equals(e.getValue())) {
        when(this.jedisPool.zcard(queueKey)).thenReturn(queueCountMap.get(e.getKey()));
      } else {
        when(this.jedisPool.llen(queueKey)).thenReturn(queueCountMap.get(e.getKey()));
      }
    }
    final long pendingCount = this.qInfoDAO.getPendingCount();
    Assert.assertEquals(8, pendingCount);
  }

  @Test
  public void testGetProcessedCount() {
    final Long count = 5L;
    when(this.jedisPool.get("resque:stat:processed")).thenReturn(Long.toString(count));
    final long processedCount = this.qInfoDAO.getProcessedCount();
    Assert.assertEquals(count, (Long) processedCount);
  }

  @Test
  public void testGetProcessedCount_Null() {
    when(this.jedisPool.get("resque:stat:processed")).thenReturn(null);
    final long processedCount = this.qInfoDAO.getProcessedCount();
    Assert.assertEquals(0L, processedCount);
  }

  @Test
  public void testRemoveQueue() {
    final String queue = "queue1";
    this.qInfoDAO.removeQueue(queue);
    verify(this.jedisPool).srem(QUEUES_KEY, queue);
    verify(this.jedisPool).del("resque:queue:" + queue);
  }

  @Test
  public void testGetQueueInfos() {
    final Map<String, Long> queueCountMap = new HashMap<String, Long>(2);
    queueCountMap.put("queue1", 3L);
    queueCountMap.put("queue2", 5L);
    final Map<String, String> queueTypeMap = new HashMap<String, String>(2);
    queueTypeMap.put("queue1", KeyType.LIST.toString());
    queueTypeMap.put("queue2", KeyType.ZSET.toString());
    when(this.jedisPool.smembers(QUEUES_KEY)).thenReturn(queueCountMap.keySet());
    for (final Entry<String, String> e : queueTypeMap.entrySet()) {
      final String queueKey = "resque:queue:" + e.getKey();
      when(this.jedisPool.type(queueKey)).thenReturn(e.getValue());
      if (KeyType.ZSET.toString().equals(e.getValue())) {
        when(this.jedisPool.zcard(queueKey)).thenReturn(queueCountMap.get(e.getKey()));
        when(this.jedisPool.zcount(eq(queueKey), eq(0.0), any(Double.class))).thenReturn(1L);
      } else {
        when(this.jedisPool.llen(queueKey)).thenReturn(queueCountMap.get(e.getKey()));
      }
    }
    final List<QueueInfo> queueInfos = this.qInfoDAO.getQueueInfos();
    Assert.assertNotNull(queueInfos);
    Assert.assertEquals(queueCountMap.size(), queueInfos.size());
    for (final QueueInfo queueInfo : queueInfos) {
      Assert.assertTrue(queueCountMap.containsKey(queueInfo.getName()));
      Assert.assertEquals(queueCountMap.get(queueInfo.getName()), queueInfo.getSize());
    }
    for (final Entry<String, String> e : queueTypeMap.entrySet()) {
      final String queueKey = "resque:queue:" + e.getKey();
      verify(this.jedisPool, times(2)).type(queueKey);
    }
  }

  @Test
  public void testGetQueueInfo_List() throws JsonProcessingException {
    final String name = "queue1";
    final String queueKey = "resque:queue:" + name;
    final long jobOffset = 1;
    final long jobCount = 2;
    final long size = 4;
    final List<String> payloads = new ArrayList<String>();
    payloads.add(ObjectMapperFactory.get().writeValueAsString(new Job("foo")));
    payloads.add(ObjectMapperFactory.get().writeValueAsString(new Job("bar")));
    when(this.jedisPool.type(queueKey)).thenReturn(KeyType.LIST.toString());
    when(this.jedisPool.llen(queueKey)).thenReturn(size);
    when(this.jedisPool.lrange(queueKey, jobOffset, jobOffset + jobCount - 1)).thenReturn(payloads);
    final QueueInfo queueInfo = this.qInfoDAO.getQueueInfo(name, jobOffset, jobCount);
    Assert.assertNotNull(queueInfo);
    Assert.assertEquals(name, queueInfo.getName());
    Assert.assertEquals((Long) size, queueInfo.getSize());
    final List<Job> jobs = queueInfo.getJobs();
    Assert.assertNotNull(jobs);
    Assert.assertEquals(payloads.size(), jobs.size());
    verify(this.jedisPool, times(3)).type(queueKey);
  }

  @Test
  public void testGetQueueInfo_ZSet() throws JsonProcessingException {
    final String name = "queue1";
    final String queueKey = "resque:queue:" + name;
    final long jobOffset = 1;
    final long jobCount = 2;
    final long size = 4;
    final List<Tuple> payloads = new ArrayList<>(2);
    payloads.add(new Tuple(ObjectMapperFactory.get().writeValueAsString(new Job("foo")), 1d));
    payloads.add(new Tuple(ObjectMapperFactory.get().writeValueAsString(new Job("bar")), 1d));
    when(this.jedisPool.type(queueKey)).thenReturn(KeyType.ZSET.toString());
    when(this.jedisPool.zcard(queueKey)).thenReturn(size);
    when(this.jedisPool.zcount(eq(queueKey), eq(0.0), any(Double.class))).thenReturn(jobCount);
    when(this.jedisPool.zrangeWithScores(queueKey, jobOffset, jobOffset + jobCount - 1))
        .thenReturn(payloads);
    final QueueInfo queueInfo = this.qInfoDAO.getQueueInfo(name, jobOffset, jobCount);
    Assert.assertNotNull(queueInfo);
    Assert.assertEquals(name, queueInfo.getName());
    Assert.assertEquals((Long) size, queueInfo.getSize());
    final List<Job> jobs = queueInfo.getJobs();
    Assert.assertNotNull(jobs);
    Assert.assertEquals(payloads.size(), jobs.size());
    verify(this.jedisPool, times(3)).type(queueKey);
  }
}
