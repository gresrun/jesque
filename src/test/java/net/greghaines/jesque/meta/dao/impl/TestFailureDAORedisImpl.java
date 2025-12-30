package net.greghaines.jesque.meta.dao.impl;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.Config.Builder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.json.ObjectMapperFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.UnifiedJedis;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestFailureDAORedisImpl {

  private static final String FAILED_KEY = DEFAULT_NAMESPACE + COLON + FAILED;
  private static final String FAILED_STAT_KEY = DEFAULT_NAMESPACE + COLON + STAT + COLON + FAILED;
  private static final String QUEUES_KEY = DEFAULT_NAMESPACE + COLON + QUEUES;

  @Mock private UnifiedJedis jedisPool;
  private FailureDAORedisImpl failureDAO;

  @Before
  public void setUp() {
    this.failureDAO = new FailureDAORedisImpl(Config.getDefaultConfig(), this.jedisPool);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullConfig() {
    new FailureDAORedisImpl(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullPool() {
    final Config config = Config.getDefaultConfig();
    new FailureDAORedisImpl(config, null);
  }

  @Test
  public void testGetCount() {
    final String failCount = "12";
    when(this.jedisPool.get(FAILED_STAT_KEY)).thenReturn(failCount);
    final long count = this.failureDAO.getCount();
    assertThat(count).isEqualTo(Long.parseLong(failCount));
  }

  @Test
  public void testGetFailQueueJobCount() {
    final long failQueueJobCount = 12;
    when(this.jedisPool.llen(FAILED_KEY)).thenReturn(failQueueJobCount);
    final long count = this.failureDAO.getFailQueueJobCount();
    assertThat(count).isEqualTo(failQueueJobCount);
  }

  @Test
  public void testClear() {
    this.failureDAO.clear();
    verify(this.jedisPool).del(FAILED_KEY);
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
    when(this.jedisPool.lrange(FAILED_KEY, offset, offset + count - 1)).thenReturn(origJsons);
    final List<JobFailure> fails = this.failureDAO.getFailures(offset, count);
    assertThat(fails).containsExactlyElementsIn(origFailures);
  }

  @Test
  public void testRemove() throws JsonProcessingException {
    final long index = 8;
    when(this.jedisPool.lset(eq(FAILED_KEY), eq(index), any(String.class))).thenReturn("OK");
    when(this.jedisPool.lrem(eq(FAILED_KEY), eq(1L), any(String.class))).thenReturn(1L);
    this.failureDAO.remove(index);
    verify(this.jedisPool).lset(eq(FAILED_KEY), eq(index), any(String.class));
    verify(this.jedisPool).lrem(eq(FAILED_KEY), eq(1L), any(String.class));
  }

  @Test
  public void testEnqueue() throws IOException {
    final String queue = "queue1";
    final Job job = new Job("foo");
    final String jobJson = ObjectMapperFactory.get().writeValueAsString(job);
    when(this.jedisPool.sadd(QUEUES_KEY, queue)).thenReturn(1L);
    when(this.jedisPool.rpush("resque:queue:" + queue, jobJson)).thenReturn(1L);
    this.failureDAO.enqueue(this.jedisPool, queue, job);
    verify(this.jedisPool).sadd(QUEUES_KEY, queue);
    verify(this.jedisPool).rpush("resque:queue:" + queue, jobJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnqueue_NullQueue() throws IOException {
    this.failureDAO.enqueue(this.jedisPool, null, new Job("foo"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnqueue_EmptyQueue() throws IOException {
    this.failureDAO.enqueue(this.jedisPool, "", new Job("foo"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnqueue_NullJob() throws IOException {
    this.failureDAO.enqueue(this.jedisPool, "foo", null);
  }

  @Test(expected = IllegalStateException.class)
  public void testEnqueue_InvalidJob() throws IOException {
    this.failureDAO.enqueue(this.jedisPool, "foo", new Job());
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
    when(this.jedisPool.lrange(FAILED_KEY, index, index + count - 1)).thenReturn(origJsons);
    when(this.jedisPool.lset(eq(FAILED_KEY), eq(index), any(String.class))).thenReturn("OK");
    when(this.jedisPool.sadd(QUEUES_KEY, queue)).thenReturn(1L);
    when(this.jedisPool.rpush("resque:queue:" + queue, jobJson)).thenReturn(1L);
    final Date requeuedAt = this.failureDAO.requeue(index);
    assertThat(requeuedAt).isAtMost(new Date());
  }
}
