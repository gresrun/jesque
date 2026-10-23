package net.greghaines.jesque.client;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.TestUtils.createBoundedJedisPool;
import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.TestAction;
import net.greghaines.jesque.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.UnifiedJedis;

public class TestClientPoolImpl {

  private static final Config CONFIG = Config.getDefaultConfig();
  private static final String TEST_QUEUE = "foo";
  private static final String TEST_QUEUE_KEY = createKey(CONFIG.getNamespace(), QUEUE, TEST_QUEUE);
  private static final Duration RECURRING_FREQUENCY = Duration.ofMinutes(1);
  private static final int POOL_SIZE = 3;

  private UnifiedJedis jedisPool;
  private Client client;

  @Before
  public void setUp() {
    TestUtils.resetRedis(CONFIG);
    this.jedisPool = createBoundedJedisPool(CONFIG, POOL_SIZE);
    this.client = new ClientPoolImpl(CONFIG, this.jedisPool);
  }

  @After
  public void tearDown() {
    this.client.end();
    this.jedisPool.close();
  }

  @Test
  public void testBatchEnqueue_ReturnsConnectionToPool() {
    final int batchCount = POOL_SIZE + 1;
    final Job job = new Job(TestAction.class.getSimpleName());
    for (int i = 0; i < batchCount; i++) {
      this.client.batchEnqueue(TEST_QUEUE, List.of(job));
    }
    try (Jedis jedis = createJedis(CONFIG)) {
      assertThat(jedis.llen(TEST_QUEUE_KEY)).isEqualTo(batchCount);
    }
  }

  @Test
  public void testRecurringEnqueue_ReturnsConnectionToPool() {
    final List<Job> jobs = createDistinctJobs(POOL_SIZE + 1);
    final Instant future = Instant.now().plus(RECURRING_FREQUENCY);
    for (Job job : jobs) {
      this.client.recurringEnqueue(TEST_QUEUE, job, future, RECURRING_FREQUENCY);
    }
    try (Jedis jedis = createJedis(CONFIG)) {
      assertThat(jedis.zcard(TEST_QUEUE_KEY)).isEqualTo(jobs.size());
    }
  }

  @Test
  public void testRemoveRecurringEnqueue_ReturnsConnectionToPool() {
    final List<Job> jobs = createDistinctJobs(POOL_SIZE + 1);
    final Instant future = Instant.now().plus(RECURRING_FREQUENCY);
    final Client seedingClient = new ClientImpl(CONFIG);
    try {
      for (Job job : jobs) {
        seedingClient.recurringEnqueue(TEST_QUEUE, job, future, RECURRING_FREQUENCY);
      }
    } finally {
      seedingClient.end();
    }
    for (Job job : jobs) {
      this.client.removeRecurringEnqueue(TEST_QUEUE, job);
    }
    try (Jedis jedis = createJedis(CONFIG)) {
      assertThat(jedis.zcard(TEST_QUEUE_KEY)).isEqualTo(0L);
    }
  }

  private static List<Job> createDistinctJobs(final int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> new Job(TestAction.class.getSimpleName(), i))
        .toList();
  }
}
