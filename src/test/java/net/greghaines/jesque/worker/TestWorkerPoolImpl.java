package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.TestUtils.createBoundedJedisPool;
import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.TestUtils.createTestActionJobFactory;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;

import java.util.List;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.TestAction;
import net.greghaines.jesque.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.UnifiedJedis;

public class TestWorkerPoolImpl {

  private static final Config CONFIG = Config.getDefaultConfig();
  private static final String TEST_QUEUE = "foo";
  private static final String FAIL_QUEUE_KEY = createKey(CONFIG.getNamespace(), FAILED);
  private static final int POOL_SIZE = 3;

  private UnifiedJedis jedisPool;
  private WorkerPoolImpl worker;

  @Before
  public void setUp() {
    TestUtils.resetRedis(CONFIG);
    this.jedisPool = createBoundedJedisPool(CONFIG, POOL_SIZE);
    this.worker =
        new WorkerPoolImpl(
            CONFIG, List.of(TEST_QUEUE), createTestActionJobFactory(), this.jedisPool);
  }

  @After
  public void tearDown() {
    this.jedisPool.close();
  }

  @Test
  public void testFailure_FailQueueMaxItems_ReturnsConnectionToPool() {
    final int failureCount = POOL_SIZE + 1;
    this.worker.setFailQueueStrategy(
        new DefaultFailQueueStrategy(CONFIG.getNamespace()) {
          @Override
          public int getFailQueueMaxItems(final String curQueue) {
            return failureCount;
          }
        });
    final Job job = new Job(TestAction.class.getSimpleName());
    for (int i = 0; i < failureCount; i++) {
      this.worker.failure(new RuntimeException("BOOM!"), job, TEST_QUEUE);
    }
    try (Jedis jedis = createJedis(CONFIG)) {
      assertThat(jedis.llen(FAIL_QUEUE_KEY)).isEqualTo(failureCount);
    }
  }
}
