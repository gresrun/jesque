package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.TestUtils.createTestActionJobFactory;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.ResqueConstants.*;

import java.util.Arrays;
import java.util.List;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/** Created by Karthik (@argvk) on 6/3/15. */
public class RecurringQueueTest {

  private static final Config config = Config.getDefaultConfig();
  private static final String recurringTestQueue = "fooRecurring";
  private static final long recurringFrequency = 1000;
  private static final Client client = new ClientImpl(config);
  private static final String queueKey =
      createKey(config.getNamespace(), QUEUE, recurringTestQueue);
  private static final String hashKey = JesqueUtils.createRecurringHashKey(queueKey);

  @Before
  public void resetRedis() {
    TestUtils.resetRedis(config);
  }

  @Test
  public void testCode() throws Exception {

    Job job =
        new Job("TestAction", new Object[] {1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
    client.recurringEnqueue(
        recurringTestQueue,
        job,
        System.currentTimeMillis() + recurringFrequency,
        recurringFrequency);

    try (Jedis jedis = createJedis(config)) { // Assert that we enqueued the job
      assertThat(jedis.zcount(queueKey, "-inf", "+inf")).isEqualTo(1L);
      List<String> jobSet = jedis.zrangeByScore(queueKey, "-inf", "+inf", 0, 1);

      String jobString = jobSet.iterator().next();
      assertThat(jedis.hget(hashKey, jobString)).isEqualTo(String.valueOf(recurringFrequency));
    }

    // Create and mark the start worker
    final Worker worker =
        new WorkerImpl(config, Arrays.asList(recurringTestQueue), createTestActionJobFactory());
    final Thread workerThread = new Thread(worker);
    Long startMillis = System.currentTimeMillis();
    workerThread.start();

    try { // let the worker run for some time,
      Thread.sleep(1000 * 6);
    } finally { // Stop the worker
      TestUtils.stopWorker(worker, workerThread);
    }

    // stop the recurring queue and mark it as ended
    client.removeRecurringEnqueue(recurringTestQueue, job);
    Long endMillis = System.currentTimeMillis();

    // should have run (startMillis-endMillis/frequency)
    Long times = ((endMillis - startMillis) / recurringFrequency);

    // Assert that the job was run by the worker
    try (Jedis jedis = createJedis(config)) {
      Long processedTimes =
          Long.valueOf(jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));

      // allowing for off by one error
      Long oneOrZero = Math.abs(times - processedTimes);

      assertThat(oneOrZero == 0 || oneOrZero == 1).isTrue();
      assertThat(jedis.get(createKey(config.getNamespace(), STAT, FAILED))).isNull();
      assertThat(
              jedis.zcount(
                  createKey(config.getNamespace(), QUEUE, recurringTestQueue), "-inf", "+inf"))
          .isEqualTo(0L);
      assertThat(jedis.hlen(hashKey)).isEqualTo(0L);
    }
  }

  @After
  public void closeClient() {
    client.end();
  }
}
