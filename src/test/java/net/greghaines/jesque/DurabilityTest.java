package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;

import java.util.Arrays;
import java.util.Map;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.ResqueConstants;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Test job durability:
 *
 * <ul>
 *   <li>A job should be in the in-flight list while being processed, but not afterwards.
 *   <li>A job should be re-queued when the worker is shut down immediately.
 *   <li>A job should <i>not</i> be re-queued when the worker shuts down after completing the job.
 * </ul>
 *
 * @author Daniël de Kok
 * @author Florian Langenhahn
 */
public class DurabilityTest {

  private static final Job sleepJob = new Job(SleepAction.class.getSimpleName(), 3000L);
  private static final Job sleepWithExceptionJob =
      new Job(SleepWithExceptionAction.class.getSimpleName(), 3000L);
  private static final Config config = Config.getDefaultConfig();

  @Before
  public void resetRedis() {
    TestUtils.resetRedis(config);
  }

  @Test
  public void testNotInterrupted() throws InterruptedException {
    final String queue = "foo";
    TestUtils.enqueueJobs(queue, Arrays.asList(sleepJob), config);

    final Worker worker =
        new WorkerImpl(
            config,
            Arrays.asList(queue),
            new MapBasedJobFactory(Map.of(SleepAction.class.getSimpleName(), SleepAction.class)));
    final Thread workerThread = new Thread(worker);
    workerThread.start();

    Thread.sleep(1000);

    final Jedis jedis = TestUtils.createJedis(config);
    assertThat(jedis.llen(inFlightKey(worker, queue))).isEqualTo(1L);
    assertThat(jedis.lindex(inFlightKey(worker, queue), 0))
        .isEqualTo(ObjectMapperFactory.get().writeValueAsString(sleepJob));

    TestUtils.stopWorker(worker, workerThread, false);

    assertThat(jedis.llen(createKey(config.getNamespace(), QUEUE, queue))).isEqualTo(0L);
    assertThat(jedis.llen(inFlightKey(worker, queue))).isEqualTo(0L);
  }

  @Test
  public void testInterruptedNoExceptionJobSucceeds() {
    final String queue = "bar";
    TestUtils.enqueueJobs(queue, Arrays.asList(sleepJob), config);

    final Worker worker =
        new WorkerImpl(
            config,
            Arrays.asList(queue),
            new MapBasedJobFactory(Map.of(SleepAction.class.getSimpleName(), SleepAction.class)));
    final Thread workerThread = new Thread(worker);
    workerThread.start();

    TestUtils.stopWorker(worker, workerThread, true);

    final Jedis jedis = TestUtils.createJedis(config);
    assertThat(jedis.llen(createKey(config.getNamespace(), QUEUE, queue))).isEqualTo(0L);
    assertThat(jedis.llen(inFlightKey(worker, queue))).isEqualTo(0L);
  }

  @Test
  public void testInterrupted() {
    final String queue = "bar";
    TestUtils.enqueueJobs(queue, Arrays.asList(sleepWithExceptionJob), config);

    final Worker worker =
        new WorkerImpl(
            config,
            Arrays.asList(queue),
            new MapBasedJobFactory(
                Map.of(
                    SleepWithExceptionAction.class.getSimpleName(),
                    SleepWithExceptionAction.class)));
    final Thread workerThread = new Thread(worker);
    workerThread.start();

    TestUtils.stopWorker(worker, workerThread, true);

    final Jedis jedis = TestUtils.createJedis(config);
    assertThat(jedis.llen(createKey(config.getNamespace(), QUEUE, queue))).isEqualTo(1L);
    assertThat(jedis.lindex(createKey(config.getNamespace(), QUEUE, queue), 0))
        .isEqualTo(ObjectMapperFactory.get().writeValueAsString(sleepWithExceptionJob));
    assertThat(jedis.llen(inFlightKey(worker, queue))).isEqualTo(0L);
  }

  @Test
  public void testJSONException() throws InterruptedException {
    final String queue = "baz";

    final Jedis jedis = TestUtils.createJedis(config);

    // Submit a job containing incorrect JSON.
    String incorrectJson = "{";
    jedis.sadd(createKey(config.getNamespace(), QUEUES), queue);
    jedis.rpush(createKey(config.getNamespace(), QUEUE, queue), incorrectJson);

    final Worker worker =
        new WorkerImpl(
            config,
            Arrays.asList(queue),
            new MapBasedJobFactory(Map.of(SleepAction.class.getSimpleName(), SleepAction.class)));
    final Thread workerThread = new Thread(worker);
    workerThread.start();

    Thread.sleep(1000);

    TestUtils.stopWorker(worker, workerThread, false);

    assertThat(jedis.llen(inFlightKey(worker, queue))).isEqualTo(0L);
  }

  private static String inFlightKey(final Worker worker, final String queue) {
    return createKey(config.getNamespace(), ResqueConstants.INFLIGHT, worker.getName(), queue);
  }
}
