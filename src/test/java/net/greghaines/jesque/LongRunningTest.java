package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import net.greghaines.jesque.worker.WorkerListener;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class LongRunningTest {

  private static final Logger log = LoggerFactory.getLogger(LongRunningTest.class);
  private static final Config config = Config.getDefaultConfig();

  private static void sleepTight(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void changeRedisTimeout(final long seconds) {
    final Jedis jedis = TestUtils.createJedis(config);
    jedis.configSet("timeout", Long.toString(seconds));
    jedis.disconnect();
  }

  @Before
  public void resetRedis() throws Exception {
    TestUtils.resetRedis(config);
  }

  @Test
  @Ignore
  public void issue28() throws InterruptedException {
    changeRedisTimeout(10);
    TestUtils.enqueueJobs(
        "longRunning",
        Arrays.asList(new Job(LongRunningAction.class.getSimpleName(), 20 * 1000L)),
        config);
    final Worker worker2 =
        new WorkerImpl(
            config,
            Arrays.asList("longRunning"),
            new MapBasedJobFactory(
                Map.of(LongRunningAction.class.getSimpleName(), LongRunningAction.class)));
    final AtomicBoolean successRef = new AtomicBoolean(false);
    worker2
        .getWorkerEventEmitter()
        .addListener(
            new WorkerListener() {
              @Override
              public void onEvent(
                  final WorkerEvent event,
                  final Worker worker,
                  final String queue,
                  final Job job,
                  final Object runner,
                  final Object result,
                  final Throwable t) {
                successRef.set(true);
                log.info("SUCCCESS: {}", job);
              }
            },
            WorkerEvent.JOB_SUCCESS);
    final Thread workerThread2 = new Thread(worker2);
    workerThread2.start();
    sleepTight(1000);
    worker2.end(false);
    workerThread2.join();

    assertThat(successRef.get()).isTrue();
    changeRedisTimeout(0);
  }

  public static class LongRunningAction implements Runnable {

    private final int millis;

    public LongRunningAction(final int millis) {
      this.millis = millis;
    }

    public void run() {
      sleepTight(this.millis);
    }
  }
}
