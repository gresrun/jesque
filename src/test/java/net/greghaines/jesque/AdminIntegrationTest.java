package net.greghaines.jesque;

import static net.greghaines.jesque.Config.Builder.DEFAULT_NAMESPACE;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import net.greghaines.jesque.admin.Admin;
import net.greghaines.jesque.admin.AdminClient;
import net.greghaines.jesque.admin.AdminClientImpl;
import net.greghaines.jesque.admin.AdminImpl;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import net.greghaines.jesque.worker.WorkerPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(AdminIntegrationTest.class);
  private static final String TEST_QUEUE = "foo";

  private static Config config;

  @BeforeClass
  public static void initConfig() {
    int suffix = new Random().nextInt(10000);
    config = Config.newBuilder().withNamespace(DEFAULT_NAMESPACE + suffix).build();
  }

  @Before
  public void resetRedis() {
    TestUtils.resetRedis(config);
  }

  @Test
  public void testAdminAndWorkerPool() {
    final WorkerPool workerPool =
        new WorkerPool(
            new Callable<WorkerImpl>() {
              public WorkerImpl call() {
                return new WorkerImpl(
                    config,
                    Set.of(TEST_QUEUE),
                    new MapBasedJobFactory(
                        Map.of(TestAction.class.getSimpleName(), TestAction.class)));
              }
            },
            2);
    final Admin admin = new AdminImpl(config);
    admin.setWorker(workerPool);

    workerPool.run();
    final Thread adminThread = new Thread(admin);
    adminThread.start();

    Assert.assertFalse(workerPool.isPaused());

    try {
      // TODO: Do client stuff here
    } finally {
      TestUtils.stopWorker(admin, adminThread);
      try {
        workerPool.endAndJoin(false, 1000);
      } catch (Exception e) {
        LOG.warn("Exception while waiting for workerThread to join", e);
      }
    }
  }

  @Ignore
  @Test
  public void testPauseAndShutdownCommands() {
    final Worker worker =
        new WorkerImpl(
            config,
            Set.of(TEST_QUEUE),
            new MapBasedJobFactory(Map.of(TestAction.class.getSimpleName(), TestAction.class)));
    final Admin admin = new AdminImpl(config);
    admin.setWorker(worker);

    final Thread workerThread = new Thread(worker);
    workerThread.start();
    final Thread adminThread = new Thread(admin);
    adminThread.start();

    Assert.assertFalse(worker.isPaused());

    try {
      final AdminClient adminClient = new AdminClientImpl(config);
      try {
        adminClient.togglePausedWorkers(true);
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException ie) {
        }
        Assert.assertTrue(worker.isPaused());

        Assert.assertFalse(worker.isShutdown());
        adminClient.shutdownWorkers(true);
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ie) {
        }
        Assert.assertTrue(worker.isShutdown());
      } finally {
        adminClient.end();
      }
    } finally {
      TestUtils.stopWorker(admin, adminThread);
      TestUtils.stopWorker(worker, workerThread);
    }
  }
}
