package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.TestUtils.createTestActionJobFactory;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import net.greghaines.jesque.Config;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class TestWorkerImpl {

  private static final Config CONFIG = Config.getDefaultConfig();

  @Test
  public void testConstructor_NullConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new WorkerImpl(null, null, null, null);
        });
  }

  @Test
  public void testConstructor_NullQueues() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new WorkerImpl(CONFIG, null, null, null);
        });
  }

  @Test
  public void testConstructor_NullJobFactory() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new WorkerImpl(CONFIG, Collections.<String>emptyList(), null, null);
        });
  }

  @Test
  public void testConstructor_NullJedis() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new WorkerImpl(
              CONFIG, Collections.<String>emptyList(), createTestActionJobFactory(), null);
        });
  }

  @Test
  public void testConstructor_NullNextQueueStrategy() {
    final Jedis jedis = mock(Jedis.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new WorkerImpl(
              CONFIG, Collections.<String>emptyList(), createTestActionJobFactory(), jedis, null);
        });
  }

  @Test
  public void testSetThreadNameChangingEnabled() {
    WorkerImpl.setThreadNameChangingEnabled(true);
    assertThat(WorkerImpl.isThreadNameChangingEnabled()).isTrue();
    WorkerImpl.setThreadNameChangingEnabled(false);
    assertThat(WorkerImpl.isThreadNameChangingEnabled()).isFalse();
  }

  @Test
  public void testCheckQueues_Null() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          WorkerImpl.checkQueues(null);
        });
  }

  @Test
  public void testCheckQueues_NullQueue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          WorkerImpl.checkQueues(Arrays.asList("foo", null));
        });
  }

  @Test
  public void testCheckQueues_EmptyQueue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          WorkerImpl.checkQueues(Arrays.asList("foo", ""));
        });
  }

  @Test
  public void testCheckQueues_OK() {
    WorkerImpl.checkQueues(Arrays.asList("foo", "bar"));
  }

  @Test
  public void verifyNoExceptionsForAllNextQueueStrategies() throws InterruptedException {
    final MapBasedJobFactory jobFactory =
        new MapBasedJobFactory(Collections.<String, Class<?>>emptyMap());
    for (NextQueueStrategy nextQueueStrategy : NextQueueStrategy.values()) {
      final Jedis jedis = mock(Jedis.class);
      final WorkerImpl worker =
          new WorkerImpl(CONFIG, new ArrayList<String>(), jobFactory, jedis, nextQueueStrategy);
      worker.pop(worker.getNextQueue());
    }
  }
}
