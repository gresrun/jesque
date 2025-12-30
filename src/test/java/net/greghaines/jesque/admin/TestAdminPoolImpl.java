package net.greghaines.jesque.admin;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.utils.ResqueConstants.ADMIN_CHANNEL;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.admin.commands.PauseCommand;
import net.greghaines.jesque.admin.commands.ShutdownCommand;
import net.greghaines.jesque.worker.ExceptionHandler;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.RecoveryStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.UnifiedJedis;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestAdminPoolImpl {

  private static final Config CONFIG = Config.getDefaultConfig();

  @Mock private UnifiedJedis jedisPool;

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_TwoArg_NullConfig() {
    new AdminPoolImpl(null, this.jedisPool);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_TwoArg_NullPool() {
    new AdminPoolImpl(CONFIG, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullConfig() {
    new AdminPoolImpl(null, null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullChannels() {
    new AdminPoolImpl(CONFIG, null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullJobFactory() {
    new AdminPoolImpl(CONFIG, Set.of(ADMIN_CHANNEL), null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullPool() {
    new AdminImpl(
        CONFIG,
        Set.of(ADMIN_CHANNEL),
        new MapBasedJobFactory(
            Map.of(
                PauseCommand.class.getSimpleName(),
                PauseCommand.class,
                ShutdownCommand.class.getSimpleName(),
                ShutdownCommand.class)),
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetChannels_Null() {
    new AdminPoolImpl(CONFIG, this.jedisPool).setChannels(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetChannels_NullChannel() {
    final Set<String> channels = new HashSet<>();
    channels.add(null);
    new AdminPoolImpl(CONFIG, this.jedisPool).setChannels(channels);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetChannels_EmptyChannel() {
    new AdminPoolImpl(CONFIG, this.jedisPool).setChannels(Set.of(""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetExceptionHandler_Null() {
    new AdminPoolImpl(CONFIG, this.jedisPool).setExceptionHandler(null);
  }

  @Test
  public void testSetExceptionHandler() {
    final AdminPoolImpl admin = new AdminPoolImpl(CONFIG, this.jedisPool);
    final ExceptionHandler handler =
        (jobExecutor, exception, curQueue) -> RecoveryStrategy.TERMINATE;
    admin.setExceptionHandler(handler);
    assertThat(admin.getExceptionHandler()).isSameInstanceAs(handler);
  }

  @Test
  public void testBasics() {
    final AdminPoolImpl admin = new AdminPoolImpl(CONFIG, this.jedisPool);
    assertThat(admin.isProcessingJob()).isFalse();
    assertThat(admin.isShutdown()).isFalse();
    assertThat(admin.getJobFactory()).isNotNull();
    assertThat(admin.getChannels()).isNotNull();
    assertThat(admin.getChannels()).hasSize(1);
    assertThat(admin.getExceptionHandler()).isNotNull();
    assertThat(admin.getWorker()).isNull();
  }
}
