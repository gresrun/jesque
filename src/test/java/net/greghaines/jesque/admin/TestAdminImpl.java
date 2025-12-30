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

public class TestAdminImpl {

  private static final Config CONFIG = Config.getDefaultConfig();

  @Test(expected = NullPointerException.class)
  public void testConstructor_OneArg_NullConfig() {
    new AdminImpl(null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructor_ThreeArg_NullConfig() {
    new AdminImpl(null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_ThreeArg_NullChannels() {
    new AdminImpl(CONFIG, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_ThreeArg_NullJobFactory() {
    new AdminImpl(CONFIG, Set.of(ADMIN_CHANNEL), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullConfig() {
    new AdminImpl(null, null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullChannels() {
    new AdminImpl(CONFIG, null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullJobFactory() {
    new AdminImpl(CONFIG, Set.of(ADMIN_CHANNEL), null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_FourArg_NullJedis() {
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
    new AdminImpl(CONFIG).setChannels(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetChannels_NullChannel() {
    final Set<String> channels = new HashSet<>();
    channels.add(null);
    new AdminImpl(CONFIG).setChannels(channels);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetChannels_EmptyChannel() {
    new AdminImpl(CONFIG).setChannels(Set.of(""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetExceptionHandler_Null() {
    new AdminImpl(CONFIG).setExceptionHandler(null);
  }

  @Test
  public void testSetExceptionHandler() {
    final AdminImpl admin = new AdminImpl(CONFIG);
    final ExceptionHandler handler =
        (jobExecutor, exception, curQueue) -> RecoveryStrategy.TERMINATE;
    admin.setExceptionHandler(handler);
    assertThat(admin.getExceptionHandler()).isSameInstanceAs(handler);
  }

  @Test
  public void testBasics() {
    final AdminImpl admin = new AdminImpl(CONFIG);
    assertThat(admin.isProcessingJob()).isFalse();
    assertThat(admin.isShutdown()).isFalse();
    assertThat(admin.getJobFactory()).isNotNull();
    assertThat(admin.getChannels()).isNotNull();
    assertThat(admin.getChannels()).hasSize(1);
    assertThat(admin.getExceptionHandler()).isNotNull();
    assertThat(admin.getReconnectAttempts()).isGreaterThan(0);
    assertThat(admin.getWorker()).isNull();
  }
}
