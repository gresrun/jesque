package net.greghaines.jesque.admin;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.utils.ResqueConstants.ADMIN_CHANNEL;
import static org.junit.Assert.assertThrows;

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

  @Test
  public void testConstructor_OneArg_NullConfig() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new AdminImpl(null);
        });
  }

  @Test
  public void testConstructor_ThreeArg_NullConfig() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new AdminImpl(null, null, null);
        });
  }

  @Test
  public void testConstructor_ThreeArg_NullChannels() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG, null, null);
        });
  }

  @Test
  public void testConstructor_ThreeArg_NullJobFactory() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG, Set.of(ADMIN_CHANNEL), null);
        });
  }

  @Test
  public void testConstructor_FourArg_NullConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(null, null, null, null);
        });
  }

  @Test
  public void testConstructor_FourArg_NullChannels() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG, null, null, null);
        });
  }

  @Test
  public void testConstructor_FourArg_NullJobFactory() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG, Set.of(ADMIN_CHANNEL), null, null);
        });
  }

  @Test
  public void testConstructor_FourArg_NullJedis() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
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
        });
  }

  @Test
  public void testSetChannels_Null() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG).setChannels(null);
        });
  }

  @Test
  public void testSetChannels_NullChannel() {
    final Set<String> channels = new HashSet<>();
    channels.add(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG).setChannels(channels);
        });
  }

  @Test
  public void testSetChannels_EmptyChannel() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG).setChannels(Set.of(""));
        });
  }

  @Test
  public void testSetExceptionHandler_Null() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new AdminImpl(CONFIG).setExceptionHandler(null);
        });
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
