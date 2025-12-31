package net.greghaines.jesque.admin.commands;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import net.greghaines.jesque.worker.Worker;
import org.junit.Test;

/**
 * Tests ShutdownCommand.
 *
 * @author Greg Haines
 */
public class TestShutdownCommand {

  @Test
  public void testRun_NoWorker() {
    final ShutdownCommand shutdownCmd = new ShutdownCommand(false);
    assertThrows(
        IllegalStateException.class,
        () -> {
          shutdownCmd.run();
        });
  }

  @Test
  public void testRun() {
    final boolean shutdown = true;
    final Worker worker = mock(Worker.class);
    doNothing().when(worker).end(shutdown);
    final ShutdownCommand shutdownCmd = new ShutdownCommand(shutdown);
    shutdownCmd.setWorker(worker);
    shutdownCmd.run();
    verify(worker).end(shutdown);
  }
}
