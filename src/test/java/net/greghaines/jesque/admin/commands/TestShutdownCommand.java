package net.greghaines.jesque.admin.commands;

import static org.mockito.Mockito.*;

import net.greghaines.jesque.worker.Worker;
import org.junit.Test;

/**
 * Tests ShutdownCommand.
 *
 * @author Greg Haines
 */
public class TestShutdownCommand {

  @Test(expected = IllegalStateException.class)
  public void testRun_NoWorker() {
    final ShutdownCommand shutdownCmd = new ShutdownCommand(false);
    shutdownCmd.run();
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
