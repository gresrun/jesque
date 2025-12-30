package net.greghaines.jesque.admin.commands;

import static org.mockito.Mockito.*;

import net.greghaines.jesque.worker.Worker;
import org.junit.Test;

/**
 * Tests PauseCommand.
 *
 * @author Greg Haines
 */
public class TestPauseCommand {

  @Test(expected = IllegalStateException.class)
  public void testRun_NoWorker() {
    final PauseCommand pauseCmd = new PauseCommand(false);
    pauseCmd.run();
  }

  @Test
  public void testRun() {
    final boolean pause = true;
    final Worker worker = mock(Worker.class);
    final PauseCommand pauseCmd = new PauseCommand(pause);
    pauseCmd.setWorker(worker);
    pauseCmd.run();
    verify(worker).togglePause(pause);
  }
}
