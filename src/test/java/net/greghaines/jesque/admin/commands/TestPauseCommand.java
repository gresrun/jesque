package net.greghaines.jesque.admin.commands;

import net.greghaines.jesque.worker.Worker;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;

/**
 * Tests PauseCommand.
 * 
 * @author Greg Haines
 */
public class TestPauseCommand {

    @Test(expected=IllegalStateException.class)
    public void testRun_NoWorker() {
        final PauseCommand pauseCmd = new PauseCommand(false);
        pauseCmd.run();
    }

    @Test
    public void testRun() {
        final boolean pause = true;
        final Mockery mockCtx = new JUnit4Mockery();
        final Worker worker = mockCtx.mock(Worker.class);
        mockCtx.checking(new Expectations(){{
            oneOf(worker).togglePause(pause);
        }});
        final PauseCommand pauseCmd = new PauseCommand(pause);
        pauseCmd.setWorker(worker);
        pauseCmd.run();
        mockCtx.assertIsSatisfied();
    }
}
