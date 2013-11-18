package net.greghaines.jesque.admin.commands;

import net.greghaines.jesque.worker.Worker;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;

/**
 * Tests ShutdownCommand.
 * 
 * @author Greg Haines
 */
public class TestShutdownCommand {

    @Test(expected=IllegalStateException.class)
    public void testRun_NoWorker() {
        final ShutdownCommand shutdownCmd = new ShutdownCommand(false);
        shutdownCmd.run();
    }

    @Test
    public void testRun() {
        final boolean shutdown = true;
        final Mockery mockCtx = new JUnit4Mockery();
        final Worker worker = mockCtx.mock(Worker.class);
        mockCtx.checking(new Expectations(){{
            oneOf(worker).end(shutdown);
        }});
        final ShutdownCommand shutdownCmd = new ShutdownCommand(shutdown);
        shutdownCmd.setWorker(worker);
        shutdownCmd.run();
        mockCtx.assertIsSatisfied();
    }
}
