package net.greghaines.jesque.admin;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.JesqueUtils.set;
import static net.greghaines.jesque.utils.ResqueConstants.ADMIN_CHANNEL;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.admin.commands.PauseCommand;
import net.greghaines.jesque.admin.commands.ShutdownCommand;
import net.greghaines.jesque.worker.ExceptionHandler;
import net.greghaines.jesque.worker.JobExecutor;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.RecoveryStrategy;

import org.junit.Assert;
import org.junit.Test;

public class TestAdminImpl {
    
    private static final Config CONFIG = new ConfigBuilder().build();

    @Test(expected=NullPointerException.class)
    public void testConstructor_OneArg_NullConfig() {
        new AdminImpl(null);
    }

    @Test(expected=NullPointerException.class)
    public void testConstructor_ThreeArg_NullConfig() {
        new AdminImpl(null, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_ThreeArg_NullChannels() {
        new AdminImpl(CONFIG, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_ThreeArg_NullJobFactory() {
        new AdminImpl(CONFIG, set(ADMIN_CHANNEL), null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullConfig() {
        new AdminImpl(null, null, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullChannels() {
        new AdminImpl(CONFIG, null, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullJobFactory() {
        new AdminImpl(CONFIG, set(ADMIN_CHANNEL), null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullJedis() {
        new AdminImpl(CONFIG, set(ADMIN_CHANNEL), new MapBasedJobFactory(map(
                entry("PauseCommand", PauseCommand.class), 
                entry("ShutdownCommand", ShutdownCommand.class))), null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetChannels_Null() {
        new AdminImpl(CONFIG).setChannels(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetChannels_NullChannel() {
        new AdminImpl(CONFIG).setChannels(set((String)null));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetChannels_EmptyChannel() {
        new AdminImpl(CONFIG).setChannels(set(""));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetExceptionHandler_Null() {
        new AdminImpl(CONFIG).setExceptionHandler(null);
    }

    @Test
    public void testSetExceptionHandler() {
        final AdminImpl admin = new AdminImpl(CONFIG);
        final ExceptionHandler handler = new ExceptionHandler(){

            /**
             * {@inheritDoc}
             */
            @Override
            public RecoveryStrategy onException(final JobExecutor jobExecutor, final Exception exception, 
                    final String curQueue) {
                return RecoveryStrategy.TERMINATE;
            }
        };
        admin.setExceptionHandler(handler);
        Assert.assertSame(handler, admin.getExceptionHandler());
    }

    @Test
    public void testBasics() {
        final AdminImpl admin = new AdminImpl(CONFIG);
        Assert.assertFalse(admin.isProcessingJob());
        Assert.assertFalse(admin.isShutdown());
        Assert.assertNotNull(admin.getJobFactory());
        Assert.assertNotNull(admin.getChannels());
        Assert.assertEquals(1, admin.getChannels().size());
        Assert.assertNotNull(admin.getExceptionHandler());
        Assert.assertTrue(admin.getReconnectAttempts() > 0);
        Assert.assertNull(admin.getWorker());
    }
}
