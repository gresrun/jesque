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
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestAdminPoolImpl {
    
    private static final Config CONFIG = new ConfigBuilder().build();

    @Mock
    private Pool<Jedis> jedisPool;
    @Mock
    private Jedis jedis;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(this.jedisPool.getResource()).thenReturn(this.jedis);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_TwoArg_NullConfig() {
        new AdminPoolImpl(null, this.jedisPool);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_TwoArg_NullPool() {
        new AdminPoolImpl(CONFIG, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullConfig() {
        new AdminPoolImpl(null, null, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullChannels() {
        new AdminPoolImpl(CONFIG, null, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullJobFactory() {
        new AdminPoolImpl(CONFIG, set(ADMIN_CHANNEL), null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_FourArg_NullPool() {
        new AdminImpl(CONFIG, set(ADMIN_CHANNEL), new MapBasedJobFactory(map(
                entry("PauseCommand", PauseCommand.class), 
                entry("ShutdownCommand", ShutdownCommand.class))), null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetChannels_Null() {
        new AdminPoolImpl(CONFIG, this.jedisPool).setChannels(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetChannels_NullChannel() {
        new AdminPoolImpl(CONFIG, this.jedisPool).setChannels(set((String)null));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetChannels_EmptyChannel() {
        new AdminPoolImpl(CONFIG, this.jedisPool).setChannels(set(""));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetExceptionHandler_Null() {
        new AdminPoolImpl(CONFIG, this.jedisPool).setExceptionHandler(null);
    }

    @Test
    public void testSetExceptionHandler() {
        final AdminPoolImpl admin = new AdminPoolImpl(CONFIG, this.jedisPool);
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
        final AdminPoolImpl admin = new AdminPoolImpl(CONFIG, this.jedisPool);
        Assert.assertFalse(admin.isProcessingJob());
        Assert.assertFalse(admin.isShutdown());
        Assert.assertNotNull(admin.getJobFactory());
        Assert.assertNotNull(admin.getChannels());
        Assert.assertEquals(1, admin.getChannels().size());
        Assert.assertNotNull(admin.getExceptionHandler());
        Assert.assertNull(admin.getWorker());
    }
}
