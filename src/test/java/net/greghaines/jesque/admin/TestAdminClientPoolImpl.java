package net.greghaines.jesque.admin;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Test;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

public class TestAdminClientPoolImpl {

    private static final Config CONFIG = new ConfigBuilder().build();

    private Mockery mockCtx;
    private Pool<Jedis> jedisPool;
    private Jedis jedis;
    private AdminClientPoolImpl adminClient;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
        this.mockCtx.setThreadingPolicy(new Synchroniser());
        this.jedisPool = this.mockCtx.mock(Pool.class);
        this.jedis = this.mockCtx.mock(Jedis.class);
        this.mockCtx.checking(new Expectations() {
            {
                oneOf(jedisPool).getResource();
                will(returnValue(jedis));
                oneOf(jedis).close();
            }
        });
        this.adminClient = new AdminClientPoolImpl(CONFIG, this.jedisPool);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new AdminClientPoolImpl(null, this.jedisPool);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullPool() {
        new AdminClientPoolImpl(CONFIG, null);
    }

    @Test
    public void testShutdownWorkers() {
        this.mockCtx.checking(new Expectations() {
            {
                oneOf(jedis).publish("resque:channel:admin",
                        "{\"class\":\"ShutdownCommand\",\"args\":[true],\"vars\":null}");
                will(returnValue(0L));
            }
        });
        this.adminClient.shutdownWorkers(true);
    }
}
