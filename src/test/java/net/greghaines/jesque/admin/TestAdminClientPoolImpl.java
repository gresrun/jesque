package net.greghaines.jesque.admin;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class TestAdminClientPoolImpl {
    
    private static final Config CONFIG = new ConfigBuilder().build();

    @Mock
    private Pool<Jedis> jedisPool;
    @Mock
    private Jedis jedis;
    private AdminClientPoolImpl adminClient;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(this.jedisPool.getResource()).thenReturn(this.jedis);
        this.adminClient = new AdminClientPoolImpl(CONFIG, this.jedisPool);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new AdminClientPoolImpl(null, this.jedisPool);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullPool() {
        new AdminClientPoolImpl(CONFIG, null);
    }

    @Test
    public void testShutdownWorkers() {
        this.adminClient.shutdownWorkers(true);
        verify(this.jedis).publish("resque:channel:admin", 
                "{\"class\":\"ShutdownCommand\",\"args\":[true],\"vars\":null}");
    }
}
