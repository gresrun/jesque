package net.greghaines.jesque.admin;

import static org.mockito.Mockito.*;

import net.greghaines.jesque.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.UnifiedJedis;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestAdminClientPoolImpl {

  private static final Config CONFIG = Config.getDefaultConfig();

  @Mock private UnifiedJedis jedisPool;
  private AdminClientPoolImpl adminClient;

  @Before
  public void setUp() {
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
    when(this.jedisPool.publish(
            "resque:channel:admin",
            "{\"class\":\"ShutdownCommand\",\"args\":[true],\"vars\":null}"))
        .thenReturn(0L);
    this.adminClient.shutdownWorkers(true);
    verify(this.jedisPool)
        .publish(
            "resque:channel:admin",
            "{\"class\":\"ShutdownCommand\",\"args\":[true],\"vars\":null}");
  }
}
