package net.greghaines.jesque.admin;

import net.greghaines.jesque.Config;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.UnifiedJedis;

public class TestAdminClientPoolImpl {

  private static final Config CONFIG = Config.getDefaultConfig();

  private Mockery mockCtx;
  private UnifiedJedis jedisPool;
  private AdminClientPoolImpl adminClient;

  @Before
  public void setUp() {
    this.mockCtx = new JUnit4Mockery();
    this.mockCtx.setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
    this.mockCtx.setThreadingPolicy(new Synchroniser());
    this.jedisPool = this.mockCtx.mock(UnifiedJedis.class);
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
    this.mockCtx.checking(
        new Expectations() {
          {
            oneOf(jedisPool)
                .publish(
                    "resque:channel:admin",
                    "{\"class\":\"ShutdownCommand\",\"args\":[true],\"vars\":null}");
            will(returnValue(0L));
          }
        });
    this.adminClient.shutdownWorkers(true);
  }
}
