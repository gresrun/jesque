package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.HashSet;
import net.greghaines.jesque.Config;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisSentinelClient;
import redis.clients.jedis.UnifiedJedis;

public class TestPoolUtils {

  @Test
  public void testGetDefaultPoolConfig() {
    assertThat(PoolUtils.getDefaultPoolConfig()).isNotNull();
  }

  @Test
  public void testCreateJedisPool_NullConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PoolUtils.createJedisPool(null);
        });
  }

  @Test
  public void testCreateJedisPool() {
    final Config config = Config.getDefaultConfig();
    final UnifiedJedis pool = PoolUtils.createJedisPool(config);
    assertThat(pool).isInstanceOf(RedisClient.class);
  }

  /**
   * This will need a sentinel running with the following config
   *
   * <p>sentinel monitor mymaster 127.0.0.1 6379 1 sentinel down-after-milliseconds mymaster 6000
   * sentinel failover-timeout mymaster 180000 sentinel parallel-syncs mymaster 1
   *
   * <p>You should also have a redis-server running to act as the master [mymaster]
   */
  @Test
  @Ignore("Will only work with sentinel running and travis-ci sentinel support is sketchy at best")
  public void testCreateJedisSentinelPool() {
    final Config config =
        Config.newBuilder()
            .withMasterNameAndSentinels(
                "mymaster",
                new HashSet<>(Collections.singletonList(new HostAndPort("localhost", 26379))))
            .build();
    final UnifiedJedis pool = PoolUtils.createJedisPool(config);
    assertThat(pool).isInstanceOf(RedisSentinelClient.class);
  }

  @Test
  public void testCreateJedisPool_NullPoolConfig() {
    final Config config = Config.getDefaultConfig();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PoolUtils.createJedisPool(config, null);
        });
  }
}
