package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.TestUtils.createJedis;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.TestUtils;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class TestJedisUtils {

  private static final Config CONFIG = Config.getDefaultConfig();
  private static final String TEST_KEY = "foo";

  @Before
  public void resetRedis() {
    TestUtils.resetRedis(CONFIG);
  }

  @Test
  public void testEnsureJedisConnection_Success() {
    final Jedis jedis = createJedis(CONFIG);
    assertThat(JedisUtils.ensureJedisConnection(jedis)).isTrue();
    assertThat(JedisUtils.testJedisConnection(jedis)).isTrue();
  }

  @Test
  public void testEnsureJedisConnection_Fail() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.disconnect();
    assertThat(JedisUtils.ensureJedisConnection(jedis)).isFalse();
    assertThat(JedisUtils.testJedisConnection(jedis)).isTrue();
  }

  @Test
  public void testTestJedisConnection_Success() {
    assertThat(JedisUtils.testJedisConnection(createJedis(CONFIG))).isTrue();
  }

  @Test
  public void testTestJedisConnection_Fail() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.disconnect();
    assertThat(JedisUtils.testJedisConnection(jedis)).isFalse();
  }

  @Test
  public void testReconnect_Success() {
    assertThat(JedisUtils.reconnect(createJedis(CONFIG), 1, 1)).isTrue();
  }

  @Test
  public void testIsRegularQueue_Success() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.lpush(TEST_KEY, "bar");
    assertThat(JedisUtils.isRegularQueue(jedis, TEST_KEY)).isTrue();
  }

  @Test
  public void testIsRegularQueue_Failure() {
    assertThat(JedisUtils.isRegularQueue(createJedis(CONFIG), TEST_KEY)).isFalse();
  }

  @Test
  public void testIsDelayedQueue_Success() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.zadd(TEST_KEY, 1.0, "bar");
    assertThat(JedisUtils.isDelayedQueue(jedis, TEST_KEY)).isTrue();
  }

  @Test
  public void testIsDelayedQueue_Failure() {
    assertThat(JedisUtils.isDelayedQueue(createJedis(CONFIG), TEST_KEY)).isFalse();
  }

  @Test
  public void testIsKeyUsed_Success() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.set(TEST_KEY, "bar");
    assertThat(JedisUtils.isKeyUsed(jedis, TEST_KEY)).isTrue();
  }

  @Test
  public void testIsKeyUsed_Failure() {
    assertThat(JedisUtils.isKeyUsed(createJedis(CONFIG), "foo")).isFalse();
  }

  @Test
  public void testCanUseAsDelayedQueue_Success_ZSet() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.zadd(TEST_KEY, 1.0, "bar");
    assertThat(JedisUtils.canUseAsDelayedQueue(jedis, TEST_KEY)).isTrue();
  }

  @Test
  public void testCanUseAsDelayedQueue_Success_None() {
    assertThat(JedisUtils.canUseAsDelayedQueue(createJedis(CONFIG), TEST_KEY)).isTrue();
  }

  @Test
  public void testCanUseAsDelayedQueue_Failure() {
    final Jedis jedis = createJedis(CONFIG);
    jedis.set(TEST_KEY, "bar");
    assertThat(JedisUtils.canUseAsDelayedQueue(jedis, TEST_KEY)).isFalse();
  }
}
