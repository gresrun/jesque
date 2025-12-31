package net.greghaines.jesque.meta.dao.impl;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.meta.KeyInfo;
import net.greghaines.jesque.meta.KeyType;
import net.greghaines.jesque.meta.dao.impl.KeysDAORedisImpl.KeyDAOWork;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.UnifiedJedis;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestKeysDAORedisImpl {

  @Mock private UnifiedJedis jedisPool;
  private KeysDAORedisImpl keysDAO;

  @Before
  public void setUp() {
    this.keysDAO = new KeysDAORedisImpl(Config.getDefaultConfig(), this.jedisPool);
  }

  @Test
  public void testConstructor_NullConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new KeysDAORedisImpl(null, null);
        });
  }

  @Test
  public void testConstructor_NullPool() {
    final Config config = Config.getDefaultConfig();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new KeysDAORedisImpl(config, null);
        });
  }

  @Test
  public void testGetRedisInfo() {
    final String infoString = "foo:bar\r\n# CPU\r\nbaz\r\nqux";
    when(this.jedisPool.info()).thenReturn(infoString);
    final Map<String, String> redisInfo = this.keysDAO.getRedisInfo();
    assertThat(redisInfo).containsExactly("foo", "bar", "baz", null, "qux", null);
  }

  @Test
  public void testGetKeyInfos() throws Exception {
    final Set<String> keys = new LinkedHashSet<>(List.of("resque:bar", "resque:qux"));
    final List<String> keyNames = List.of("bar", "qux");
    final List<String> values = List.of("bazqux", "abc123456");
    when(this.jedisPool.keys("resque:*")).thenReturn(keys);
    int idx = 0;
    for (final String key : keys) {
      when(this.jedisPool.type(key)).thenReturn(KeyType.STRING.toString());
      when(this.jedisPool.strlen(key)).thenReturn((long) values.get(idx++).length());
    }
    final List<KeyInfo> keyInfos = this.keysDAO.getKeyInfos();
    assertThat(keyInfos).isNotNull();
    assertThat(keyInfos).hasSize(2);
    int i = 0;
    for (final KeyInfo keyInfo : keyInfos) {
      assertThat(keyInfo.getNamespace()).isEqualTo("resque");
      assertThat(keyInfo.getName()).isEqualTo(keyNames.get(i));
      assertThat(keyInfo.getType()).isEqualTo(KeyType.STRING);
      final long size = values.get(i++).length();
      assertThat(keyInfo.getSize()).isEqualTo((Long) size);
      assertThat(keyInfo.getArrayValue()).isNull();
    }
  }

  @Test
  public void testGetKeyInfo() throws Exception {
    final String key = "foo:bar";
    final String value = "bazqux";
    final long size = value.length();
    when(this.jedisPool.type(key)).thenReturn(KeyType.STRING.toString());
    when(this.jedisPool.strlen(key)).thenReturn(size);
    final KeyInfo keyInfo = this.keysDAO.getKeyInfo(key);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.STRING);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNull();
  }

  @Test
  public void testGetKeyInfo_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final String value = "bazqux";
    final long size = value.length();
    when(this.jedisPool.type(key)).thenReturn(KeyType.STRING.toString());
    when(this.jedisPool.strlen(key)).thenReturn(size);
    when(this.jedisPool.get(key)).thenReturn(value);
    final KeyInfo keyInfo = this.keysDAO.getKeyInfo(key, 0, 1);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.STRING);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).hasSize(1);
    assertThat(keyInfo.getArrayValue().get(0)).isEqualTo(value);
  }

  @Test
  public void testDoWork_HandleString() throws Exception {
    final String key = "foo:bar";
    final String value = "bazqux";
    final long size = value.length();
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.STRING.toString());
    when(this.jedisPool.strlen(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.STRING);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNull();
  }

  @Test
  public void testDoWork_HandleString_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final String value = "bazqux";
    final long size = value.length();
    final KeyDAOWork work = new KeyDAOWork(key, 0, 1);
    when(this.jedisPool.type(key)).thenReturn(KeyType.STRING.toString());
    when(this.jedisPool.strlen(key)).thenReturn(size);
    when(this.jedisPool.get(key)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.STRING);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).hasSize(1);
    assertThat(keyInfo.getArrayValue().get(0)).isEqualTo(value);
  }

  @Test
  public void testDoWork_HandleZSet() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.ZSET.toString());
    when(this.jedisPool.zcard(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.ZSET);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNull();
  }

  @Test
  public void testDoWork_HandleZSet_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final List<String> value = List.of("foo", "bar", "baz");
    final long size = 8;
    final int offset = 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.ZSET.toString());
    when(this.jedisPool.zcard(key)).thenReturn(size);
    when(this.jedisPool.zrange(key, offset, offset + count)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.ZSET);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).hasSize(count);
    assertThat(keyInfo.getArrayValue()).containsExactlyElementsIn(value);
  }

  @Test
  public void testDoWork_HandleSet() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.SET);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNull();
  }

  @Test
  public void testDoWork_HandleSet_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final Set<String> value = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final long size = value.size();
    final int offset = 0;
    final int count = 1;
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    when(this.jedisPool.smembers(key)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.SET);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).hasSize(1);
    assertThat(keyInfo.getArrayValue()).contains("foo");
  }

  @Test
  public void testDoWork_HandleSet_ArrayValue_TooBig() throws Exception {
    final String key = "foo:bar";
    final Set<String> value = Set.of("foo", "bar", "baz");
    final long size = value.size();
    final int offset = value.size() + 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    when(this.jedisPool.smembers(key)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.SET);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).isEmpty();
  }

  @Test
  public void testDoWork_HandleSet_ArrayValue_OverEnd() throws Exception {
    final String key = "foo:bar";
    // Need to retain order for this test
    final Set<String> value = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final long size = value.size();
    final int offset = value.size() - 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    when(this.jedisPool.smembers(key)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.SET);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).containsExactly("baz");
  }

  @Test
  public void testDoWork_HandleHash() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.HASH.toString());
    when(this.jedisPool.hlen(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.HASH);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNull();
  }

  @Test
  public void testDoWork_HandleHash_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final Set<String> valueKeys = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final List<String> valueValues = Arrays.asList("123");
    final long size = valueKeys.size();
    final int offset = 0;
    final int count = 1;
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.HASH.toString());
    when(this.jedisPool.hlen(key)).thenReturn(size);
    when(this.jedisPool.hkeys(key)).thenReturn(valueKeys);
    when(this.jedisPool.hmget(key, "foo")).thenReturn(valueValues);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.HASH);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).containsExactly("{foo=123}");
  }

  @Test
  public void testDoWork_HandleHash_ArrayValue_TooBig() throws Exception {
    final String key = "foo:bar";
    final Set<String> valueKeys = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final long size = valueKeys.size();
    final int offset = valueKeys.size() + 1;
    final int count = valueKeys.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.HASH.toString());
    when(this.jedisPool.hlen(key)).thenReturn(size);
    when(this.jedisPool.hkeys(key)).thenReturn(valueKeys);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.HASH);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).isEmpty();
  }

  @Test
  public void testDoWork_HandleHash_ArrayValue_OverEnd() throws Exception {
    final String key = "foo:bar";
    final Set<String> valueKeys = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final List<String> valueValues = Arrays.asList("789");
    final long size = valueKeys.size();
    final int offset = valueKeys.size() - 1;
    final int count = valueKeys.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.HASH.toString());
    when(this.jedisPool.hlen(key)).thenReturn(size);
    when(this.jedisPool.hkeys(key)).thenReturn(valueKeys);
    when(this.jedisPool.hmget(key, "baz")).thenReturn(valueValues);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.HASH);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).containsExactly("{baz=789}");
  }

  @Test
  public void testDoWork_HandleList() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.LIST.toString());
    when(this.jedisPool.llen(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.LIST);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNull();
  }

  @Test
  public void testDoWork_HandleList_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final List<String> value = List.of("foo", "bar", "baz");
    final long size = 8;
    final int offset = 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.LIST.toString());
    when(this.jedisPool.llen(key)).thenReturn(size);
    when(this.jedisPool.lrange(key, offset, offset + count)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNotNull();
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar");
    assertThat(keyInfo.getType()).isEqualTo(KeyType.LIST);
    assertThat(keyInfo.getSize()).isEqualTo((Long) size);
    assertThat(keyInfo.getArrayValue()).isNotNull();
    assertThat(keyInfo.getArrayValue()).hasSize(count);
    assertThat(keyInfo.getArrayValue()).containsAtLeastElementsIn(value);
  }

  @Test
  public void testDoWork_Unkonwn() throws Exception {
    final String key = "foo:bar";
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn("?");
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNull();
  }

  @Test
  public void testDoWork_None() throws Exception {
    final String key = "foo:bar";
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.NONE.toString());
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    assertThat(keyInfo).isNull();
  }
}
