package net.greghaines.jesque.meta.dao.impl;

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
import org.junit.Assert;
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

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullConfig() {
    new KeysDAORedisImpl(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullPool() {
    final Config config = Config.getDefaultConfig();
    new KeysDAORedisImpl(config, null);
  }

  @Test
  public void testGetRedisInfo() {
    final String infoString = "foo:bar\r\n# CPU\r\nbaz\r\nqux";
    when(this.jedisPool.info()).thenReturn(infoString);
    final Map<String, String> redisInfo = this.keysDAO.getRedisInfo();
    Assert.assertNotNull(redisInfo);
    Assert.assertEquals(3, redisInfo.size());
    Assert.assertTrue(redisInfo.containsKey("foo"));
    Assert.assertEquals("bar", redisInfo.get("foo"));
    Assert.assertTrue(redisInfo.containsKey("baz"));
    Assert.assertNull(redisInfo.get("baz"));
    Assert.assertTrue(redisInfo.containsKey("qux"));
    Assert.assertNull(redisInfo.get("qux"));
  }

  @Test
  public void testGetKeyInfos() throws Exception {
    final Set<String> keys = new LinkedHashSet<String>(Arrays.asList("resque:bar", "resque:qux"));
    final List<String> keyNames = Arrays.asList("bar", "qux");
    final List<String> values = Arrays.asList("bazqux", "abc123456");
    when(this.jedisPool.keys("resque:*")).thenReturn(keys);
    int idx = 0;
    for (final String key : keys) {
      when(this.jedisPool.type(key)).thenReturn(KeyType.STRING.toString());
      when(this.jedisPool.strlen(key)).thenReturn((long) values.get(idx++).length());
    }
    final List<KeyInfo> keyInfos = this.keysDAO.getKeyInfos();
    Assert.assertNotNull(keyInfos);
    Assert.assertEquals(2, keyInfos.size());
    int i = 0;
    for (final KeyInfo keyInfo : keyInfos) {
      Assert.assertEquals("resque", keyInfo.getNamespace());
      Assert.assertEquals(keyNames.get(i), keyInfo.getName());
      Assert.assertEquals(KeyType.STRING, keyInfo.getType());
      final long size = values.get(i++).length();
      Assert.assertEquals((Long) size, keyInfo.getSize());
      Assert.assertNull(keyInfo.getArrayValue());
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.STRING, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNull(keyInfo.getArrayValue());
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.STRING, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(1, keyInfo.getArrayValue().size());
    Assert.assertEquals(value, keyInfo.getArrayValue().get(0));
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.STRING, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNull(keyInfo.getArrayValue());
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.STRING, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(1, keyInfo.getArrayValue().size());
    Assert.assertEquals(value, keyInfo.getArrayValue().get(0));
  }

  @Test
  public void testDoWork_HandleZSet() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.ZSET.toString());
    when(this.jedisPool.zcard(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.ZSET, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNull(keyInfo.getArrayValue());
  }

  @Test
  public void testDoWork_HandleZSet_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final List<String> value = Arrays.asList("foo", "bar", "baz");
    final long size = 8;
    final int offset = 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.ZSET.toString());
    when(this.jedisPool.zcard(key)).thenReturn(size);
    when(this.jedisPool.zrange(key, offset, offset + count)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.ZSET, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(count, keyInfo.getArrayValue().size());
    Assert.assertTrue(value.containsAll(keyInfo.getArrayValue()));
  }

  @Test
  public void testDoWork_HandleSet() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.SET, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNull(keyInfo.getArrayValue());
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.SET, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(1, keyInfo.getArrayValue().size());
    Assert.assertTrue(keyInfo.getArrayValue().contains("foo"));
  }

  @Test
  public void testDoWork_HandleSet_ArrayValue_TooBig() throws Exception {
    final String key = "foo:bar";
    final Set<String> value = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final long size = value.size();
    final int offset = value.size() + 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    when(this.jedisPool.smembers(key)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.SET, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertTrue(keyInfo.getArrayValue().isEmpty());
  }

  @Test
  public void testDoWork_HandleSet_ArrayValue_OverEnd() throws Exception {
    final String key = "foo:bar";
    final Set<String> value = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
    final long size = value.size();
    final int offset = value.size() - 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.SET.toString());
    when(this.jedisPool.scard(key)).thenReturn(size);
    when(this.jedisPool.smembers(key)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.SET, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(1, keyInfo.getArrayValue().size());
    Assert.assertTrue(keyInfo.getArrayValue().contains("baz"));
  }

  @Test
  public void testDoWork_HandleHash() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.HASH.toString());
    when(this.jedisPool.hlen(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.HASH, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNull(keyInfo.getArrayValue());
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.HASH, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(1, keyInfo.getArrayValue().size());
    Assert.assertTrue(keyInfo.getArrayValue().contains("{foo=123}"));
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.HASH, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertTrue(keyInfo.getArrayValue().isEmpty());
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
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.HASH, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(1, keyInfo.getArrayValue().size());
    Assert.assertTrue(keyInfo.getArrayValue().contains("{baz=789}"));
  }

  @Test
  public void testDoWork_HandleList() throws Exception {
    final String key = "foo:bar";
    final long size = 8;
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.LIST.toString());
    when(this.jedisPool.llen(key)).thenReturn(size);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.LIST, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNull(keyInfo.getArrayValue());
  }

  @Test
  public void testDoWork_HandleList_ArrayValue() throws Exception {
    final String key = "foo:bar";
    final List<String> value = Arrays.asList("foo", "bar", "baz");
    final long size = 8;
    final int offset = 1;
    final int count = value.size();
    final KeyDAOWork work = new KeyDAOWork(key, offset, count);
    when(this.jedisPool.type(key)).thenReturn(KeyType.LIST.toString());
    when(this.jedisPool.llen(key)).thenReturn(size);
    when(this.jedisPool.lrange(key, offset, offset + count)).thenReturn(value);
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNotNull(keyInfo);
    Assert.assertEquals("foo", keyInfo.getNamespace());
    Assert.assertEquals("bar", keyInfo.getName());
    Assert.assertEquals(KeyType.LIST, keyInfo.getType());
    Assert.assertEquals((Long) size, keyInfo.getSize());
    Assert.assertNotNull(keyInfo.getArrayValue());
    Assert.assertEquals(count, keyInfo.getArrayValue().size());
    Assert.assertTrue(value.containsAll(keyInfo.getArrayValue()));
  }

  @Test
  public void testDoWork_Unkonwn() throws Exception {
    final String key = "foo:bar";
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn("?");
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNull(keyInfo);
  }

  @Test
  public void testDoWork_None() throws Exception {
    final String key = "foo:bar";
    final KeyDAOWork work = new KeyDAOWork(key);
    when(this.jedisPool.type(key)).thenReturn(KeyType.NONE.toString());
    final KeyInfo keyInfo = work.doWork(this.jedisPool);
    Assert.assertNull(keyInfo);
  }
}
