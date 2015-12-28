package net.greghaines.jesque.meta.dao.impl;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.meta.KeyInfo;
import net.greghaines.jesque.meta.KeyType;
import net.greghaines.jesque.meta.dao.impl.KeysDAORedisImpl.KeyDAOPoolWork;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class TestKeysDAORedisImpl {

    private Mockery mockCtx;
    private Pool<Jedis> pool;
    private Jedis jedis;
    private KeysDAORedisImpl keysDAO;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        this.mockCtx.setThreadingPolicy(new Synchroniser());
        this.pool = this.mockCtx.mock(Pool.class);
        this.jedis = this.mockCtx.mock(Jedis.class);
        this.keysDAO = new KeysDAORedisImpl(new ConfigBuilder().build(), this.pool);
    }
    
    @After
    public void tearDown() {
        this.mockCtx.assertIsSatisfied();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new KeysDAORedisImpl(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullPool() {
        final Config config = new ConfigBuilder().build();
        new KeysDAORedisImpl(config, null);
    }
    
    @Test
    public void testGetRedisInfo() {
        final String infoString = "foo:bar\r\n# CPU\r\nbaz\r\nqux";
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).info(); will(returnValue(infoString));
            oneOf(jedis).close();
        }});
        final Map<String,String> redisInfo = this.keysDAO.getRedisInfo();
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
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).keys("resque:*"); will(returnValue(keys));
            int i = 0;
            for (final String key : keys) {
                oneOf(jedis).type(key); will(returnValue(KeyType.STRING.toString()));
                oneOf(jedis).strlen(key); will(returnValue((long)values.get(i++).length()));
            }
            oneOf(jedis).close();
        }});
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
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).type(key); will(returnValue(KeyType.STRING.toString()));
            oneOf(jedis).strlen(key); will(returnValue(size));
            oneOf(jedis).close();
        }});
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
        this.mockCtx.checking(new Expectations(){{
            oneOf(pool).getResource(); will(returnValue(jedis));
            oneOf(jedis).type(key); will(returnValue(KeyType.STRING.toString()));
            oneOf(jedis).strlen(key); will(returnValue(size));
            oneOf(jedis).get(key); will(returnValue(value));
            oneOf(jedis).close();
        }});
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.STRING.toString()));
            oneOf(jedis).strlen(key); will(returnValue(size));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, 0, 1);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.STRING.toString()));
            oneOf(jedis).strlen(key); will(returnValue(size));
            oneOf(jedis).get(key); will(returnValue(value));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.ZSET.toString()));
            oneOf(jedis).zcard(key); will(returnValue(size));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final Set<String> value = new LinkedHashSet<String>(Arrays.asList("foo", "bar", "baz"));
        final long size = 8;
        final int offset = 1;
        final int count = value.size();
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.ZSET.toString()));
            oneOf(jedis).zcard(key); will(returnValue(size));
            oneOf(jedis).zrange(key, offset, offset + count); will(returnValue(value));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.SET.toString()));
            oneOf(jedis).scard(key); will(returnValue(size));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.SET.toString()));
            oneOf(jedis).scard(key); will(returnValue(size));
            oneOf(jedis).smembers(key); will(returnValue(value));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.SET.toString()));
            oneOf(jedis).scard(key); will(returnValue(size));
            oneOf(jedis).smembers(key); will(returnValue(value));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.SET.toString()));
            oneOf(jedis).scard(key); will(returnValue(size));
            oneOf(jedis).smembers(key); will(returnValue(value));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.HASH.toString()));
            oneOf(jedis).hlen(key); will(returnValue(size));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.HASH.toString()));
            oneOf(jedis).hlen(key); will(returnValue(size));
            oneOf(jedis).hkeys(key); will(returnValue(valueKeys));
            oneOf(jedis).hmget(key, "foo"); will(returnValue(valueValues));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.HASH.toString()));
            oneOf(jedis).hlen(key); will(returnValue(size));
            oneOf(jedis).hkeys(key); will(returnValue(valueKeys));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.HASH.toString()));
            oneOf(jedis).hlen(key); will(returnValue(size));
            oneOf(jedis).hkeys(key); will(returnValue(valueKeys));
            oneOf(jedis).hmget(key, "baz"); will(returnValue(valueValues));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.LIST.toString()));
            oneOf(jedis).llen(key); will(returnValue(size));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key, offset, count);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.LIST.toString()));
            oneOf(jedis).llen(key); will(returnValue(size));
            oneOf(jedis).lrange(key, offset, offset + count); will(returnValue(value));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
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
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue("?"));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
        Assert.assertNull(keyInfo);
    }

    @Test
    public void testDoWork_None() throws Exception {
        final String key = "foo:bar";
        final KeyDAOPoolWork work = new KeyDAOPoolWork(key);
        this.mockCtx.checking(new Expectations(){{
            oneOf(jedis).type(key); will(returnValue(KeyType.NONE.toString()));
        }});
        final KeyInfo keyInfo = work.doWork(this.jedis);
        Assert.assertNull(keyInfo);
    }
}
