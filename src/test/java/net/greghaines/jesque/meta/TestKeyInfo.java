package net.greghaines.jesque.meta;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestKeyInfo {
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullKey() {
        new KeyInfo(null, null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_BadKey() {
        new KeyInfo("foo", null);
    }
    
    @Test
    public void testConstructor() {
        final KeyType type = KeyType.HASH;
        final KeyInfo keyInfo = new KeyInfo("foo:bar:baz:qux", type);
        Assert.assertEquals("foo", keyInfo.getNamespace());
        Assert.assertEquals("bar:baz:qux", keyInfo.getName());
        Assert.assertEquals(type, keyInfo.getType());
    }

    @Test
    public void testProperties() {
        final KeyInfo keyInfo = new KeyInfo();
        final String name = "foo";
        keyInfo.setName(name);
        Assert.assertEquals(name, keyInfo.getName());
        Assert.assertEquals(name, keyInfo.toString());
        final String namespace = "bar";
        keyInfo.setNamespace(namespace);
        Assert.assertEquals(namespace, keyInfo.getNamespace());
        final KeyType type = KeyType.HASH;
        keyInfo.setType(type);
        Assert.assertEquals(type, keyInfo.getType());
        final Long size = 3l;
        keyInfo.setSize(size);
        Assert.assertEquals(size, keyInfo.getSize());
        final List<String> arrayValue = Arrays.asList("foo", "bar");
        keyInfo.setArrayValue(arrayValue);
        Assert.assertEquals(arrayValue, keyInfo.getArrayValue());
    }
    
    @Test
    public void testCompareToEqualsHashCode() {
        final KeyInfo ki1 = new KeyInfo();
        Assert.assertTrue(ki1.compareTo(null) > 0);
        Assert.assertFalse(ki1.equals(null));
        Assert.assertTrue(ki1.equals(ki1));
        final KeyInfo ki2 = new KeyInfo();
        Assert.assertEquals(0, ki1.compareTo(ki2));
        Assert.assertTrue(ki1.equals(ki2));
        Assert.assertEquals(ki1.hashCode(), ki2.hashCode());
        ki1.setName("foo");
        Assert.assertTrue(ki1.compareTo(ki2) > 0);
        Assert.assertFalse(ki1.equals(ki2));
        ki1.setName(null);
        ki2.setName("foo");
        Assert.assertTrue(ki1.compareTo(ki2) < 0);
        Assert.assertFalse(ki1.equals(ki2));
        ki1.setName("foo");
        Assert.assertEquals(0, ki1.compareTo(ki2));
        Assert.assertTrue(ki1.equals(ki2));
        Assert.assertEquals(ki1.hashCode(), ki2.hashCode());
        ki1.setName("bar");
        Assert.assertTrue(ki1.compareTo(ki2) < 0);
        Assert.assertFalse(ki1.equals(ki2));
        ki1.setName("qux");
        Assert.assertTrue(ki1.compareTo(ki2) > 0);
        Assert.assertFalse(ki1.equals(ki2));
    }
}
