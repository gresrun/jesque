package net.greghaines.jesque;

import org.junit.Assert;
import org.junit.Test;

public class TestConfig {

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullHost() {
        new Config(null, 0, -1, null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_EmptyHost() {
        new Config("", 0, -1, null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_LowPort() {
        new Config("foo", 0, -1, null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_HighPort() {
        new Config("foo", Integer.MAX_VALUE, -1, null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NegativeTimeout() {
        new Config("foo", 123, -1, null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullNamespace() {
        new Config("foo", 123, 0, null, null, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_EmptyNamespace() {
        new Config("foo", 123, 0, null, "", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NegativeDatabase() {
        new Config("foo", 123, 0, null, "bar", -1);
    }

    @Test
    public void testConstructorAndGetters() {
        final Config config = new Config("foo", 123, 456, "secret", "bar", 10);
        Assert.assertEquals("foo", config.getHost());
        Assert.assertEquals(123, config.getPort());
        Assert.assertEquals(456, config.getTimeout());
        Assert.assertEquals("secret", config.getPassword());
        Assert.assertEquals("bar", config.getNamespace());
        Assert.assertEquals(10, config.getDatabase());
        Assert.assertEquals("foo", config.getHost());
        Assert.assertEquals("redis://foo:123/10", config.getURI());
        Assert.assertNotNull(config.toString());
        Assert.assertTrue(config.equals(config));
    }
}
