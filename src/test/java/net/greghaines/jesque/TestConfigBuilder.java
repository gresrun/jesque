package net.greghaines.jesque;

import redis.clients.jedis.HostAndPort;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestConfigBuilder {

    @Test
    public void testGetDefaultConfig() {
        final Config config = Config.getDefaultConfig();
        Assert.assertNotNull(config);
        Assert.assertEquals(Config.Builder.DEFAULT_HOST, config.getHostAndPort().getHost());
        Assert.assertEquals(Config.Builder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(Config.Builder.DEFAULT_PORT, config.getHostAndPort().getPort());
        Assert.assertNull(config.getSentinels());
        Assert.assertNull(config.getMasterName());
    }

    @Test
    public void testConstructor_Cloning() {
        final Config orig = Config.newBuilder().withNamespace("foo").withDatabase(10)
                .withPassword("bar").withHostAndPort("abc.com", 123).withTimeout(10000).build();
        final Config copy = orig.toBuilder().build();
        Assert.assertEquals(orig.getHostAndPort(), copy.getHostAndPort());
        Assert.assertEquals(orig.getMasterName(), copy.getMasterName());
        Assert.assertEquals(orig.getSentinels(), copy.getSentinels());
        Assert.assertEquals(orig.getNamespace(), copy.getNamespace());
        Assert.assertEquals(orig.getJedisClientConfig().getConnectionTimeoutMillis(),
                copy.getJedisClientConfig().getConnectionTimeoutMillis());
        Assert.assertEquals(orig.getJedisClientConfig().getPassword(),
                copy.getJedisClientConfig().getPassword());
        Assert.assertEquals(orig.toString(), copy.toString());
    }

    @Test
    public void testWithHostAndPort() {
        final String myHost = "foobar";
        final int myPort = 1234;
        final Config config = Config.newBuilder().withHostAndPort(myHost, myPort).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(myHost, config.getHostAndPort().getHost());
        Assert.assertEquals(myPort, config.getHostAndPort().getPort());
        Assert.assertNull(config.getSentinels());
        Assert.assertNull(config.getMasterName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithHost_Null() {
        Config.newBuilder().withHostAndPort(null, 1234);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithHost_Empty() {
        Config.newBuilder().withHostAndPort("", 1234);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithPort_Low() {
        Config.newBuilder().withHostAndPort("localhost", 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithPort_High() {
        Config.newBuilder().withHostAndPort("localhost", Integer.MAX_VALUE);
    }

    @Test
    public void testWithTimeout() {
        final int myTimeout = 77777;
        final Config config = Config.newBuilder().withTimeout(myTimeout).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(myTimeout, config.getJedisClientConfig().getConnectionTimeoutMillis());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithTimeout_Negative() {
        Config.newBuilder().withTimeout(-1);
    }

    @Test
    public void testWithDatabase() {
        final int myDB = 2;
        final Config config = Config.newBuilder().withDatabase(myDB).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(myDB, config.getJedisClientConfig().getDatabase());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithDatabase_Negative() {
        Config.newBuilder().withDatabase(-1);
    }

    @Test
    public void testWithPassword() {
        final String myPassword = "s3r3t5!";
        final Config config = Config.newBuilder().withPassword(myPassword).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(myPassword, config.getJedisClientConfig().getPassword());
    }

    @Test
    public void testWithNamespace() {
        final String myNamespace = "foo";
        final Config config = Config.newBuilder().withNamespace(myNamespace).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(myNamespace, config.getNamespace());
    }

    @Test
    public void testWithMasterNameAndSentinels() {
        final String myMasterName = "foo";
        final Set<HostAndPort> mySentinels =
                new HashSet<>(Arrays.asList(new HostAndPort("a", 123), new HostAndPort("b", 456)));
        final Config config =
                Config.newBuilder().withMasterNameAndSentinels(myMasterName, mySentinels).build();
        Assert.assertNotNull(config);
        Assert.assertNull(config.getHostAndPort());
        Assert.assertEquals(mySentinels, config.getSentinels());
        Assert.assertEquals(myMasterName, config.getMasterName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithNamespace_Null() {
        Config.newBuilder().withNamespace(null);
    }

    @Test
    public void testWithNamespace_Empty() {
        final String myNamespace = "";
        final Config config = Config.newBuilder().withNamespace(myNamespace).build();
        Assert.assertEquals(myNamespace, config.getNamespace());
    }
}
