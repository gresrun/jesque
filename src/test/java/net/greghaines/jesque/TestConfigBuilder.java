package net.greghaines.jesque;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestConfigBuilder {

    @Test
    public void testGetDefaultConfig() {
        final Config config = ConfigBuilder.getDefaultConfig();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test
    public void testConstructor_NoArg() {
        final Config config = new ConfigBuilder().build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test
    public void testConstructor_Cloning() {
        final Config orig = new ConfigBuilder().withNamespace("foo")
                .withDatabase(10).withPassword("bar").withPort(123)
                .withHost("abc.com").withTimeout(10000).build();
        final Config copy = new ConfigBuilder(orig).build();
        TestUtils.assertFullyEquals(orig, copy);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_Cloning_Null() {
        new ConfigBuilder(null);
    }

    @Test
    public void testWithHost() {
        final String myHost = "foobar";
        final Config config = new ConfigBuilder().withHost(myHost).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(myHost, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithHost_Null() {
        new ConfigBuilder().withHost(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithHost_Empty() {
        new ConfigBuilder().withHost("");
    }

    @Test
    public void testWithPort() {
        final int myPort = 1234;
        final Config config = new ConfigBuilder().withPort(myPort).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(myPort, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithPort_Low() {
        new ConfigBuilder().withPort(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithPort_High() {
        new ConfigBuilder().withPort(Integer.MAX_VALUE);
    }

    @Test
    public void testWithTimeout() {
        final int myTimeout = 77777;
        final Config config = new ConfigBuilder().withTimeout(myTimeout).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
        Assert.assertEquals(myTimeout, config.getTimeout());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithTimeout_Negative() {
        new ConfigBuilder().withTimeout(-1);
    }

    @Test
    public void testWithDatabase() {
        final int myDB = 2;
        final Config config = new ConfigBuilder().withDatabase(myDB).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(myDB, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithDatabase_Negative() {
        new ConfigBuilder().withDatabase(-1);
    }

    @Test
    public void testWithPassword() {
        final String myPassword = "s3r3t5!";
        final Config config = new ConfigBuilder().withPassword(myPassword).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(myPassword, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test
    public void testWithNamespace() {
        final String myNamespace = "foo";
        final Config config = new ConfigBuilder().withNamespace(myNamespace).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(myNamespace, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(ConfigBuilder.DEFAULT_SENTINELS, config.getSentinels());
        Assert.assertEquals(ConfigBuilder.DEFAULT_MASTERNAME, config.getMasterName());
    }

    @Test
    public void testWithMasterNameAndSentinels() {
        final String myMasterName = "foo";
        final Set<String> mySentinels = new HashSet<>(Arrays.asList("a", "b"));
        final Config config = new ConfigBuilder().withSentinels(mySentinels).withMasterName(myMasterName).build();
        Assert.assertNotNull(config);
        Assert.assertEquals(ConfigBuilder.DEFAULT_HOST, config.getHost());
        Assert.assertEquals(ConfigBuilder.DEFAULT_NAMESPACE, config.getNamespace());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PASSWORD, config.getPassword());
        Assert.assertEquals(ConfigBuilder.DEFAULT_DATABASE, config.getDatabase());
        Assert.assertEquals(ConfigBuilder.DEFAULT_PORT, config.getPort());
        Assert.assertEquals(ConfigBuilder.DEFAULT_TIMEOUT, config.getTimeout());
        Assert.assertEquals(mySentinels, config.getSentinels());
        Assert.assertEquals(myMasterName, config.getMasterName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithNamespace_Null() {
        new ConfigBuilder().withNamespace(null);
    }

    @Test
    public void testWithNamespace_Empty() {
        final String myNamespace = "";
        final Config config = new ConfigBuilder().withNamespace(myNamespace).build();
        Assert.assertEquals(myNamespace, config.getNamespace());
    }
}
