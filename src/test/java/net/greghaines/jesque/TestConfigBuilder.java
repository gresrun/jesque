package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

public class TestConfigBuilder {

  @Test
  public void testGetDefaultConfig() {
    final Config config = Config.getDefaultConfig();
    assertThat(config).isNotNull();
    assertThat(config.getHostAndPort().getHost()).isEqualTo(Config.Builder.DEFAULT_HOST);
    assertThat(config.getNamespace()).isEqualTo(Config.Builder.DEFAULT_NAMESPACE);
    assertThat(config.getHostAndPort().getPort()).isEqualTo(Config.Builder.DEFAULT_PORT);
    assertThat(config.getSentinels()).isNull();
    assertThat(config.getMasterName()).isNull();
  }

  @Test
  public void testConstructor_Cloning() {
    final Config orig =
        Config.newBuilder()
            .withNamespace("foo")
            .withDatabase(10)
            .withPassword("bar")
            .withHostAndPort("abc.com", 123)
            .withTimeout(10000)
            .build();
    final Config copy = orig.toBuilder().build();
    assertThat(copy.getHostAndPort()).isEqualTo(orig.getHostAndPort());
    assertThat(copy.getMasterName()).isEqualTo(orig.getMasterName());
    assertThat(copy.getSentinels()).isEqualTo(orig.getSentinels());
    assertThat(copy.getNamespace()).isEqualTo(orig.getNamespace());
    assertThat(copy.getJedisClientConfig().getConnectionTimeoutMillis())
        .isEqualTo(orig.getJedisClientConfig().getConnectionTimeoutMillis());
    assertThat(copy.getJedisClientConfig().getPassword())
        .isEqualTo(orig.getJedisClientConfig().getPassword());
    assertThat(copy.toString()).isEqualTo(orig.toString());
  }

  @Test
  public void testWithHostAndPort() {
    final String myHost = "foobar";
    final int myPort = 1234;
    final Config config = Config.newBuilder().withHostAndPort(myHost, myPort).build();
    assertThat(config).isNotNull();
    assertThat(config.getHostAndPort().getHost()).isEqualTo(myHost);
    assertThat(config.getHostAndPort().getPort()).isEqualTo(myPort);
    assertThat(config.getSentinels()).isNull();
    assertThat(config.getMasterName()).isNull();
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
    assertThat(config).isNotNull();
    assertThat(config.getJedisClientConfig().getConnectionTimeoutMillis()).isEqualTo(myTimeout);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithTimeout_Negative() {
    Config.newBuilder().withTimeout(-1);
  }

  @Test
  public void testWithDatabase() {
    final int myDB = 2;
    final Config config = Config.newBuilder().withDatabase(myDB).build();
    assertThat(config).isNotNull();
    assertThat(config.getJedisClientConfig().getDatabase()).isEqualTo(myDB);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithDatabase_Negative() {
    Config.newBuilder().withDatabase(-1);
  }

  @Test
  public void testWithPassword() {
    final String myPassword = "s3r3t5!";
    final Config config = Config.newBuilder().withPassword(myPassword).build();
    assertThat(config).isNotNull();
    assertThat(config.getJedisClientConfig().getPassword()).isEqualTo(myPassword);
  }

  @Test
  public void testWithNamespace() {
    final String myNamespace = "foo";
    final Config config = Config.newBuilder().withNamespace(myNamespace).build();
    assertThat(config).isNotNull();
    assertThat(config.getNamespace()).isEqualTo(myNamespace);
  }

  @Test
  public void testWithMasterNameAndSentinels() {
    final String myMasterName = "foo";
    final Set<HostAndPort> mySentinels =
        new HashSet<>(Arrays.asList(new HostAndPort("a", 123), new HostAndPort("b", 456)));
    final Config config =
        Config.newBuilder().withMasterNameAndSentinels(myMasterName, mySentinels).build();
    assertThat(config).isNotNull();
    assertThat(config.getHostAndPort()).isNull();
    assertThat(config.getSentinels()).isEqualTo(mySentinels);
    assertThat(config.getMasterName()).isEqualTo(myMasterName);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithNamespace_Null() {
    Config.newBuilder().withNamespace(null);
  }

  @Test
  public void testWithNamespace_Empty() {
    final String myNamespace = "";
    final Config config = Config.newBuilder().withNamespace(myNamespace).build();
    assertThat(config.getNamespace()).isEqualTo(myNamespace);
  }
}
