/*
 * Copyright 2011 Greg Haines
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package net.greghaines.jesque;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import net.greghaines.jesque.utils.VersionUtils;
import redis.clients.jedis.ClientSetInfoConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;

/**
 * An immutable configuration bean for use with the rest of the project.
 *
 * @author Greg Haines
 */
public final class Config {

  private final HostAndPort hostAndPort;
  private final String masterName;
  private final Set<HostAndPort> sentinels;
  private final String namespace;
  private final JedisClientConfig clientConfig;

  private Config(final Builder Builder) {
    this.hostAndPort = Builder.hostAndPort;
    this.masterName = Builder.masterName;
    this.sentinels = Builder.sentinels;
    this.namespace = Builder.namespace;
    this.clientConfig = Builder.clientBuilder.build();
  }

  /**
   * @return the Redis host and port
   */
  public HostAndPort getHostAndPort() {
    return this.hostAndPort;
  }

  /**
   * @return the Redis master name
   */
  public String getMasterName() {
    return this.masterName;
  }

  /**
   * @return the Redis set of sentinels
   */
  public Set<HostAndPort> getSentinels() {
    return this.sentinels;
  }

  /**
   * @return the Redis namespace to prefix keys with
   */
  public String getNamespace() {
    return this.namespace;
  }

  /**
   * @return the Redis protocol URI this Config will connect to
   */
  public String getURI() {
    return "redis://"
        + this.hostAndPort.getHost()
        + ":"
        + this.hostAndPort.getPort()
        + "/"
        + this.clientConfig.getDatabase();
  }

  public JedisClientConfig getJedisClientConfig() {
    return this.clientConfig;
  }

  /**
   * @return a new Builder with the values from this Config
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "<" + getURI() + " namespace=" + this.namespace + ">";
  }

  /**
   * @return a Config with all the default values set
   */
  public static Config getDefaultConfig() {
    return new Builder().build();
  }

  /**
   * @return a new Builder with all the default values set
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A fluent-style builder for {@link Config}s.
   *
   * @author Greg Haines
   * @see Config
   */
  public static class Builder {

    /** localhost */
    public static final String DEFAULT_HOST = "localhost";

    /** Default Redis port is 6379 */
    public static final int DEFAULT_PORT = 6379;

    /** All Resque clients use "resque" by default */
    public static final String DEFAULT_NAMESPACE = "resque";

    private HostAndPort hostAndPort = new HostAndPort(DEFAULT_HOST, DEFAULT_PORT);
    private String masterName = null;
    private Set<HostAndPort> sentinels = null;
    private String namespace = DEFAULT_NAMESPACE;
    private final DefaultJedisClientConfig.Builder clientBuilder;

    /** No-arg constructor. */
    private Builder() {
      this.clientBuilder = DefaultJedisClientConfig.builder();
      setClientSetInfoConfig();
    }

    /**
     * Create a new Builder using an existing Config as the starting point.
     *
     * @param startingPoint the Config instance to copy the values from
     */
    private Builder(final Config startingPoint) {
      if (startingPoint == null) {
        throw new IllegalArgumentException("startingPoint must not be null");
      }
      this.hostAndPort = startingPoint.getHostAndPort();
      this.namespace = startingPoint.getNamespace();
      this.clientBuilder =
          DefaultJedisClientConfig.builder().from(startingPoint.getJedisClientConfig());
      setClientSetInfoConfig();
    }

    private void setClientSetInfoConfig() {
      this.clientBuilder.clientSetInfoConfig(
          ClientSetInfoConfig.withLibNameSuffix("jesque_v" + VersionUtils.getVersion()));
    }

    /**
     * Configs created by this Builder will connect to the specified hostname and port.
     *
     * <p>Clients created from this config will not use sentinels.
     *
     * @param host the Redis hostname
     * @param port the Redis port number
     * @return this Builder
     */
    public Builder withHostAndPort(final String host, int port) {
      if (host == null || "".equals(host)) {
        throw new IllegalArgumentException("host must not be null or empty: " + host);
      }
      if (port < 1 || port > 65535) {
        throw new IllegalArgumentException(
            "port must be a valid port in the range 1-65535: " + port);
      }
      this.hostAndPort = new HostAndPort(host, port);
      this.masterName = null;
      this.sentinels = null;
      return this;
    }

    /**
     * Configs created by this Builder will connect to the specified master name and sentinels.
     *
     * <p>Clients created from this config will not use hostname and port.
     *
     * @param masterName the Redis master name
     * @param sentinels the set of Redis sentinel hostnames and ports
     * @return this Builder
     */
    public Builder withMasterNameAndSentinels(
        final String masterName, final Set<HostAndPort> sentinels) {
      if (masterName == null || "".equals(masterName)) {
        throw new IllegalArgumentException("masterName is null or empty: " + masterName);
      }
      if (sentinels == null || sentinels.isEmpty()) {
        throw new IllegalArgumentException("sentinels is null or empty: " + sentinels);
      }
      this.masterName = masterName;
      this.sentinels = Collections.unmodifiableSet(new LinkedHashSet<HostAndPort>(sentinels));
      this.hostAndPort = null;
      return this;
    }

    /**
     * Configs created by this Builder will have the given Redis namespace to prefix keys with.
     *
     * @param namespace the Redis namespace to prefix keys with
     * @return this Builder
     */
    public Builder withNamespace(final String namespace) {
      if (namespace == null) {
        throw new IllegalArgumentException("namespace must not be null");
      }
      this.namespace = namespace;
      return this;
    }

    /**
     * Configs created by this Builder will have the given Redis connection timeout.
     *
     * @param timeout the Redis connection timeout
     * @return this Builder
     */
    public Builder withTimeout(final int timeout) {
      if (timeout < 0) {
        throw new IllegalArgumentException("timeout must not be negative: " + timeout);
      }
      this.clientBuilder.connectionTimeoutMillis(timeout);
      return this;
    }

    /**
     * Configs created by this Builder will authenticate with the given Redis user.
     *
     * @param user the Redis user
     * @return this Builder
     */
    public Builder withUser(final String user) {
      this.clientBuilder.user(user);
      return this;
    }

    /**
     * Configs created by this Builder will authenticate with the given Redis password.
     *
     * @param password the Redis password
     * @return this Builder
     */
    public Builder withPassword(final String password) {
      this.clientBuilder.password(password);
      return this;
    }

    /**
     * Configs created by this Builder will use the given Redis database.
     *
     * @param database the Redis database to use
     * @return this Builder
     */
    public Builder withDatabase(final int database) {
      if (database < 0) {
        throw new IllegalArgumentException("database must not be negative: " + database);
      }
      this.clientBuilder.database(database);
      return this;
    }

    /**
     * Configs created by this Builder will use the JedisClientConfig.
     *
     * @param clientConfig the JedisClientConfig to use
     * @return this Builder
     */
    public Builder withJedisClientConfig(final JedisClientConfig clientConfig) {
      if (clientConfig == null) {
        throw new IllegalArgumentException("clientConfig must not be null");
      }
      this.clientBuilder.from(clientConfig);
      return this;
    }

    /**
     * @return a new Config initialized with the current values
     */
    public Config build() {
      return new Config(this);
    }
  }
}
