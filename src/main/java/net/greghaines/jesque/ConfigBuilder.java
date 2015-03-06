/*
 * Copyright 2011 Greg Haines
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.greghaines.jesque;

import java.io.Serializable;

/**
 * A fluent-style builder for {@link Config}s.
 *
 * @author Greg Haines
 * @see Config
 */
public class ConfigBuilder implements Serializable {

    private static final long serialVersionUID = 730947307298353317L;

    /** localhost */
    public static final String DEFAULT_HOST = "localhost";
    /** 6379 */
    public static final int DEFAULT_PORT = 6379;
    /** 5 seconds */
    public static final int DEFAULT_TIMEOUT = 5000;
    /** null */
    public static final String DEFAULT_PASSWORD = null;
    /** All resque clients use "resque" by default */
    public static final String DEFAULT_NAMESPACE = "resque";
    /** 0 */
    public static final int DEFAULT_DATABASE = 0;

    /**
     * @return a Config with all the default values set
     */
    public static Config getDefaultConfig() {
        return new ConfigBuilder().build();
    }

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private int timeout = DEFAULT_TIMEOUT;
    private String password = DEFAULT_PASSWORD;
    private String namespace = DEFAULT_NAMESPACE;
    private int database = DEFAULT_DATABASE;

    /**
     * No-arg constructor.
     */
    public ConfigBuilder() {
        // Do nothing
    }

    /**
     * Create a new ConfigBuilder using an existing Config as the starting
     * point.
     *
     * @param startingPoint
     *            the Config instance to copy the values from
     */
    public ConfigBuilder(final Config startingPoint) {
        if (startingPoint == null) {
            throw new IllegalArgumentException("startingPoint must not be null");
        }
        this.host = startingPoint.getHost();
        this.port = startingPoint.getPort();
        this.timeout = startingPoint.getTimeout();
        this.namespace = startingPoint.getNamespace();
        this.database = startingPoint.getDatabase();
    }

    /**
     * Configs created by this ConfigBuilder will have the given Redis hostname.
     *
     * @param host
     *            the Redis hostname
     * @return this ConfigBuilder
     */
    public ConfigBuilder withHost(final String host) {
        if (host == null || "".equals(host)) {
            throw new IllegalArgumentException("host must not be null or empty: " + host);
        }
        this.host = host;
        return this;
    }

    /**
     * Configs created by this ConfigBuilder will have the given Redis port
     * number.
     *
     * @param port
     *            the Redis port number
     * @return this ConfigBuilder
     */
    public ConfigBuilder withPort(final int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("post must be a valid port in the range 1-65535: " + port);
        }
        this.port = port;
        return this;
    }

    /**
     * Configs created by this ConfigBuilder will have the given Redis
     * connection timeout.
     *
     * @param timeout
     *            the Redis connection timeout
     * @return this ConfigBuilder
     */
    public ConfigBuilder withTimeout(final int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        this.timeout = timeout;
        return this;
    }

    /**
     * Configs created by this ConfigBuilder will authenticate with the given
     * Redis password.
     *
     * @param password
     *            the Redis password
     * @return this ConfigBuilder
     */
    public ConfigBuilder withPassword(final String password) {
        this.password = password;
        return this;
    }

    /**
     * Configs created by this ConfigBuilder will have the given Redis namespace
     * to prefix keys with.
     *
     * @param namespace
     *            the Redis namespace to prefix keys with
     * @return this ConfigBuilder
     */
    public ConfigBuilder withNamespace(final String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("namespace must not be null");
        }
        this.namespace = namespace;
        return this;
    }

    /**
     * Configs created by this ConfigBuilder will use the given Redis database.
     *
     * @param database
     *            the Redis database to use
     * @return this ConfigBuilder
     */
    public ConfigBuilder withDatabase(final int database) {
        if (database < 0) {
            throw new IllegalArgumentException("database must not be negative: " + database);
        }
        this.database = database;
        return this;
    }

    /**
     * @return a new Config initialized with the current values
     */
    public Config build() {
        return new Config(this.host, this.port, this.timeout, this.password, this.namespace, this.database);
    }
}
