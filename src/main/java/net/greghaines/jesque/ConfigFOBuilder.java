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
import java.util.ArrayList;
import java.util.Collection;

/**
 * A fluent-style builder for {@link ConfigFO}s.
 * 
 * @author Greg Haines
 * @author Heebyung
 * @see ConfigFO
 */
public class ConfigFOBuilder implements Serializable {
    
    private static final long serialVersionUID = -4815539606528776604L;
    
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
    /** 10 */
    public static final int DEFAULT_EMPTY_FAILOVER_QUEUE_SLEEP_TIME_SEC = 10;
    
    /**
     * @return a ConfigFO with all the default values set
     */
    public static ConfigFO getDefaultConfig() {
        return new ConfigFOBuilder().build();
    }

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private int timeout = DEFAULT_TIMEOUT;
    private String password = DEFAULT_PASSWORD;
    private String namespace = DEFAULT_NAMESPACE;
    private int database = DEFAULT_DATABASE;

    private int emptyFailoverQueueSleepTimeSec = DEFAULT_EMPTY_FAILOVER_QUEUE_SLEEP_TIME_SEC;
    private Collection<String> failoverQueues;
    /**
     * No-arg constructor.
     */
    public ConfigFOBuilder() {
        failoverQueues = new ArrayList<String>();
        failoverQueues.add(ConfigBuilder.DEFAULT_FAILOVER_QUEUE);        
    }

    /**
     * Create a new ConfigBuilder using an existing Config as the starting
     * point.
     * 
     * @param startingPoint
     *            the Config instance to copy the values from
     */
    public ConfigFOBuilder(final Config startingPoint) {
        this();
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
     * Create a new ConfigBuilder using an existing Config as the starting
     * point.
     * 
     * @param startingPoint
     *            the Config instance to copy the values from
     */
    public ConfigFOBuilder(final ConfigFO startingPoint) {
        this((Config)startingPoint);
        if (startingPoint == null) {
            throw new IllegalArgumentException("startingPoint must not be null");
        }
        this.emptyFailoverQueueSleepTimeSec = startingPoint.getEmptyFailoverQueueSleepTimeSec();
        this.failoverQueues = startingPoint.getFailoverQueues();
    }

    /**
     * Configs created by this ConfigBuilder will have the given Redis hostname.
     * 
     * @param host
     *            the Redis hostname
     * @return this ConfigBuilder
     */
    public ConfigFOBuilder withHost(final String host) {
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
    public ConfigFOBuilder withPort(final int port) {
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
    public ConfigFOBuilder withTimeout(final int timeout) {
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
    public ConfigFOBuilder withPassword(final String password) {
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
    public ConfigFOBuilder withNamespace(final String namespace) {
        if (namespace == null || "".equals(namespace)) {
            throw new IllegalArgumentException("namespace must not be null or empty: " + namespace);
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
    public ConfigFOBuilder withDatabase(final int database) {
        if (database < 0) {
            throw new IllegalArgumentException("database must not be negative: " + database);
        }
        this.database = database;
        return this;
    }    
    
    /**
     * Configs created by this ConfigBuilder will have the given emptyFailoverQueueSleepTimeMs
     * 
     * @param emptyFailoverQueueSleepTimeSec
     *            emptyFailoverQueueSleepTimeSec
     * @return this ConfigBuilder
     */
    public ConfigFOBuilder withEmptyFailoverQueueSleepTimeSec(final int emptyFailoverQueueSleepTimeSec) {
        if (emptyFailoverQueueSleepTimeSec < 1) {
            throw new IllegalArgumentException(""
                    + "EmptyFailoverQueueSleepTimeSec must not be negative or zero: "
                    + emptyFailoverQueueSleepTimeSec);
        }
        this.emptyFailoverQueueSleepTimeSec = emptyFailoverQueueSleepTimeSec;
        return this;
    }
    
    /**
     * Configs created by this ConfigBuilder will have the given emptyFailoverQueueSleepTimeMs
     * 
     * @param failoverQueues
     *            failoverQueues
     * @return this ConfigBuilder
     */
    public ConfigFOBuilder withFailoverQueues(Collection<String> failoverQueues) {
        if (failoverQueues == null || failoverQueues.isEmpty()) {
            throw new IllegalArgumentException(""
                    + "failoverQueues must not be null or empty: "
                    + failoverQueues);
        }
        this.failoverQueues = failoverQueues;
        return this;
    }
    
    /**
     * @return a new Config initialized with the current values
     */
    public ConfigFO build() {
        return new ConfigFO(this.host, this.port, this.timeout, this.password, this.namespace, this.database,
                this.emptyFailoverQueueSleepTimeSec, this.failoverQueues);
    }
}
