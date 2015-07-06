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
import java.util.Set;

import net.greghaines.jesque.utils.JesqueUtils;

/**
 * An immutable configuration bean for use with the rest of the project.
 *
 * @author Greg Haines
 * @see ConfigBuilder
 */
public class Config implements Serializable {

    private static final long serialVersionUID = -6638770587683679373L;

    private final String host;
    private final int port;
    private final int timeout;
    private final String password;
    private final String namespace;
    private final int database;
    private final Set<String> sentinels;
    private final String masterName;

    /**
     * Using a ConfigBuilder is recommended...
     *
     * @param host      the Reds hostname
     * @param port      the Redis port number
     * @param timeout   the Redis connection timeout
     * @param password  the Redis database password
     * @param namespace the Redis namespace to prefix keys with
     * @param database  the Redis database to use
     * @see ConfigBuilder
     */
    public Config(final String host, final int port, final int timeout, final String password, final String namespace, 
            final int database) {
        if (host == null || "".equals(host)) {
            throw new IllegalArgumentException("host must not be null or empty: " + host);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("post must be a valid port in the range 1-65535: " + port);
        }
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        if (namespace == null) {
            throw new IllegalArgumentException("namespace must not be null");
        }
        if (database < 0) {
            throw new IllegalArgumentException("database must not be negative: " + database);
        }
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.password = password;
        this.namespace = namespace;
        this.database = database;
        this.sentinels = null;
        this.masterName = null;
    }

    /**
     * Using a ConfigBuilder is recommended...
     *
     * @param sentinels  the Redis set of sentinels
     * @param masterName the Redis master name
     * @param timeout    the Redis connection timeout
     * @param password   the Redis database password
     * @param namespace  the Redis namespace to prefix keys with
     * @param database   the Redis database to use
     * @see ConfigBuilder
     */
    public Config(final Set<String> sentinels, final String masterName, final int timeout, final String password, 
            final String namespace, final int database) {
        if (sentinels == null || sentinels.size() < 1) {
            throw new IllegalArgumentException("sentinels must not be null or empty: " + sentinels);
        }
        if (masterName == null || "".equals(masterName)) {
            throw new IllegalArgumentException("master must not be null or empty: " + masterName);
        }
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        if (namespace == null) {
            throw new IllegalArgumentException("namespace must not be null");
        }
        if (database < 0) {
            throw new IllegalArgumentException("database must not be negative: " + database);
        }
        this.sentinels = sentinels;
        this.masterName = masterName;
        this.timeout = timeout;
        this.password = password;
        this.namespace = namespace;
        this.database = database;
        this.host = ConfigBuilder.DEFAULT_HOST;
        this.port = ConfigBuilder.DEFAULT_PORT;
    }

    /**
     * @return the Redis hostname
     */
    public String getHost() {
        return this.host;
    }

    /**
     * @return the Redis port number
     */
    public int getPort() {
        return this.port;
    }

    /**
     * @return the Redis connection timeout
     */
    public int getTimeout() {
        return this.timeout;
    }

    /**
     * @return the Redis password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * @return the Redis namespace to prefix keys with
     */
    public String getNamespace() {
        return this.namespace;
    }

    /**
     * @return the Redis database to use
     */
    public int getDatabase() {
        return this.database;
    }

    /**
     * @return the Redis set of sentinels
     */
    public Set<String> getSentinels() {
        return this.sentinels;
    }

    /**
     * @return the Redis master name
     */
    public String getMasterName() {
        return this.masterName;
    }

    /**
     * @return the Redis protocol URI this Config will connect to
     */
    public String getURI() {
        return "redis://" + this.host + ":" + this.port + "/" + this.database;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "<" + getURI() + " namespace=" + this.namespace + " timeout=" + this.timeout + ">";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.database;
        result = prime * result + (this.host.hashCode());
        result = prime * result + (this.namespace.hashCode());
        result = prime * result + ((this.sentinels == null) ? 0 : this.sentinels.hashCode());
        result = prime * result + ((this.masterName == null) ? 0 : this.masterName.hashCode());
        result = prime * result + this.port;
        result = prime * result + this.timeout;
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        boolean equal = false;
        if (this == obj) {
            equal = true;
        } else if (obj instanceof Config) {
            final Config other = (Config) obj;
            equal = ((this.database == other.database) && (this.port == other.port) && (this.timeout == other.timeout) 
                    && JesqueUtils.nullSafeEquals(this.host, other.host)
                    && JesqueUtils.nullSafeEquals(this.namespace, other.namespace)) 
                    && JesqueUtils.nullSafeEquals(this.sentinels, other.sentinels) 
                    && JesqueUtils.nullSafeEquals(this.masterName, other.masterName);
        }
        return equal;
    }
}
