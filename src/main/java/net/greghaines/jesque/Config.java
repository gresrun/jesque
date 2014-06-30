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

import net.greghaines.jesque.utils.JesqueUtils;

/**
 * An immutable configuration bean for use with the rest of the project.
 * 
 * @author Greg Haines
 * @see ConfigBuilder
 */
public class Config implements Serializable {
    
    private static final long serialVersionUID = -6638770587683679373L;

    protected final String host;
    protected final int port;
    protected final int timeout;
    protected final String password;
    protected final String namespace;
    protected final int database;
    
    private boolean failoverEnabled;
    private int heartbeatTimeoutPeriodSec;
    private int heartbeatTransmissionPeriodSec;
    private String failoverQueueName;
    private boolean isFailoverDataKeyContainsUUID;
    
    /**
     * Using a ConfigBuilder is recommended...
     * 
     * @param host
     *            the Reds hostname
     * @param port
     *            the Redis port number
     * @param timeout
     *            the Redis connection timeout
     * @param namespace
     *            the Redis namespace to prefix keys with
     * @param database
     *            the Redis database to use
     * @param failoverEnabled
     *            failoverEnabled
     * @param heartbeatTimeoutPeriodSec
     *            heartbeatTimeoutPeriodSec
     * @param heartbeatTransmissionPeriodSec
     *            heartbeatTransmissionPeriodSec
     * @param failoverQueueName
     *            failoverQueueName
     * @param isFailoverDataKeyContainsUUID
     *            isFailoverDataKeyContainsUUID
     * @see ConfigBuilder
     */
    public Config(final String host, final int port, final int timeout,
            final String password, final String namespace, final int database,
            final boolean failoverEnabled, final int heartbeatTimeoutPeriodSec,
            final int heartbeatTransmissionPeriodSec, final String failoverQueueName,
            final boolean isFailoverDataKeyContainsUUID) {
        this(host, port, timeout, password, namespace, database);
        if (heartbeatTimeoutPeriodSec < 1) {
            throw new IllegalArgumentException("heartbeatTimeoutPeriodSec must not be negative or zero: "
                    + heartbeatTimeoutPeriodSec);
        }
        if (heartbeatTimeoutPeriodSec <= heartbeatTransmissionPeriodSec) {
            throw new IllegalArgumentException(
                    "heartbeatTimeoutPeriodSec must greater than heartbeatTransmissionPeriodSec: "
                            + heartbeatTransmissionPeriodSec);
        }
        if (failoverQueueName == null || "".equals(failoverQueueName)) {
            throw new IllegalArgumentException(
                    "failoverQueueName must not be null or empty: " + failoverQueueName);
        }
        this.failoverEnabled = failoverEnabled;
        this.heartbeatTimeoutPeriodSec = heartbeatTimeoutPeriodSec;
        this.heartbeatTransmissionPeriodSec = heartbeatTransmissionPeriodSec;
        this.failoverQueueName = failoverQueueName;
        this.isFailoverDataKeyContainsUUID = isFailoverDataKeyContainsUUID;
    }
    /**
     * Using a ConfigBuilder is recommended...
     * 
     * @param host
     *            the Reds hostname
     * @param port
     *            the Redis port number
     * @param timeout
     *            the Redis connection timeout
     * @param namespace
     *            the Redis namespace to prefix keys with
     * @param database
     *            the Redis database to use
     * @see ConfigBuilder
     */
    public Config(final String host, final int port, final int timeout, final String password, final String namespace, final int database) {
        if (host == null || "".equals(host)) {
            throw new IllegalArgumentException("host must not be null or empty: " + host);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("post must be a valid port in the range 1-65535: " + port);
        }
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        if (namespace == null || "".equals(namespace)) {
            throw new IllegalArgumentException("namespace must not be null or empty: " + namespace);
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
     * @return the Redis protocol URI this Config will connect to
     */
    public String getURI() {
        return "redis://" + this.host + ":" + this.port + "/" + this.database;
    }

    /**
     * @return true if failover is enabled
     */
    public boolean isFailoverEnabled() {
        return failoverEnabled;
    }
    
    /**
     * @return heartbeat timeout value in seconds for failover detection
     */
    public int getHeartbeatTimeoutPeriodSec() {
        return heartbeatTimeoutPeriodSec;
    }
    
    /**
     * @return heartebeat interval in seconds for failover detection
     */
    public int getHeartbeatTransmissionPeriodSec() {
        return heartbeatTransmissionPeriodSec;
    }
    
    /**
     * @return the queue contains heatbeatTimeout-value and key of the redundant data pair for failover
     */
    public String getFailoverQueueName() {
        return failoverQueueName;
    }    

    /**
     * @return true if key of redundant data contains uuid to guarantee uniqueness
     */
    public boolean isFailoverDataKeyContainsUUID() {
        return isFailoverDataKeyContainsUUID;
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
        result = prime * result + ((this.host == null) ? 0 : this.host.hashCode());
        result = prime * result + ((this.namespace == null) ? 0 : this.namespace.hashCode());
        result = prime * result + this.port;
        result = prime * result + this.timeout;
        
        result = prime * result + Boolean.toString(failoverEnabled).hashCode();
        result = prime * result + this.heartbeatTimeoutPeriodSec;
        result = prime * result + this.heartbeatTransmissionPeriodSec;
        result = prime * result + ((this.failoverQueueName == null) ? 0 : this.failoverQueueName.hashCode());
        result = prime * result + Boolean.toString(isFailoverDataKeyContainsUUID).hashCode();
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
            equal = ((this.database == other.database)
                    && (this.port == other.port)
                    && (this.timeout == other.timeout)
                    && JesqueUtils.nullSafeEquals(this.host, other.host)
                    && JesqueUtils.nullSafeEquals(this.namespace, other.namespace))
                    && (this.failoverEnabled == other.failoverEnabled)
                    && (this.heartbeatTimeoutPeriodSec == other.heartbeatTimeoutPeriodSec)
                    && (this.heartbeatTransmissionPeriodSec == other.heartbeatTransmissionPeriodSec)
                    && JesqueUtils.nullSafeEquals(this.failoverQueueName, other.failoverQueueName)
                    && (this.isFailoverDataKeyContainsUUID == other.isFailoverDataKeyContainsUUID);
        }
        return equal;
    }
}
