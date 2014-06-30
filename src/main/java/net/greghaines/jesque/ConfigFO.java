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
import java.util.Collection;

import net.greghaines.jesque.utils.JesqueUtils;

/**
 * An immutable configuration bean for use with the failover modules.
 * 
 * @author Greg Haines
 * @author Heebyung
 * @see ConfigFOBuilder
 */
public class ConfigFO extends Config implements Serializable {
    
    private static final long serialVersionUID = 832559337030832779L;
    
    private int emptyFailoverQueueSleepTimeSec;
    private Collection<String> failoverQueues;

    public ConfigFO(final String host, final int port, final int timeout, final String password, final String namespace, final int database,
            final int emptyFailoverQueueSleepTimeSec, final Collection<String> failoverQueues) {
        super(host, port, timeout, password, namespace, database);
        if (emptyFailoverQueueSleepTimeSec < 1) {
            throw new IllegalArgumentException(
                    "emptyFailoverQueueSleepTimeMs must not be negative or zero: "
                            + emptyFailoverQueueSleepTimeSec);
        }
        this.emptyFailoverQueueSleepTimeSec = emptyFailoverQueueSleepTimeSec;
        this.failoverQueues = failoverQueues;
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

    public int getEmptyFailoverQueueSleepTimeSec() {
        return emptyFailoverQueueSleepTimeSec;
    }
    
    public Collection<String> getFailoverQueues() {
        return failoverQueues;
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
        result = prime * result + this.emptyFailoverQueueSleepTimeSec;
        result = prime * result + ((this.failoverQueues == null) ? 0 : this.failoverQueues.hashCode());
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
        } else if (obj instanceof ConfigFO) {
            final ConfigFO other = (ConfigFO) obj;
            equal = ((this.database == other.database)
                    && (this.port == other.port)
                    && (this.timeout == other.timeout)
                    && JesqueUtils.nullSafeEquals(this.host, other.host)
                    && JesqueUtils.nullSafeEquals(this.namespace, other.namespace))
                    && (this.emptyFailoverQueueSleepTimeSec == other.emptyFailoverQueueSleepTimeSec)
                    && JesqueUtils.nullSafeEquals(this.failoverQueues, other.failoverQueues);
        }
        return equal;
    }
}
