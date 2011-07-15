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
 * An immutable configuration bean for use with the rest of the project.
 * 
 * @author Greg Haines
 * @see ConfigBuilder
 */
public class Config implements Serializable
{
	private static final long serialVersionUID = -6638770587683679373L;
	
	private final String host;
	private final int port;
	private final int timeout;
	private final String password;
	private final String namespace;
	private final int database;

	/**
	 * Using a ConfigBuilder is recommended...
	 * 
	 * @param host the Reds hostname
	 * @param port the Redis port number
	 * @param timeout the Redis connection timeout
	 * @param namespace the Redis namespace to prefix keys with
	 * @param database the Redis database to use
	 * or empty string if the full class names will be sent in the jobs
	 * @see ConfigBuilder
	 */
	public Config(final String host, final int port, final int timeout, final String password, 
			final String namespace, final int database)
	{
		if (host == null || "".equals(host))
		{
			throw new IllegalArgumentException("host must not be null or empty: " + host);
		}
		if (port < 1 || port > 65535)
		{
			throw new IllegalArgumentException("post must be a valid port in the range 1-65535: " + port);
		}
		if (timeout < 0)
		{
			throw new IllegalArgumentException("timeout must not be negative: " + timeout);
		}
		if (namespace == null || "".equals(namespace))
		{
			throw new IllegalArgumentException("namespace must not be null or empty: " + namespace);
		}
		if (database < 0)
		{
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
	public String getHost()
	{
		return this.host;
	}

	/**
	 * @return the Redis port number
	 */
	public int getPort()
	{
		return this.port;
	}

	/**
	 * @return the Redis connection timeout
	 */
	public int getTimeout()
	{
		return this.timeout;
	}

	/**
	 * @return the Redis password
	 */
	public String getPassword()
	{
		return this.password;
	}

	/**
	 * @return the Redis namespace to prefix keys with
	 */
	public String getNamespace()
	{
		return this.namespace;
	}

	/**
	 * @return the Redis database to use
	 */
	public int getDatabase()
	{
		return this.database;
	}

	/**
	 * @return the Redis protocol URI this Config will connect to
	 */
	public String getURI()
	{
		return "redis://" + this.host + ":" + this.port + "/" + this.database;
	}

	@Override
	public String toString()
	{
		return "<" + getURI() + " namespace=" + this.namespace + 
			" timeout=" + this.timeout + ">";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + this.database;
		result = prime * result + ((this.host == null) ? 0 : this.host.hashCode());
		result = prime * result + ((this.namespace == null) ? 0 : this.namespace.hashCode());
		result = prime * result + this.port;
		result = prime * result + this.timeout;
		return result;
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final Config other = (Config) obj;
		if (this.database != other.database)
		{
			return false;
		}
		if (this.port != other.port)
		{
			return false;
		}
		if (this.timeout != other.timeout)
		{
			return false;
		}
		if (this.host == null)
		{
			if (other.host != null)
			{
				return false;
			}
		}
		else if (!this.host.equals(other.host))
		{
			return false;
		}
		if (this.namespace == null)
		{
			if (other.namespace != null)
			{
				return false;
			}
		}
		else if (!this.namespace.equals(other.namespace))
		{
			return false;
		}
		return true;
	}
}
