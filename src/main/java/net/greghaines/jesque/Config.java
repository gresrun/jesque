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
	private final String namespace;
	private final Integer database;
	private final String jobPackage;
	
	public Config(final String host, final int port, final int timeout,
			final String namespace, final Integer database, final String jobPackage)
	{
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.namespace = namespace;
		this.database = database;
		this.jobPackage = jobPackage;
	}

	public String getHost()
	{
		return this.host;
	}

	public int getPort()
	{
		return this.port;
	}

	public int getTimeout()
	{
		return this.timeout;
	}

	public String getNamespace()
	{
		return this.namespace;
	}

	public Integer getDatabase()
	{
		return this.database;
	}

	public String getJobPackage()
	{
		return this.jobPackage;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.database == null) ? 0 : this.database.hashCode());
		result = prime * result + ((this.host == null) ? 0 : this.host.hashCode());
		result = prime * result + ((this.jobPackage == null) ? 0 : this.jobPackage.hashCode());
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
		if (this.database == null)
		{
			if (other.database != null)
			{
				return false;
			}
		}
		else if (!this.database.equals(other.database))
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
		if (this.jobPackage == null)
		{
			if (other.jobPackage != null)
			{
				return false;
			}
		}
		else if (!this.jobPackage.equals(other.jobPackage))
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
		if (this.port != other.port)
		{
			return false;
		}
		if (this.timeout != other.timeout)
		{
			return false;
		}
		return true;
	}
}
