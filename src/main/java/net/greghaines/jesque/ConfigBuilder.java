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

/**
 * A builder for Configs.
 * 
 * @author Greg Haines
 * @see Config
 */
public class ConfigBuilder
{
	/** localhost */
	public static final String DEFAULT_HOST = "localhost";
	/** 6379 */
	public static final int DEFAULT_PORT = 6379;
	/** 5 seconds */
	public static final int DEFAULT_TIMEOUT = 5000;
	/** All resque clients use "resque" by default */
	public static final String DEFAULT_NAMESPACE = "resque";
	
	public static Config getDefaultConfig()
	{
		return new ConfigBuilder().build();
	}
	
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private int timeout = DEFAULT_TIMEOUT; 
	private String namespace = DEFAULT_NAMESPACE;
	private Integer database = null;
	private String jobPackage = null;
	
	public ConfigBuilder(){}
	
	public ConfigBuilder(final Config startingPoint)
	{
		if (startingPoint == null)
		{
			throw new IllegalArgumentException("startingPoint must not be null");
		}
		this.host = startingPoint.getHost();
		this.port = startingPoint.getPort();
		this.timeout = startingPoint.getTimeout();
		this.namespace = startingPoint.getNamespace();
		this.database = startingPoint.getDatabase();
		this.jobPackage = startingPoint.getJobPackage();
	}
	
	public ConfigBuilder withHost(final String host)
	{
		this.host = host;
		return this;
	}
	
	public ConfigBuilder withPort(final int port)
	{
		this.port = port;
		return this;
	}
	
	public ConfigBuilder withTimeout(final int timeout)
	{
		this.timeout = timeout;
		return this;
	}
	
	public ConfigBuilder withNamespace(final String namespace)
	{
		this.namespace = namespace;
		return this;
	}
	
	public ConfigBuilder withDatabase(final Integer database)
	{
		this.database = database;
		return this;
	}
	
	public ConfigBuilder withJobPackage(final String jobPackage)
	{
		this.jobPackage = jobPackage;
		return this;
	}
	
	public Config build()
	{
		return new Config(this.host, this.port, this.timeout, 
			this.namespace, this.database, this.jobPackage);
	}
}
