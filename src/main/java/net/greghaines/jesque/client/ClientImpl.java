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
package net.greghaines.jesque.client;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;

import redis.clients.jedis.Jedis;

/**
 * Basic implementation of the Client interface.
 * 
 * @author Greg Haines
 */
public class ClientImpl extends AbstractClient
{	
	private final Jedis jedis;
	
	/**
	 * Create a new ClientImpl, which creates it's own connection to Redis using values from the config.
	 * 
	 * @param config used to create a connection to Redis
	 * @throws IllegalArgumentException if the config is null
	 */
	public ClientImpl(final Config config)
	{
		super(config);
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getPassword() != null)
		{
			this.jedis.auth(config.getPassword());
		}
		this.jedis.select(config.getDatabase());
	}
	
	@Override
	protected void doEnqueue(final String queue, final String jobJson)
	{
		JedisUtils.ensureJedisConnection(this.jedis);
		doEnqueue(this.jedis, getNamespace(), queue, jobJson);
	}
	
	public void end()
	{
		this.jedis.quit();
	}
}
