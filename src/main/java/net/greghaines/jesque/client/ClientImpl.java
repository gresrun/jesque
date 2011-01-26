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

import java.io.IOException;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Basic implementation of the Client interface.
 * 
 * @author Greg Haines
 */
public class ClientImpl implements Client
{
	private static final Logger log = LoggerFactory.getLogger(ClientImpl.class);
	
	private final Jedis jedis;
	private final String namespace;
	
	/**
	 * Create a new ClientImpl, which creates it's own connection to Redis using values from the config.
	 * 
	 * @param config used to create a connection to Redis
	 * @throws IllegalArgumentException if the config is null
	 */
	public ClientImpl(final Config config)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		this.namespace = config.getNamespace();
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getDatabase() != null)
		{
			this.jedis.select(config.getDatabase());
		}
	}
	
	public void enqueue(final String queue, final Job job)
	{
		if (queue == null || "".equals(queue))
		{
			throw new IllegalArgumentException("queue must not be null or empty: " + queue);
		}
		if (job == null || "".equals(job))
		{
			throw new IllegalArgumentException("job must not be null");
		}
		try
		{
			final String msg = ObjectMapperFactory.get().writeValueAsString(job);
			log.debug("enqueue msg={}", msg);
			this.jedis.sadd(key("queues"), queue);
			this.jedis.rpush(key("queue", queue), msg);
		}
		catch (IOException ioe)
		{
			throw new RuntimeException(ioe);
		}
	}
	
	public void end()
	{
		this.jedis.quit();
	}

	/**
	 * Builds a namespaced Redis key with the given arguments.
	 * 
	 * @param parts the key parts to be joined
	 * @return an assembled String key
	 */
	private String key(final String... parts)
	{
		return JesqueUtils.createKey(this.namespace, parts);
	}
}
