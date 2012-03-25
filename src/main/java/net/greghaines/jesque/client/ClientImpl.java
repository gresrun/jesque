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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
	public static final boolean DEFAULT_CHECK_CONNECTION_BEFORE_USE = false;
	
	private final Jedis jedis;
	private final boolean checkConnectionBeforeUse;
	private final ScheduledExecutorService keepAliveService;

	/**
	 * Create a new ClientImpl, which creates it's own connection to Redis using values from the config.
	 * It will not verify the connection before use.
	 * 
	 * @param config used to create a connection to Redis
	 */
	public ClientImpl(final Config config)
	{
		this(config, DEFAULT_CHECK_CONNECTION_BEFORE_USE);
	}
	
	/**
	 * Create a new ClientImpl, which creates it's own connection to Redis using values from the config.
	 * 
	 * @param config used to create a connection to Redis
	 * @param checkConnectionBeforeUse check to make sure the connection is alive before using it
	 * @throws IllegalArgumentException if the config is null
	 */
	public ClientImpl(final Config config, final boolean checkConnectionBeforeUse)
	{
		super(config);
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getPassword() != null)
		{
			this.jedis.auth(config.getPassword());
		}
		this.jedis.select(config.getDatabase());
		this.checkConnectionBeforeUse = checkConnectionBeforeUse;
		this.keepAliveService = null;
	}

	/**
	 * Create a new ClientImpl, which creates it's own connection to Redis using values from the config and 
	 * spawns a thread to ensure the connection stays open.
	 * 
	 * @param config used to create a connection to Redis
	 * @param initialDelay the time to delay first connection check
	 * @param period the period between successive connection checks
	 * @param timeUnit the time unit of the initialDelay and period parameters
	 */
	public ClientImpl(final Config config, final long initialDelay, final long period, final TimeUnit timeUnit)
	{
		super(config);
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		if (config.getPassword() != null)
		{
			this.jedis.auth(config.getPassword());
		}
		this.jedis.select(config.getDatabase());
		this.checkConnectionBeforeUse = false;
		this.keepAliveService = Executors.newSingleThreadScheduledExecutor();
		this.keepAliveService.scheduleAtFixedRate(new Runnable()
		{
			public void run()
			{
				JedisUtils.ensureJedisConnection(ClientImpl.this.jedis);
			}
		}, initialDelay, period, timeUnit);
	}
	
	@Override
	protected void doEnqueue(final String queue, final String jobJson)
	{
		if (this.checkConnectionBeforeUse)
		{
			JedisUtils.ensureJedisConnection(this.jedis);
		}
		doEnqueue(this.jedis, getNamespace(), queue, jobJson);
	}
	
	@Override
	protected void doHeadQueue(final String queue, final String jobJson)
	{
		if (this.checkConnectionBeforeUse)
		{
			JedisUtils.ensureJedisConnection(this.jedis);
		}
		doHeadQueue(this.jedis, getNamespace(), queue, jobJson);
	}

	
	@Override
    protected boolean doAcquireLock(String lockName, String lockHolder, Integer timeout) throws Exception {
		if (this.checkConnectionBeforeUse)
		{
			JedisUtils.ensureJedisConnection(this.jedis);
		}
		return doAcquireLock(this.jedis, getNamespace(), lockName, lockHolder, timeout);
    }

    public void end()
	{
		if (this.keepAliveService != null)
		{
			this.keepAliveService.shutdownNow();
		}
		this.jedis.quit();
	}
}
