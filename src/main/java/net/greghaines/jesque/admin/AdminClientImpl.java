package net.greghaines.jesque.admin;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;
import redis.clients.jedis.Jedis;

public class AdminClientImpl extends AbstractAdminClient
{
	public static final boolean DEFAULT_CHECK_CONNECTION_BEFORE_USE = false;
	
	private final Config config;
	private final Jedis jedis;
	private final boolean checkConnectionBeforeUse;
	private final ScheduledExecutorService keepAliveService;
	
	/**
	 * Create a new AdminClientImpl, which creates it's own connection to Redis using values from the config.
	 * It will not verify the connection before use.
	 * 
	 * @param config used to create a connection to Redis
	 */
	public AdminClientImpl(final Config config)
	{
		this(config, DEFAULT_CHECK_CONNECTION_BEFORE_USE);
	}
	
	/**
	 * Create a new AdminClientImpl, which creates it's own connection to Redis using values from the config.
	 * 
	 * @param config used to create a connection to Redis
	 * @param checkConnectionBeforeUse check to make sure the connection is alive before using it
	 * @throws IllegalArgumentException if the config is null
	 */
	public AdminClientImpl(final Config config, final boolean checkConnectionBeforeUse)
	{
		super(config);
		this.config = config;
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		authenticateAndSelectDB();
		this.checkConnectionBeforeUse = checkConnectionBeforeUse;
		this.keepAliveService = null;
	}

	/**
	 * Create a new AdminClientImpl, which creates it's own connection to Redis using values from the config and 
	 * spawns a thread to ensure the connection stays open.
	 * 
	 * @param config used to create a connection to Redis
	 * @param initialDelay the time to delay first connection check
	 * @param period the period between successive connection checks
	 * @param timeUnit the time unit of the initialDelay and period parameters
	 */
	public AdminClientImpl(final Config config, final long initialDelay, final long period, final TimeUnit timeUnit)
	{
		super(config);
		this.config = config;
		this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
		authenticateAndSelectDB();
		this.checkConnectionBeforeUse = false;
		this.keepAliveService = Executors.newSingleThreadScheduledExecutor();
		this.keepAliveService.scheduleAtFixedRate(new Runnable()
		{
			public void run()
			{
				ensureJedisConnection();
			}
		}, initialDelay, period, timeUnit);
	}
	
	@Override
	protected void doPublish(final String queue, final String jobJson)
	{
		ensureJedisConnection();
		doPublish(this.jedis, getNamespace(), queue, jobJson);
	}

	public void end()
	{
		if (this.keepAliveService != null)
		{
			this.keepAliveService.shutdownNow();
		}
		this.jedis.quit();
	}
	
	private void authenticateAndSelectDB()
	{
		if (this.config.getPassword() != null)
	 	{
		 	this.jedis.auth(this.config.getPassword());
	 	}
	 	this.jedis.select(this.config.getDatabase());
	}
	
	private void ensureJedisConnection()
	{
		if (this.checkConnectionBeforeUse && !JedisUtils.ensureJedisConnection(this.jedis))
		{
			authenticateAndSelectDB();
		}
	}
}
