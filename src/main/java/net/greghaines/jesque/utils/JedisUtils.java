package net.greghaines.jesque.utils;

import redis.clients.jedis.Jedis;

public final class JedisUtils
{
	public static final String PONG = "PONG";
	
	/**
	 * Ensure that the given connection is established.
	 * 
	 * @param jedis a connection to Redis
	 * @return true if the supplied connection was already connected
	 */
	public static boolean ensureJedisConnection(final Jedis jedis)
	{
		final boolean jedisOK = testJedisConnection(jedis);
		if (!jedisOK)
		{
			try { jedis.quit(); } catch (Exception e){} // Ignore
			try { jedis.disconnect(); } catch (Exception e){} // Ignore
			jedis.connect();
		}
		return jedisOK;
	}

	/**
	 * Test if a connection is valid.
	 * 
	 * @param jedis a connection to Redis
	 * @return true if the supplied connection is connected
	 */
	public static boolean testJedisConnection(final Jedis jedis)
	{
		boolean jedisOK = false;
		try
		{
			jedisOK = (jedis.isConnected() && PONG.equals(jedis.ping()));
		}
		catch (Exception e)
		{
			jedisOK = false;
		}
		return jedisOK;
	}
	
	private JedisUtils(){}
}
