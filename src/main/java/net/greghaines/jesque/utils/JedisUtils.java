package net.greghaines.jesque.utils;

import redis.clients.jedis.Jedis;

public final class JedisUtils
{
	public static final String PONG = "PONG";
	
	public static void ensureJedisConnection(final Jedis jedis)
	{
		if (!testJedisConnection(jedis))
		{
			try { jedis.quit(); } catch (Exception e){} // Ignore
			try { jedis.disconnect(); } catch (Exception e){} // Ignore
			jedis.connect();
		}
	}

	public static boolean testJedisConnection(final Jedis jedis)
	{
		boolean jedisOK = false;
		try
		{
			jedisOK = (jedis.isConnected() && jedis.ping().equals(PONG));
		}
		catch (Exception e)
		{
			jedisOK = false;
		}
		return jedisOK;
	}
	
	private JedisUtils(){}
}
