package net.greghaines.jesque.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public final class JedisUtils
{
	private static final Logger log = LoggerFactory.getLogger(JedisUtils.class);
	
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

	/**
	 * Attempt to reconnect to Redis.
	 * 
	 * @param jedis the connection to Redis
	 * @param reconAttempts number of times to attempt to reconnect before giving up
	 * @param reconnectSleepTime time in milliseconds to wait between attempts
	 * @return true if reconnection was successful
	 */
	public static boolean reconnect(final Jedis jedis, final int reconAttempts, final long reconnectSleepTime)
	{
		int i = 1;
		do 
		{
			try
			{
				jedis.disconnect();
				try { Thread.sleep(reconnectSleepTime); } catch (Exception e2){}
				jedis.connect();
			}
			catch (JedisConnectionException jce){} // Ignore bad connection attempts
			catch (Exception e3)
			{
				log.error("Unknown Exception while trying to reconnect to Redis", e3);
			}
		} while (++i <= reconAttempts && !testJedisConnection(jedis));
		return testJedisConnection(jedis);
	}
	
	private JedisUtils(){}
}
