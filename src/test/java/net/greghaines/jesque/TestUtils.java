package net.greghaines.jesque;

import redis.clients.jedis.Jedis;

public final class TestUtils
{
	public static Jedis createJedis(final Config config)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		return new Jedis(config.getHost(), config.getPort(), config.getTimeout());
	}

	private TestUtils(){} // Utility class
}
