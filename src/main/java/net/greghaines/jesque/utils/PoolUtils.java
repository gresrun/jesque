/*
 * Copyright 2011 Greg Haines
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package net.greghaines.jesque.utils;

import net.greghaines.jesque.Config;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisSentinelClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Convenience methods for doing work with pooled Jedis connections.
 *
 * @author Greg Haines
 */
public final class PoolUtils {

  /**
   * @return a ConnectionPoolConfig configured with: maxActive=-1, maxIdle=10, minIdle=1,
   *     testOnBorrow=true, blockWhenExhausted=false
   */
  public static ConnectionPoolConfig getDefaultPoolConfig() {
    final ConnectionPoolConfig cfg = new ConnectionPoolConfig();
    cfg.setMaxTotal(-1); // Infinite
    cfg.setMaxIdle(10);
    cfg.setMinIdle(1);
    cfg.setTestOnBorrow(true);
    cfg.setBlockWhenExhausted(false);
    return cfg;
  }

  /**
   * A simple helper method that creates a pool of connections to Redis using the supplied Config
   * and the default pool config.
   *
   * @param jesqueConfig the config used to create the pooled Jedis connection
   * @return a configured UnifiedJedis, implementation determined by the supplied config
   */
  public static UnifiedJedis createJedisPool(final Config jesqueConfig) {
    return createJedisPool(jesqueConfig, getDefaultPoolConfig());
  }

  /**
   * A simple helper method that creates a pool of connections to Redis using the supplied
   * configurations.
   *
   * @param jesqueConfig the config used to create the pooled Jedis connections
   * @param poolConfig the config used to create the pool
   * @return a configured UnifiedJedis, implementation determined by the supplied config
   */
  public static UnifiedJedis createJedisPool(
      final Config jesqueConfig, final ConnectionPoolConfig poolConfig) {
    if (jesqueConfig == null) {
      throw new IllegalArgumentException("jesqueConfig must not be null");
    }
    if (poolConfig == null) {
      throw new IllegalArgumentException("poolConfig must not be null");
    }
    if (jesqueConfig.getMasterName() != null
        && !"".equals(jesqueConfig.getMasterName())
        && jesqueConfig.getSentinels() != null
        && jesqueConfig.getSentinels().size() > 0) {
      return RedisSentinelClient.builder()
          .masterName(jesqueConfig.getMasterName())
          .sentinels(jesqueConfig.getSentinels())
          .poolConfig(poolConfig)
          .clientConfig(jesqueConfig.getJedisClientConfig())
          .build();
    } else {
      return RedisClient.builder()
          .hostAndPort(jesqueConfig.getHostAndPort())
          .poolConfig(poolConfig)
          .clientConfig(jesqueConfig.getJedisClientConfig())
          .build();
    }
  }

  private PoolUtils() {
    // Utility class
  }
}
