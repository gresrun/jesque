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
package net.greghaines.jesque.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * A collection of utilities for Redis connections.
 * 
 * @author Greg Haines
 * @author Animesh Kumar
 */
public final class JedisUtils {
    
    public static final String PONG = "PONG";

    private static final Logger LOG = LoggerFactory.getLogger(JedisUtils.class);
    private static final String LIST = "list";
    private static final String ZSET = "zset";
    private static final String HASH = "hash";
    private static final String NONE = "none";

    /**
     * Ensure that the given connection is established.
     * 
     * @param jedis
     *            a connection to Redis
     * @return true if the supplied connection was already connected
     */
    public static boolean ensureJedisConnection(final Jedis jedis) {
        final boolean jedisOK = testJedisConnection(jedis);
        if (!jedisOK) {
            try {
                jedis.quit();
            } catch (Exception e) {
            } // Ignore
            try {
                jedis.disconnect();
            } catch (Exception e) {
            } // Ignore
            jedis.connect();
        }
        return jedisOK;
    }

    /**
     * Test if a connection is valid.
     * 
     * @param jedis
     *            a connection to Redis
     * @return true if the supplied connection is connected
     */
    public static boolean testJedisConnection(final Jedis jedis) {
        boolean jedisOK = false;
        try {
            jedisOK = (jedis.isConnected() && PONG.equals(jedis.ping()));
        } catch (Exception e) {
            jedisOK = false;
        }
        return jedisOK;
    }

    /**
     * Attempt to reconnect to Redis.
     * 
     * @param jedis
     *            the connection to Redis
     * @param reconAttempts
     *            number of times to attempt to reconnect before giving up
     * @param reconnectSleepTime
     *            time in milliseconds to wait between attempts
     * @return true if reconnection was successful
     */
    public static boolean reconnect(final Jedis jedis, final int reconAttempts, final long reconnectSleepTime) {
        int i = 1;
        do {
            try {
                jedis.disconnect();
                jedis.getClient().resetPipelinedCount();
                try {
                    Thread.sleep(reconnectSleepTime);
                } catch (Exception e2) {
                }
                jedis.connect();
            } catch (JedisConnectionException jce) {
            } // Ignore bad connection attempts
            catch (Exception e3) {
                LOG.error("Unknown Exception while trying to reconnect to Redis", e3);
            }
        } while (++i <= reconAttempts && !testJedisConnection(jedis));
        return testJedisConnection(jedis);
    }

    /**
     * Determines if the queue identified by the given key is a regular queue.
     * 
     * @param jedis
     *            connection to Redis
     * @param key
     *            the key that identifies a queue
     * @return true if the key identifies a regular queue, false otherwise
     */
    public static boolean isRegularQueue(final Jedis jedis, final String key) {
        return LIST.equalsIgnoreCase(jedis.type(key));
    }

    /**
     * Determines if the queue identified by the given key is a delayed queue.
     * 
     * @param jedis
     *            connection to Redis
     * @param key
     *            the key that identifies a queue
     * @return true if the key identifies a delayed queue, false otherwise
     */
    public static boolean isDelayedQueue(final Jedis jedis, final String key) {
        return ZSET.equalsIgnoreCase(jedis.type(key));
    }

    public static boolean isRecurringQueue(final Jedis jedis, final String queueKey, final String hashKey) {
        final String hashType = jedis.type(hashKey);
        return (isDelayedQueue(jedis, queueKey) && HASH.equalsIgnoreCase(hashType));
    }

    /**
     * Determines if the queue identified by the given key is used.
     * 
     * @param jedis
     *            connection to Redis
     * @param key
     *            the key that identifies a queue
     * @return true if the key is used, false otherwise
     */
    public static boolean isKeyUsed(final Jedis jedis, final String key) {
        return !NONE.equalsIgnoreCase(jedis.type(key));
    }

    /**
     * Determines if the queue identified by the given key can be used as a delayed queue.
     * 
     * @param jedis
     *            connection to Redis
     * @param key
     *            the key that identifies a queue
     * @return true if the key already is a delayed queue or is not currently used, false otherwise
     */
    public static boolean canUseAsDelayedQueue(final Jedis jedis, final String key) {
        final String type = jedis.type(key);
        return (ZSET.equalsIgnoreCase(type) || NONE.equalsIgnoreCase(type));
    }

    public static boolean canUseAsRecurringQueue(final Jedis jedis, final String queueKey, final String hashKey)
    {
        final String hashType = jedis.type(hashKey);
        return (canUseAsDelayedQueue(jedis, queueKey) && (HASH.equalsIgnoreCase(hashType) || NONE.equalsIgnoreCase(hashType)));
    }

    private JedisUtils() {
        // Utility class
    }
}
