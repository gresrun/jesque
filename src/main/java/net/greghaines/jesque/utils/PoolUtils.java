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

import net.greghaines.jesque.Config;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;

/**
 * Convenience methods for doing work with pooled resources.
 * 
 * @author Greg Haines
 */
public final class PoolUtils {

    /**
     * Perform the given work with a resource from the given pool.
     * 
     * @param pool
     *            the resource pool
     * @param work
     *            the work to perform
     * @param <T>
     *            the resource type
     * @param <V>
     *            the result type
     * @return the result of the given work
     * @throws Exception
     *             if something went wrong
     */
    public static <T, V> V doWorkInPool(final Pool<T> pool, final PoolWork<T, V> work) throws Exception {
        if (pool == null) {
            throw new IllegalArgumentException("pool must not be null");
        }
        if (work == null) {
            throw new IllegalArgumentException("work must not be null");
        }
        final V result;
        final T poolResource = pool.getResource();
        try {
            result = work.doWork(poolResource);
        } finally {
            pool.returnResource(poolResource);
        }
        return result;
    }

    /**
     * Perform the given work with a resource from the given pool. Wraps any
     * thrown checked exceptions in a RuntimeException.
     * 
     * @param pool
     *            the resource pool
     * @param work
     *            the work to perform
     * @param <T>
     *            the resource type
     * @param <V>
     *            the result type
     * @return the result of the given work
     */
    public static <T, V> V doWorkInPoolNicely(final Pool<T> pool, final PoolWork<T, V> work) {
        final V result;
        try {
            result = doWorkInPool(pool, work);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * @return a GenericObjectPool.Config configured with: maxActive=-1,
     *         maxIdle=10, minIdle=1, testOnBorrow=true,
     *         whenExhaustedAction=GROW
     */
    public static GenericObjectPool.Config getDefaultPoolConfig() {
        final GenericObjectPool.Config cfg = new GenericObjectPool.Config();
        cfg.maxActive = -1; // Infinite
        cfg.maxIdle = 10;
        cfg.minIdle = 1;
        cfg.testOnBorrow = true;
        cfg.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
        return cfg;
    }

    /**
     * A simple helper method that creates a pool of connections to Redis using
     * the supplied Config and the default pool config.
     * 
     * @param jesqueConfig
     *            the config used to create the pooled Jedis connection
     * @return a configured Pool of Jedis connections
     */
    public static Pool<Jedis> createJedisPool(final Config jesqueConfig) {
        return createJedisPool(jesqueConfig, getDefaultPoolConfig());
    }

    /**
     * A simple helper method that creates a pool of connections to Redis using
     * the supplied configurations.
     * 
     * @param jesqueConfig
     *            the config used to create the pooled Jedis connections
     * @param poolConfig
     *            the config used to create the pool
     * @return a configured Pool of Jedis connections
     */
    public static Pool<Jedis> createJedisPool(final Config jesqueConfig, final GenericObjectPool.Config poolConfig) {
        if (jesqueConfig == null) {
            throw new IllegalArgumentException("jesqueConfig must not be null");
        }
        if (poolConfig == null) {
            throw new IllegalArgumentException("poolConfig must not be null");
        }
        return new JedisPool(poolConfig, jesqueConfig.getHost(), jesqueConfig.getPort(), jesqueConfig.getTimeout(), jesqueConfig.getPassword());
    }

    /**
     * A unit of work that utilizes a pooled resource.
     * 
     * @author Greg Haines
     * 
     * @param <T>
     *            the kind of pooled resource used
     * @param <V>
     *            the kind of result returned
     */
    public interface PoolWork<T, V> {
        /**
         * Do work with a pooled resource and return a result.
         * 
         * @param poolResource
         *            the pooled resource
         * @return the result of the work done
         * @throws Exception
         *             in case something goes wrong
         */
        V doWork(T poolResource) throws Exception;
    }

    private PoolUtils() {
        // Utility class
    }
}
