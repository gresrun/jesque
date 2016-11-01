/*
 * Copyright 2016 Greg Haines
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
package net.greghaines.jesque.admin;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * AdminClientPoolImpl publishes jobs to channels using a connection pool.
 * 
 * @author Greg Haines
 */
public class AdminClientPoolImpl extends AbstractAdminClient {
    
    private final Pool<Jedis> jedisPool;

    /**
     * Create a new AdminClientPoolImpl using the supplied configuration and connection pool.
     * 
     * @param config used to create a connection to Redis
     * @param jedisPool the Redis connection pool
     */
    public AdminClientPoolImpl(final Config config, final Pool<Jedis> jedisPool) {
        super(config);
        if (jedisPool == null) {
            throw new IllegalArgumentException("jedisPool must not be null");
        }
        this.jedisPool = jedisPool;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void doPublish(final String channel, final String jobJson) throws Exception {
        PoolUtils.doWorkInPool(this.jedisPool, new PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) {
                doPublish(jedis, getNamespace(), channel, jobJson);
                return null;
            }
        });
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void end() {
        // Do nothing
    }
}
