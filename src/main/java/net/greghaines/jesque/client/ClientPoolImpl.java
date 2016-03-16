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

import net.greghaines.jesque.Config;
import net.greghaines.jesque.queue.JedisPoolQueueDao;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * A Client implementation that gets its connection to Redis from a connection
 * pool.
 * 
 * @author Greg Haines
 * @author Animesh Kumar
 */
public class ClientPoolImpl extends AbstractClient {
    /**
     * Create a ClientPoolImpl.
     * 
     * @param config
     *            used to get the namespace for key creation
     * @param jedisPool
     *            the connection pool
     */
    public ClientPoolImpl(final Config config, final Pool<Jedis> jedisPool) {
        super(config, new JedisPoolQueueDao(config, jedisPool));
    }
}
