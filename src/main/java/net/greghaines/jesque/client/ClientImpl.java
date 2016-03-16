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
import net.greghaines.jesque.queue.JedisQueueDao;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Basic implementation of the Client interface.
 *
 * @author Greg Haines
 * @author Animesh Kumar
 */
public class ClientImpl extends AbstractClient {

    public static final boolean DEFAULT_CHECK_CONNECTION_BEFORE_USE = false;

    private final ScheduledExecutorService keepAliveService;

    /**
     * Create a new ClientImpl, which creates it's own connection to Redis using
     * values from the config. It will not verify the connection before use.
     *
     * @param config
     *            used to create a connection to Redis
     */
    public ClientImpl(final Config config) {
        this(config, DEFAULT_CHECK_CONNECTION_BEFORE_USE);
    }

    /**
     * Create a new ClientImpl, which creates it's own connection to Redis using
     * values from the config.
     *
     * @param config
     *            used to create a connection to Redis
     * @param checkConnectionBeforeUse
     *            check to make sure the connection is alive before using it
     * @throws IllegalArgumentException
     *             if the config is null
     */
    public ClientImpl(final Config config, final boolean checkConnectionBeforeUse) {
        super(config, new JedisQueueDao(config, checkConnectionBeforeUse));
        this.keepAliveService = null;
    }

    /**
     * Create a new ClientImpl, which creates it's own connection to Redis using
     * values from the config and spawns a thread to ensure the connection stays
     * open.
     *
     * @param config
     *            used to create a connection to Redis
     * @param initialDelay
     *            the time to delay first connection check
     * @param period
     *            the period between successive connection checks
     * @param timeUnit
     *            the time unit of the initialDelay and period parameters
     */
    public ClientImpl(final Config config, final long initialDelay, final long period, final TimeUnit timeUnit) {
        super(config, new JedisQueueDao(config, false));
        this.keepAliveService = Executors.newSingleThreadScheduledExecutor();
        this.keepAliveService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                ((JedisQueueDao) queueDao).ensureJedisConnection();
            }
        }, initialDelay, period, timeUnit);
    }

    @Override
    public void end() {
        if (this.keepAliveService != null) {
            this.keepAliveService.shutdownNow();
        }
        ((JedisQueueDao) this.queueDao).close();
    }
}
