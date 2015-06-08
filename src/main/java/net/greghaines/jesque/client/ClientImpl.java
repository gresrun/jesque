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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;

import redis.clients.jedis.Jedis;

/**
 * Basic implementation of the Client interface.
 *
 * @author Greg Haines
 * @author Animesh Kumar
 */
public class ClientImpl extends AbstractClient {

    public static final boolean DEFAULT_CHECK_CONNECTION_BEFORE_USE = false;

    private final Config config;
    private final Jedis jedis;
    private final boolean checkConnectionBeforeUse;
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
        super(config);
        this.config = config;
        this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        authenticateAndSelectDB();
        this.checkConnectionBeforeUse = checkConnectionBeforeUse;
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
        super(config);
        this.config = config;
        this.jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        authenticateAndSelectDB();
        this.checkConnectionBeforeUse = false;
        this.keepAliveService = Executors.newSingleThreadScheduledExecutor();
        this.keepAliveService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (!JedisUtils.ensureJedisConnection(jedis)) {
                    authenticateAndSelectDB();
                }
            }
        }, initialDelay, period, timeUnit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doEnqueue(final String queue, final String jobJson) {
        ensureJedisConnection();
        doEnqueue(this.jedis, getNamespace(), queue, jobJson);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doPriorityEnqueue(final String queue, final String jobJson) {
        ensureJedisConnection();
        doPriorityEnqueue(this.jedis, getNamespace(), queue, jobJson);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doAcquireLock(final String lockName, final String lockHolder, final int timeout) throws Exception {
        ensureJedisConnection();
        return doAcquireLock(this.jedis, getNamespace(), lockName, lockHolder, timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end() {
        ensureJedisConnection();
        if (this.keepAliveService != null) {
            this.keepAliveService.shutdownNow();
        }
        this.jedis.quit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doDelayedEnqueue(final String queue, final String msg, final long future) throws Exception {
        ensureJedisConnection();
        doDelayedEnqueue(this.jedis, getNamespace(), queue, msg, future);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doRemoveDelayedEnqueue(final String queue, final String msg) throws Exception {
        ensureJedisConnection();
        doRemoveDelayedEnqueue(this.jedis, getNamespace(), queue, msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doRecurringEnqueue(final String queue, final String msg, final long future, final long frequency) throws Exception{
        ensureJedisConnection();
        doRecurringEnqueue(this.jedis, getNamespace(), queue, msg, future, frequency);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doRemoveRecurringEnqueue(final String queue, final String msg) throws Exception{
        ensureJedisConnection();
        doRemoveRecurringEnqueue(this.jedis, getNamespace(), queue, msg);
    }

    private void authenticateAndSelectDB() {
        if (this.config.getPassword() != null) {
            this.jedis.auth(this.config.getPassword());
        }
        this.jedis.select(this.config.getDatabase());
    }

    private void ensureJedisConnection() {
        if (this.checkConnectionBeforeUse && !JedisUtils.ensureJedisConnection(this.jedis)) {
            authenticateAndSelectDB();
        }
    }
}
