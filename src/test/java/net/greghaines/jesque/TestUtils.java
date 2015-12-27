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
package net.greghaines.jesque;

import java.util.List;

import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.worker.JobExecutor;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Test helpers.
 * 
 * @author Greg Haines
 * @author Animesh Kumar
 */
public final class TestUtils {
    
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Reset the Redis database using the supplied Config.
     * 
     * @param config
     *            the location of the Redis server
     */
    public static void resetRedis(final Config config) {
        final Jedis jedis = createJedis(config);
        try {
            log.info("Resetting Redis for next test...");
            jedis.flushDB();
        } finally {
            jedis.quit();
        }
    }

    /**
     * Create a connection to Redis from the given Config.
     * 
     * @param config
     *            the location of the Redis server
     * @return a new connection
     */
    public static Jedis createJedis(final Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        final Jedis jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        if (config.getPassword() != null) {
            jedis.auth(config.getPassword());
        }
        jedis.select(config.getDatabase());
        return jedis;
    }

    public static void enqueueJobs(final String queue, final List<Job> jobs, final Config config) {
        final Client client = new ClientImpl(config);
        try {
            for (final Job job : jobs) {
                client.enqueue(queue, job);
            }
        } finally {
            client.end();
        }
    }

    public static void stopWorker(final JobExecutor worker, final Thread workerThread) {
        stopWorker(worker, workerThread, false);
    }

    public static void stopWorker(final JobExecutor worker, final Thread workerThread, boolean now) {
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
        } // Give worker time to process
        worker.end(now);
        try {
            workerThread.join();
        } catch (Exception e) {
            log.warn("Exception while waiting for workerThread to join", e);
        }
    }

    public static void delayEnqueueJobs(final String queue, final List<Job> jobs, final Config config) {
        final Client client = new ClientImpl(config);
        try {
            int i = 1;
            for (final Job job : jobs) {
                final long value = System.currentTimeMillis() + (500 * i++);
                client.delayedEnqueue(queue, job, value);
            }
        } finally {
            client.end();
        }
    }

    public static void removeDelayEnqueueJobs(final String queue, final List<Job> jobs, final Config config) {
        final Client client = new ClientImpl(config);
        try {
            for (final Job job : jobs) {
                client.removeDelayedEnqueue(queue, job);
            }
        } finally {
            client.end();
        }
    }

    public static void assertFullyEquals(final Object obj1, final Object obj2) {
        Assert.assertEquals(obj1, obj2);
        Assert.assertEquals(obj1.hashCode(), obj2.hashCode());
        Assert.assertEquals(obj1.toString(), obj2.toString());
    }

    private TestUtils() {} // Utility class
}
