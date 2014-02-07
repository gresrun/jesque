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
package net.greghaines.jesque.admin;

import static net.greghaines.jesque.utils.ResqueConstants.ADMIN_CHANNEL;
import static net.greghaines.jesque.utils.ResqueConstants.CHANNEL;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import redis.clients.jedis.Jedis;

/**
 * Common logic for AdminClient implementations.
 * 
 * @author Greg Haines
 */
public abstract class AbstractAdminClient implements AdminClient {
    
    private final String namespace;

    /**
     * @param config
     *            used to get the namespace for key creation
     */
    protected AbstractAdminClient(final Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.namespace = config.getNamespace();
    }

    /**
     * @return the namespace this client will use
     */
    protected String getNamespace() {
        return this.namespace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdownWorkers(final boolean now) {
        publish(new Job("ShutdownCommand", now));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdownWorkers(final String channel, final boolean now) {
        publish(channel, new Job("ShutdownCommand", now));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void togglePausedWorkers(final boolean paused) {
        publish(new Job("PauseCommand", paused));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void togglePausedWorkers(final String channel, final boolean paused) {
        publish(channel, new Job("PauseCommand", paused));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(final Job job) {
        publish(ADMIN_CHANNEL, job);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(final String channel, final Job job) {
        validateArguments(channel, job);
        try {
            doPublish(channel, ObjectMapperFactory.get().writeValueAsString(job));
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Actually publish the serialized job.
     * 
     * @param queue
     *            the queue to add the Job to
     * @param msg
     *            the serialized Job
     * @throws Exception
     *             in case something goes wrong
     */
    protected abstract void doPublish(String queue, String msg) throws Exception;

    /**
     * Helper method that encapsulates the minimum logic for publishing a job to
     * a channel.
     * 
     * @param jedis
     *            the connection to Redis
     * @param namespace
     *            the Resque namespace
     * @param channel
     *            the channel name
     * @param jobJson
     *            the job serialized as JSON
     */
    public static void doPublish(final Jedis jedis, final String namespace, final String channel, final String jobJson) {
        jedis.publish(JesqueUtils.createKey(namespace, CHANNEL, channel), jobJson);
    }

    private static void validateArguments(final String channel, final Job job) {
        if (channel == null || "".equals(channel)) {
            throw new IllegalArgumentException("channel must not be null or empty: " + channel);
        }
        if (job == null) {
            throw new IllegalArgumentException("job must not be null");
        }
        if (!job.isValid()) {
            throw new IllegalStateException("job is not valid: " + job);
        }
    }
}
