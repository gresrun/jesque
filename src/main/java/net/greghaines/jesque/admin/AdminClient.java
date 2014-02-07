/*
 * Copyright 2012 Greg Haines
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

import net.greghaines.jesque.Job;

/**
 * An AdminClient publishes jobs to channels.
 * 
 * @author Greg Haines
 */
public interface AdminClient {
    
    /**
     * Send a shutdown command on the
     * {@link net.greghaines.jesque.utils.ResqueConstants#ADMIN_CHANNEL}
     * channel.
     * 
     * @param now
     *            if true, an effort will be made to stop any job in progress
     */
    void shutdownWorkers(boolean now);

    /**
     * Send a shutdown command on the given channel.
     * 
     * @param channel
     *            the channel to publish the pause command to
     * @param now
     *            if true, an effort will be made to stop any job in progress
     */
    void shutdownWorkers(String channel, boolean now);

    /**
     * Send a pause command on the
     * {@link net.greghaines.jesque.utils.ResqueConstants#ADMIN_CHANNEL}
     * channel.
     * 
     * @param paused
     *            if true, the workers will not process any new jobs; if false,
     *            the workers will process new jobs
     */
    void togglePausedWorkers(boolean paused);

    /**
     * Send a pause command on the given channel.
     * 
     * @param channel
     *            the channel to publish the pause command to
     * @param paused
     *            if true, the workers will not process any new jobs; if false,
     *            the workers will process new jobs
     */
    void togglePausedWorkers(String channel, boolean paused);

    /**
     * Publishes a job on the
     * {@link net.greghaines.jesque.utils.ResqueConstants#ADMIN_CHANNEL}
     * channel.
     * 
     * @param job
     *            job the job to be published
     * @throws IllegalArgumentException
     *             if the job is null
     */
    void publish(Job job);

    /**
     * Publishes a job on the given channel.
     * 
     * @param channel
     *            the channel to publish the job to
     * @param job
     *            job the job to be published
     * @throws IllegalArgumentException
     *             if the channel is null or empty or if the job is null
     */
    void publish(String channel, Job job);

    /**
     * Quits the connection to the Redis server.
     */
    void end();
}
