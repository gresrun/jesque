/*
 * Copyright 2015 Greg Haines
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
package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

/**
 * FailQueueStrategy allows for configurable failure queues.
 */
public interface FailQueueStrategy {

    /**
     * Determine the key for the failure queue.
     * A null return value is an indication that no failure queue is needed.
     * Note that the default implementation (@see DefaultFailQueueStrategy) will never return null.
     * @param thrwbl the Throwable that occurred
     * @param job the Job that failed
     * @param curQueue the queue the Job came from
     * @return the key of the failure queue to put the job into or null
     */
    String getFailQueueKey(Throwable thrwbl, Job job, String curQueue);

    /**
     * Determine the max number of items to keep in the failure queue.
     * Returning a value <1 means that there is no limit.
     * @param curQueue the queue the Job came from
     * @return the max number of items to keep for the failure queue
     */
    int getFailQueueMaxItems(String curQueue);
}
