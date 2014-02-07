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
package net.greghaines.jesque.meta.dao;

import java.util.List;

import net.greghaines.jesque.meta.QueueInfo;

/**
 * QueueInfoDAO provides access to the queues in use by Jesque.
 * 
 * @author Greg Haines
 */
public interface QueueInfoDAO {
    
    /**
     * @return the list of queue names
     */
    List<String> getQueueNames();

    /**
     * @return total number of jobs pending in all queues
     */
    long getPendingCount();

    /**
     * @return total number of jobs processed
     */
    long getProcessedCount();

    /**
     * @return the list of queue informations
     */
    List<QueueInfo> getQueueInfos();

    /**
     * @param name the queue name
     * @param jobOffset the offset into the queue
     * @param jobCount the number of jobs to return
     * @return the queue information or null if the queue does not exist
     */
    QueueInfo getQueueInfo(String name, long jobOffset, long jobCount);

    /**
     * Delete the given queue.
     * @param name the name of the queue
     */
    void removeQueue(String name);
}
