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
import java.util.Map;

import net.greghaines.jesque.meta.WorkerInfo;

/**
 * WorkerInfoDAO provides access to information about workers.
 * 
 * @author Greg Haines
 */
public interface WorkerInfoDAO {
    
    /**
     * @return total number of workers known
     */
    long getWorkerCount();

    /**
     * @return number of active workers
     */
    long getActiveWorkerCount();

    /**
     * @return number of paused workers
     */
    long getPausedWorkerCount();

    /**
     * @return information about all active workers
     */
    List<WorkerInfo> getActiveWorkers();

    /**
     * @return information about all paused workers
     */
    List<WorkerInfo> getPausedWorkers();

    /**
     * @return information about all workers
     */
    List<WorkerInfo> getAllWorkers();

    /**
     * @param workerName the name of the worker
     * @return information about the given worker or null if that worker does not exist
     */
    WorkerInfo getWorker(String workerName);

    /**
     * @return a map of worker informations by hostname
     */
    Map<String, List<WorkerInfo>> getWorkerHostMap();

    /**
     * Removes the metadata about a worker.
     * 
     * @param workerName
     *            The worker name to remove
     */
    void removeWorker(String workerName);
}
