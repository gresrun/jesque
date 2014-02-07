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

import java.util.Set;

import net.greghaines.jesque.worker.JobExecutor;
import net.greghaines.jesque.worker.Worker;

/**
 * Admin is an interface to receive administrative jobs for a worker.
 * 
 * @author Greg Haines
 */
public interface Admin extends JobExecutor, Runnable {
    
    /**
     * @return the set of subscribed Redis Pub/Sub channels
     */
    Set<String> getChannels();

    /**
     * @param channels the set of Redis Pub/Sub channels to subscribe to
     */
    void setChannels(Set<String> channels);

    /**
     * @return the worker this admin controls
     */
    Worker getWorker();

    /**
     * @param worker the worker this admin controls
     */
    void setWorker(Worker worker);
}
