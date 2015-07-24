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
package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

import java.lang.Throwable;

/**
 * A WorkerListener can register with a Worker to be notified of WorkerEvents.
 * 
 * @author Greg Haines
 * @see WorkerEvent
 */
public interface WorkerListener {

    /**
     * This method is called by the Worker upon the occurrence of a registered WorkerEvent.
     * @param event the WorkerEvent that occurred
     * @param worker the Worker that the event occurred in
     * @param queue the queue the Worker is processing
     * @param job the Job related to the event (only set for JOB_PROCESS, JOB_EXECUTE, JOB_SUCCESS, and 
     * JOB_FAILURE events)
     * @param runner the materialized object that the Job specified (only set for JOB_EXECUTE and JOB_SUCCESS events)
     * @param result the result of the successful execution of the Job (only set for JOB_SUCCESS and if the Job was 
     * a Callable that returned a value)
     * @param t the Throwable that caused the event (only set for JOB_FAILURE and ERROR events)
     */
    void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Throwable t);
}
