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

/**
 * The possible WorkerEvents that a WorkerListener may register for.
 */
public enum WorkerEvent {
    
    /**
     * The Worker just finished starting up and is about to start running.
     */
    WORKER_START,
    /**
     * The Worker is polling the queue.
     */
    WORKER_POLL,
    /**
     * The Worker is processing a Job.
     */
    JOB_PROCESS,
    /**
     * The Worker is about to execute a materialized Job.
     */
    JOB_EXECUTE,
    /**
     * The Worker successfully executed a materialized Job.
     */
    JOB_SUCCESS,
    /**
     * The Worker caught an Exception during the execution of a materialized Job.
     */
    JOB_FAILURE,
    /**
     * The Worker caught an Exception during normal operation.
     */
    WORKER_ERROR,
    /**
     * The Worker just finished running and is about to shutdown.
     */
    WORKER_STOP;
}
