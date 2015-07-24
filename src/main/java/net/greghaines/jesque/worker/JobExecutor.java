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
package net.greghaines.jesque.worker;

/**
 * JobExecutor is an object that executes jobs.
 */
public interface JobExecutor {
    
    /**
     * States of the job executor.
     */
    public enum State {
        /**
         * The JobExecutor has not started running.
         */
        NEW,
        /**
         * The JobExecutor is currently running.
         */
        RUNNING,
        /**
         * The JobExecutor has shutdown.
         */
        SHUTDOWN,
        /**
         * The JobExecutor has shutdown, interrupting running jobs.
         */
        SHUTDOWN_IMMEDIATE;
    }
    
    /**
     * The job factory.
     * @return the job factory
     */
    JobFactory getJobFactory();

    /**
     * The current exception handler.
     * @return the current exception handler
     */
    ExceptionHandler getExceptionHandler();

    /**
     * Set this JobExecutor's exception handler to the given handler.
     * @param exceptionHandler the exception handler to use
     */
    void setExceptionHandler(ExceptionHandler exceptionHandler);

    /**
     * Shutdown this JobExecutor.
     * @param now if true, an effort will be made to stop any job in progress
     */
    void end(boolean now);

    /**
     * Returns whether this JobExecutor is either shutdown or in the process of shutting down.
     * @return true if this JobExecutor is either shutdown or in the process of shutting down
     */
    boolean isShutdown();

    /**
     * Returns whether this JobExecutor is currently processing a job.
     * @return true if this JobExecutor is currently processing a job
     */
    boolean isProcessingJob();

    /**
     * Wait for this JobExecutor to complete. A timeout of 0 means to wait forever.
     * This method will only return after a thread has called {@link #end(boolean)}.
     * @param millis the time to wait in milliseconds
     * @throws InterruptedException if any thread has interrupted the current thread
     */
    void join(long millis) throws InterruptedException;
}
