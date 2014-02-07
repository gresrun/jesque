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

import java.util.Date;
import java.util.List;

import net.greghaines.jesque.JobFailure;

/**
 * FailureDAO provides access to job failures.
 * 
 * @author Greg Haines
 */
public interface FailureDAO {
    
    /**
     * @return total number of failures
     */
    long getCount();

    /**
     * @param offset offset into the failures
     * @param count number of failures to return
     * @return a sub-list of the failures
     */
    List<JobFailure> getFailures(long offset, long count);

    /**
     * Clear the list of failures.
     */
    void clear();

    /**
     * Re-queue a job for execution.
     * @param index the index into the failure list
     * @return the date the job was re-queued
     */
    Date requeue(long index);

    /**
     * Remove a failure from the list.
     * @param index the index of the failure to remove
     */
    void remove(long index);
}
