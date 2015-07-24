/*
 * Copyright 2014 Greg Haines
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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * MapBasedJobFactory uses a map of job names and types to materialize jobs.
 */
public class MapBasedJobFactory implements JobFactory {
    
    private final ConcurrentMap<String, Class<?>> jobTypes = 
            new ConcurrentHashMap<String, Class<?>>();

    /**
     * Constructor.
     * @param jobTypes the map of job names and types to execute
     */
    public MapBasedJobFactory(final Map<String, ? extends Class<?>> jobTypes) {
        setJobTypes(jobTypes);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object materializeJob(final Job job) throws Exception {
        return JesqueUtils.materializeJob(job, this.jobTypes);
    }

    /**
     * The allowed job names and types that this JobExecutor will execute.
     * @return an unmodifiable view of the allowed job names and types
     */
    public Map<String, Class<?>> getJobTypes() {
        return Collections.unmodifiableMap(this.jobTypes);
    }

    /**
     * Allow the given job type to be executed.
     * @param jobName the job name as seen
     * @param jobType the job type to allow
     */
    public void addJobType(final String jobName, final Class<?> jobType) {
        checkJobType(jobName, jobType);
        this.jobTypes.put(jobName, jobType);
    }

    /**
     * Disallow the job type from being executed.
     * @param jobType the job type to disallow
     */
    public void removeJobType(final Class<?> jobType) {
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null");
        }
        this.jobTypes.values().remove(jobType);
    }

    /**
     * Disallow the job name from being executed.
     * @param jobName the job name to disallow
     */
    public void removeJobName(final String jobName) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null");
        }
        this.jobTypes.remove(jobName);
    }

    /**
     * Clear any current allowed job types and use the given set.
     * @param jobTypes the job types to allow
     */
    public void setJobTypes(final Map<String, ? extends Class<?>> jobTypes) {
        checkJobTypes(jobTypes);
        this.jobTypes.clear();
        this.jobTypes.putAll(jobTypes);
    }

    /**
     * Verify the given job types are all valid.
     * 
     * @param jobTypes the given job types
     * @throws IllegalArgumentException if any of the job types are invalid
     * @see #checkJobType(String, Class)
     */
    protected void checkJobTypes(final Map<String, ? extends Class<?>> jobTypes) {
        if (jobTypes == null) {
            throw new IllegalArgumentException("jobTypes must not be null");
        }
        for (final Entry<String, ? extends Class<?>> entry : jobTypes.entrySet()) {
            try {
                checkJobType(entry.getKey(), entry.getValue());
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("jobTypes contained invalid value", iae);
            }
        }
    }

    /**
     * Determine if a job name and job type are valid.
     * @param jobName the name of the job
     * @param jobType the class of the job
     * @throws IllegalArgumentException if the name or type are invalid
     */
    protected void checkJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null");
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null");
        }
        if (!(Runnable.class.isAssignableFrom(jobType)) 
                && !(Callable.class.isAssignableFrom(jobType))) {
            throw new IllegalArgumentException(
                    "jobType must implement either Runnable or Callable: " + jobType);
        }
    }
}
