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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import net.greghaines.jesque.Config;

/**
 * A simple factory for <code>WorkerImpl</code>s. Designed to be used with
 * <code>WorkerPool</code>.
 * 
 * @author Greg Haines
 */
public class WorkerImplFactory implements Callable<WorkerImpl> {
    
    private final Config config;
    private final Collection<String> queues;
    private final Map<String, ? extends Class<?>> jobTypes;
    private JobFactory jobFactory = null;

    public JobFactory getJobFactory() {
		return jobFactory;
	}

	public void setJobFactory(JobFactory jobFactory) {
		this.jobFactory = jobFactory;
	}

	/**
     * Create a new factory. Returned <code>WorkerImpl</code>s will use the
     * provided arguments.
     * 
     * @param config
     *            used to create a connection to Redis and the package prefix
     *            for incoming jobs
     * @param queues
     *            the list of queues to poll
     * @param jobTypes
     *            the list of job types to execute
     */
    public WorkerImplFactory(final Config config, final Collection<String> queues,
            final Map<String, ? extends Class<?>> jobTypes) {
        this.config = config;
        this.queues = queues;
        this.jobTypes = jobTypes;
    }

    /**
     * Create a new <code>WorkerImpl</code> using the arguments provided to this
     * factory's constructor.
     */
    public WorkerImpl call() {
        WorkerImpl result = new WorkerImpl(this.config, this.queues, this.jobTypes);
        if(jobFactory != null) {
        	result.setJobFactory(jobFactory);
        }
        return result;
    }
}
