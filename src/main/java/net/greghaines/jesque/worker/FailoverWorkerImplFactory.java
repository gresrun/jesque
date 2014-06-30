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

import java.util.concurrent.Callable;

import net.greghaines.jesque.ConfigFO;

/**
 * A simple factory for <code>FailoverWorkerImpl</code>s. Designed to be used with
 * <code>WorkerPool</code>.
 * 
 * @author Greg Haines
 */
public class FailoverWorkerImplFactory implements Callable<FailoverWorkerImpl> {
    
    private final ConfigFO config;

    /**
     * Create a new factory. Returned <code>FailoverWorkerImpl</code>s will use the
     * provided arguments.
     * 
     * @param config
     *            used to create a connection to Redis and the package prefix
     *            for incoming jobs
     */
    public FailoverWorkerImplFactory(final ConfigFO config) {
        this.config = config;
    }

    /**
     * Create a new <code>FailoverWorkerImpl</code> using the arguments provided to this
     * factory's constructor.
     */
    public FailoverWorkerImpl call() {
        return new FailoverWorkerImpl(this.config);
    }
}
