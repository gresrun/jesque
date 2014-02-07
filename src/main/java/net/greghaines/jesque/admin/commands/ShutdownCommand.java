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
package net.greghaines.jesque.admin.commands;

import java.io.Serializable;

import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerAware;

/**
 * Shuts down the worker that receives this job.
 * 
 * @author Greg Haines
 */
public class ShutdownCommand implements Runnable, WorkerAware, Serializable {
    
    private static final long serialVersionUID = -4555815122109572121L;

    private final boolean now;
    private transient Worker worker;

    /**
     * Constructor.
     * @param now true if the worker should attempt to shutdown immediately, 
     * false if the worker should finish the current job
     */
    public ShutdownCommand(final boolean now) {
        this.now = now;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        if (this.worker == null) {
            throw new IllegalStateException("worker was not injected");
        }
        this.worker.end(this.now);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWorker(final Worker worker) {
        this.worker = worker;
    }
}
