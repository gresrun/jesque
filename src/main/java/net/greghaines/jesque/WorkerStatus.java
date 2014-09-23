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
package net.greghaines.jesque;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

import net.greghaines.jesque.utils.JesqueUtils;

/**
 * A bean to hold information about the status of a Worker.
 * 
 * @author Greg Haines
 */
public class WorkerStatus implements Serializable {
    
    private static final long serialVersionUID = 1852915628988733048L;

    @JsonProperty("run_at")
    private Date runAt;
    @JsonProperty
    private String queue;
    @JsonProperty
    private Job payload;
    @JsonProperty
    private boolean paused = false;

    /**
     * No-argument constructor.
     */
    public WorkerStatus() {
        // Do nothing
    }

    /**
     * Cloning constructor.
     * 
     * @param origStatus
     *            the status to start from
     * @throws IllegalArgumentException
     *             if the origStatus is null
     */
    public WorkerStatus(final WorkerStatus origStatus) {
        if (origStatus == null) {
            throw new IllegalArgumentException("origStatus must not be null");
        }
        this.runAt = origStatus.runAt;
        this.queue = origStatus.queue;
        this.payload = origStatus.payload;
        this.paused = origStatus.paused;
    }

    /**
     * @return when the Worker started on the current job
     */
    public Date getRunAt() {
        return this.runAt;
    }

    /**
     * Set when the Worker started on the current job.
     * 
     * @param runAt
     *            when the Worker started on the current job
     */
    public void setRunAt(final Date runAt) {
        this.runAt = runAt;
    }

    /**
     * @return which queue the current job came from
     */
    public String getQueue() {
        return this.queue;
    }

    /**
     * Set which queue the current job came from.
     * 
     * @param queue
     *            which queue the current job came from
     */
    public void setQueue(final String queue) {
        this.queue = queue;
    }

    /**
     * @return the job
     */
    public Job getPayload() {
        return this.payload;
    }

    /**
     * Set the job.
     * 
     * @param payload
     *            the job
     */
    public void setPayload(final Job payload) {
        this.payload = payload;
    }

    /**
     * @return true if the worker is paused
     */
    public boolean isPaused() {
        return this.paused;
    }

    /**
     * Sets whether the worker is paused.
     * 
     * @param paused
     *            whether the worker is paused
     */
    public void setPaused(final boolean paused) {
        this.paused = paused;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "WorkerStatus [queue=" + this.queue + ", runAt=" + this.runAt 
            + ", paused=" + Boolean.toString(this.paused) + ", payload=" + this.payload + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.paused ? 1231 : 1237);
        result = prime * result + ((this.payload == null) ? 0 : this.payload.hashCode());
        result = prime * result + ((this.queue == null) ? 0 : this.queue.hashCode());
        result = prime * result + ((this.runAt == null) ? 0 : this.runAt.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        boolean equal = false;
        if (this == obj) {
            equal = true;
        } else if (obj instanceof WorkerStatus) {
            final WorkerStatus other = (WorkerStatus) obj;
            equal = ((this.paused == other.paused)
                    && JesqueUtils.nullSafeEquals(this.queue, other.queue)
                    && JesqueUtils.nullSafeEquals(this.runAt, other.runAt)
                    && JesqueUtils.nullSafeEquals(this.payload, other.payload));
        }
        return equal;
    }
}
