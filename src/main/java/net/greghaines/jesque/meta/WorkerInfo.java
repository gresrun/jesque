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
package net.greghaines.jesque.meta;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * Information about the current state of a worker.
 * 
 * @author Greg Haines
 */
public class WorkerInfo implements Comparable<WorkerInfo>, Serializable {
    
    private static final long serialVersionUID = 7780544212376833441L;

    /**
     * Possible states of a worker.
     * 
     * @author Greg Haines
     */
    public enum State {
        IDLE, PAUSED, WORKING;
    }

    private String name;
    private State state;
    private Date started;
    private Long processed;
    private Long failed;
    private String host;
    private String pid;
    private List<String> queues;
    private WorkerStatus status;

    /**
     * @return the state of the worker
     */
    public State getState() {
        return this.state;
    }

    /**
     * @param state the state of the worker
     */
    public void setState(final State state) {
        this.state = state;
    }

    /**
     * @return the name of the worker
     */
    public String getName() {
        return this.name;
    }

    /**
     * @param name the name of the worker
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return when the worker started
     */
    public Date getStarted() {
        return this.started;
    }

    /**
     * @param started when the worker started
     */
    public void setStarted(final Date started) {
        this.started = started;
    }

    /**
     * @return how many jobs have been processed
     */
    public Long getProcessed() {
        return this.processed;
    }

    /**
     * @param processed how many jobs have been processed
     */
    public void setProcessed(final Long processed) {
        this.processed = processed;
    }

    /**
     * @return how many jobs have failed
     */
    public Long getFailed() {
        return this.failed;
    }

    /**
     * @param failed how many jobs have failed
     */
    public void setFailed(final Long failed) {
        this.failed = failed;
    }

    /**
     * @return the hostname of the worker
     */
    public String getHost() {
        return this.host;
    }

    /**
     * @param host the hostname of the worker
     */
    public void setHost(final String host) {
        this.host = host;
    }

    /**
     * @return the process ID of the worker
     */
    public String getPid() {
        return this.pid;
    }

    /**
     * @param pid the process ID of the worker
     */
    public void setPid(final String pid) {
        this.pid = pid;
    }

    /**
     * @return the list of queues the worker pulls from
     */
    public List<String> getQueues() {
        return this.queues;
    }

    /**
     * @param queues the list of queues the worker pulls from
     */
    public void setQueues(final List<String> queues) {
        this.queues = queues;
    }

    /**
     * @return the status of the worker
     */
    public WorkerStatus getStatus() {
        return this.status;
    }

    /**
     * @param status the status of the worker
     */
    public void setStatus(final WorkerStatus status) {
        this.status = status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.failed == null) ? 0 : this.failed.hashCode());
        result = prime * result + ((this.host == null) ? 0 : this.host.hashCode());
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime * result + ((this.pid == null) ? 0 : this.pid.hashCode());
        result = prime * result + ((this.processed == null) ? 0 : this.processed.hashCode());
        result = prime * result + ((this.queues == null) ? 0 : this.queues.hashCode());
        result = prime * result + ((this.started == null) ? 0 : this.started.hashCode());
        result = prime * result + ((this.state == null) ? 0 : this.state.hashCode());
        result = prime * result + ((this.status == null) ? 0 : this.status.hashCode());
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
        } else if (obj instanceof WorkerInfo) {
            final WorkerInfo other = (WorkerInfo) obj;
            equal = (JesqueUtils.nullSafeEquals(this.failed, other.failed)
                    && JesqueUtils.nullSafeEquals(this.host, other.host)
                    && JesqueUtils.nullSafeEquals(this.name, other.name)
                    && JesqueUtils.nullSafeEquals(this.pid, other.pid)
                    && JesqueUtils.nullSafeEquals(this.processed, other.processed)
                    && JesqueUtils.nullSafeEquals(this.queues, other.queues)
                    && JesqueUtils.nullSafeEquals(this.started, other.started)
                    && JesqueUtils.nullSafeEquals(this.state, other.state)
                    && JesqueUtils.nullSafeEquals(this.status, other.status));
        }
        return equal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(final WorkerInfo other) {
        int retVal = 1;
        if (other != null) {
            if (this.status != null && other.status != null) {
                if (this.status.getRunAt() != null && other.status.getRunAt() != null) {
                    retVal = this.status.getRunAt().compareTo(other.status.getRunAt());
                } else if (this.status.getRunAt() == null) {
                    retVal = (other.status.getRunAt() == null) ? 0 : -1;
                }
            } else if (this.status == null) {
                retVal = (other.status == null) ? 0 : -1;
            }
        }
        return retVal;
    }
}
