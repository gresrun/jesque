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

import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;

import java.io.Serializable;
import java.util.List;

/**
 * Information about the current state of a queue.
 * 
 * @author Greg Haines
 */
public class QueueInfo implements Comparable<QueueInfo>, Serializable {
    
    private static final long serialVersionUID = 562750483276247591L;

    private String name;
    private Long size;
    private List<Job> jobs;
    private Boolean delayed;

    /**
     * @return the name of the queue
     */
    public String getName() {
        return this.name;
    }

    /**
     * @param name the name of the queue
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return the size of the queue
     */
    public Long getSize() {
        return this.size;
    }

    /**
     * @param size the size of the queue
     */
    public void setSize(final Long size) {
        this.size = size;
    }

    /**
     * @return the jobs in the queue
     */
    public List<Job> getJobs() {
        return this.jobs;
    }

    /**
     * @param jobs the jobs in the queue
     */
    public void setJobs(final List<Job> jobs) {
        this.jobs = jobs;
    }

    /**
     * @return whether this queue is a delayed queue
     */
    public Boolean isDelayed() {
        return this.delayed;
    }

    /**
     * @param delayed whether this queue is a delayed queue
     */
    public void setDelayed(final Boolean delayed) {
        this.delayed = delayed;
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
        result = prime * result + ((this.jobs == null) ? 0 : this.jobs.hashCode());
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime * result + ((this.size == null) ? 0 : this.size.hashCode());
        result = prime * result + ((this.delayed == null) ? 0 : this.delayed.hashCode());
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
        } else if (obj instanceof QueueInfo) {
            final QueueInfo other = (QueueInfo) obj;
            equal = (JesqueUtils.nullSafeEquals(this.jobs, other.jobs)
                    && JesqueUtils.nullSafeEquals(this.name, other.name)
                    && JesqueUtils.nullSafeEquals(this.size, other.size)
                    && JesqueUtils.nullSafeEquals(this.delayed, other.delayed));
        }
        return equal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(final QueueInfo other) {
        int retVal = 1;
        if (other != null) {
            if (this.name != null && other.name != null) {
                retVal = this.name.compareTo(other.name);
            } else if (this.name == null) {
                retVal = (other.name == null) ? 0 : -1;
            }
        }
        return retVal;
    }
}
