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
import java.util.List;

import net.greghaines.jesque.utils.JesqueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A bean to hold information about a job that failed.
 * 
 * @author Greg Haines
 */
public class JobFailure implements Serializable {
    
    private static final long serialVersionUID = -2160045729341301316L;
    private static final Logger LOG = LoggerFactory.getLogger(JobFailure.class);

    @JsonProperty
    private String worker;
    @JsonProperty
    private String queue;
    @JsonProperty
    private Job payload;
    @JsonIgnore
    private Throwable throwable;
    private String throwableString;
    private List<String> backtrace;
    private String error;
    @JsonProperty("failed_at")
    private Date failedAt;
    @JsonProperty("retried_at")
    private Date retriedAt;

    /**
     * No-argument constructor.
     */
    public JobFailure() {
        // Do nothing
    }

    /**
     * Cloning constructor.
     * 
     * @param origFailure
     *            the failure to start from
     * @throws IllegalArgumentException
     *             if the origFailure is null
     */
    public JobFailure(final JobFailure origFailure) {
        if (origFailure == null) {
            throw new IllegalArgumentException("origFailure must not be null");
        }
        this.worker = origFailure.worker;
        this.queue = origFailure.queue;
        this.payload = origFailure.payload;
        this.throwable = origFailure.throwable;
        this.failedAt = origFailure.failedAt;
        this.retriedAt = origFailure.retriedAt;
        this.throwableString = origFailure.throwableString;
        this.error = origFailure.error;
        this.backtrace = origFailure.backtrace;
    }

    /**
     * @return the name of the worker where the job failed
     */
    public String getWorker() {
        return this.worker;
    }

    /**
     * Set the name of the worker where the job failed.
     * 
     * @param worker
     *            the name of the worker
     */
    public void setWorker(final String worker) {
        this.worker = worker;
    }

    /**
     * @return the queue the job came from
     */
    public String getQueue() {
        return this.queue;
    }

    /**
     * Set the queue the job came from.
     * 
     * @param queue
     *            the queue the job came from
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
     * @return the exception that occurred
     */
    public Throwable getThrowable() {
        return this.throwable;
    }

    /**
     * Set the throwable that occurred.
     * 
     * @param throwable
     *            the kind of throwable that occurred
     */
    public void setThrowable(final Throwable throwable) {
        this.throwable = throwable;
    }

    /**
     * @return the exception that occurred as a string
     */
    @JsonProperty("exception")
    public String getThrowableString() {
        return (this.throwable == null) ? this.throwableString : this.throwable.getClass().getName();
    }

    /**
     * Set the exception that occurred.
     * 
     * @param throwableString
     *            the kind of exception that occurred as a string
     */
    @JsonProperty("exception")
    public void setThrowableString(final String throwableString) {
        this.throwableString = throwableString;
        tryCreateThrowable();
    }

    /**
     * @return the error that occurred
     */
    @JsonProperty
    public String getError() {
        return (this.throwable == null) ? this.error : this.throwable.getMessage();
    }

    /**
     * Set the error that occurred
     * 
     * @param error
     *            the error that occurred
     */
    @JsonProperty
    public void setError(final String error) {
        this.error = error;
        tryCreateThrowable();
    }

    /**
     * @return the backtrace of the throwable
     */
    @JsonProperty
    public List<String> getBacktrace() {
        return (this.throwable == null) ? this.backtrace : JesqueUtils.createBacktrace(this.throwable);
    }

    /**
     * Set the backtrace of the throwable
     * 
     * @param backtrace
     *            the backtrace of the throwable
     */
    @JsonProperty
    public void setBacktrace(final List<String> backtrace) {
        this.backtrace = backtrace;
        tryCreateThrowable();
    }

    /**
     * @return when the error occurred
     */
    public Date getFailedAt() {
        return this.failedAt;
    }

    /**
     * Set when the error occurred.
     * 
     * @param failedAt
     *            when the error occurred
     */
    public void setFailedAt(final Date failedAt) {
        this.failedAt = failedAt;
    }

    /**
     * @return when the job was retried
     */
    public Date getRetriedAt() {
        return this.retriedAt;
    }

    /**
     * Set when the job was retried.
     * 
     * @param retriedAt
     *            when the job was retried
     */
    public void setRetriedAt(final Date retriedAt) {
        this.retriedAt = retriedAt;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "JobFailure [worker=" + this.worker + ", queue=" + this.queue+ ", payload=" + this.payload
            + ", throwable=" + this.throwable + ", failedAt=" + this.failedAt + ", retriedAt=" + this.retriedAt + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.throwable == null) ? 0 : this.throwable.hashCode());
        result = prime * result + ((this.failedAt == null) ? 0 : this.failedAt.hashCode());
        result = prime * result + ((this.retriedAt == null) ? 0 : this.retriedAt.hashCode());
        result = prime * result + ((this.payload == null) ? 0 : this.payload.hashCode());
        result = prime * result + ((this.worker == null) ? 0 : this.worker.hashCode());
        result = prime * result + ((this.queue == null) ? 0 : this.queue.hashCode());
        result = prime * result + ((this.throwableString == null) ? 0 : this.throwableString.hashCode());
        result = prime * result + ((this.error == null) ? 0 : this.error.hashCode());
        result = prime * result + ((this.backtrace == null) ? 0 : this.backtrace.hashCode());
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
        } else if (obj instanceof JobFailure) {
            final JobFailure other = (JobFailure) obj;
            equal = (JesqueUtils.nullSafeEquals(this.queue, other.queue)
                    && JesqueUtils.nullSafeEquals(this.worker, other.worker)
                    && JesqueUtils.nullSafeEquals(this.throwableString, other.throwableString)
                    && JesqueUtils.nullSafeEquals(this.error, other.error)
                    && JesqueUtils.nullSafeEquals(this.failedAt, other.failedAt)
                    && JesqueUtils.nullSafeEquals(this.retriedAt, other.retriedAt)
                    && JesqueUtils.equal(this.throwable, other.throwable)
                    && JesqueUtils.nullSafeEquals(this.payload, other.payload)
                    && JesqueUtils.nullSafeEquals(this.backtrace, other.backtrace));
        }
        return equal;
    }
    
    private void tryCreateThrowable() {
        if (this.throwable == null && this.throwableString != null && this.error != null && this.backtrace != null) {
            try {
                this.throwable = JesqueUtils.recreateThrowable(this.throwableString, this.error, this.backtrace);
            } catch (Exception e) {
                LOG.warn("Error while recreating throwable: " + this.throwableString + " " 
                        + this.error + " " + this.backtrace, e);
            }
        }
    }
}
