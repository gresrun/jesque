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

/**
 * A bean to hold information about a job that failed.
 * 
 * @author Greg Haines
 */
public class JobFailure implements Serializable {
    
    private static final long serialVersionUID = -2160045729341301316L;

    private String worker;
    private String queue;
    private Job payload;
    private Throwable exception;
    private String exceptionString;
    private List<String> backtrace;
    private String error;
    private Date failedAt;
    private Date retriedAt;

    /**
     * No-arg constructor.
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
        this.exception = origFailure.exception;
        this.failedAt = origFailure.failedAt;
        this.retriedAt = origFailure.retriedAt;
        this.exceptionString = origFailure.exceptionString;
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
     * @return the exception that occured
     */
    public Throwable getException() {
        return this.exception;
    }

    /**
     * Set the exception that occurred.
     * 
     * @param exception
     *            the kind of exception that occurred
     */
    public void setException(final Throwable exception) {
        this.exception = exception;
    }

    /**
     * @return the exception that occurred as a string
     */
    public String getExceptionString() {
        return this.exceptionString;
    }

    /**
     * Set the exception that occurred.
     * 
     * @param exceptionString
     *            the kind of exception that occurred as a string
     */
    public void setExceptionString(final String exceptionString) {
        this.exceptionString = exceptionString;
    }

    /**
     * @return the error that occurred
     */
    public String getError() {
        return this.error;
    }

    /**
     * Set the error that occurred
     * 
     * @param error
     *            the error that occurred
     */
    public void setError(final String error) {
        this.error = error;
    }

    /**
     * @return the backtrace of the exception
     */
    public List<String> getBacktrace() {
        return this.backtrace;
    }

    /**
     * Set the backtrace of the exception
     * 
     * @param backtrace
     *            the backtrace of the exception
     */
    public void setBacktrace(final List<String> backtrace) {
        this.backtrace = backtrace;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.exception == null) ? 0 : this.exception.hashCode());
        result = prime * result + ((this.failedAt == null) ? 0 : this.failedAt.hashCode());
        result = prime * result + ((this.retriedAt == null) ? 0 : this.retriedAt.hashCode());
        result = prime * result + ((this.payload == null) ? 0 : this.payload.hashCode());
        result = prime * result + ((this.worker == null) ? 0 : this.worker.hashCode());
        result = prime * result + ((this.queue == null) ? 0 : this.queue.hashCode());
        result = prime * result + ((this.exceptionString == null) ? 0 : this.exceptionString.hashCode());
        result = prime * result + ((this.error == null) ? 0 : this.error.hashCode());
        result = prime * result + ((this.backtrace == null) ? 0 : this.backtrace.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JobFailure other = (JobFailure) obj;
        if (this.exception == null) {
            if (other.exception != null) {
                return false;
            }
        } else if (!equal(this.exception, other.exception)) {
            return false;
        }
        if (this.failedAt == null) {
            if (other.failedAt != null) {
                return false;
            }
        } else if (!this.failedAt.equals(other.failedAt)) {
            return false;
        }
        if (this.retriedAt == null) {
            if (other.retriedAt != null) {
                return false;
            }
        } else if (!this.retriedAt.equals(other.retriedAt)) {
            return false;
        }
        if (this.payload == null) {
            if (other.payload != null) {
                return false;
            }
        } else if (!this.payload.equals(other.payload)) {
            return false;
        }
        if (this.worker == null) {
            if (other.worker != null) {
                return false;
            }
        } else if (!this.worker.equals(other.worker)) {
            return false;
        }
        if (this.queue == null) {
            if (other.queue != null) {
                return false;
            }
        } else if (!this.queue.equals(other.queue)) {
            return false;
        }
        if (this.exceptionString == null) {
            if (other.exceptionString != null) {
                return false;
            }
        } else if (!this.exceptionString.equals(other.exceptionString)) {
            return false;
        }
        if (this.error == null) {
            if (other.error != null) {
                return false;
            }
        } else if (!this.error.equals(other.error)) {
            return false;
        }
        if (this.backtrace == null) {
            if (other.backtrace != null) {
                return false;
            }
        } else if (!this.backtrace.equals(other.backtrace)) {
            return false;
        }
        return true;
    }

    /**
     * This is needed because Throwable doesn't override equals() and object
     * equality is not what we want to test.
     * 
     * @param ex
     *            original Throwable
     * @param newEx
     *            other Throwable
     * @return true if the two arguments are equal, as we define it.
     */
    private static boolean equal(final Throwable ex, final Throwable newEx) {
        if (ex == newEx) {
            return true;
        }
        if (ex == null) {
            if (newEx != null) {
                return false;
            }
        } else {
            if (ex.getClass() != newEx.getClass()) {
                return false;
            }
            if (ex.getMessage() == null) {
                if (newEx.getMessage() != null) {
                    return false;
                }
            } else if (!ex.getMessage().equals(newEx.getMessage())) {
                return false;
            }
            if (ex.getCause() == null) {
                if (newEx.getCause() != null) {
                    return false;
                }
            } else if (!equal(ex.getCause(), newEx.getCause())) {
                return false;
            }
        }
        return true;
    }
}
