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
import java.util.Arrays;
import java.util.List;

/**
 * A simple class to describe a job to be run by a worker.
 * 
 * @author Greg Haines
 */
public class Job implements Serializable {
    
    private static final long serialVersionUID = -1523425239512691383L;

    private String className;
    private Object[] args;

    /**
     * No-argument constructor.
     */
    public Job() {
        // Do nothing
    }

    /**
     * Cloning constructor. Makes a clone of the arguments, if they exist.
     * 
     * @param origJob
     *            the Job to start from
     * @throws IllegalArgumentException
     *             if the origJob is null
     */
    public Job(final Job origJob) {
        if (origJob == null) {
            throw new IllegalArgumentException("origJob must not be null");
        }
        this.className = origJob.className;
        this.args = (origJob.args == null) ? null : origJob.args.clone();
    }

    /**
     * A convenience constructor. Delegates to Job(String, Object...) by calling
     * args.toArray().
     * 
     * @param className
     *            the class name of the Job
     * @param args
     *            the arguments for the Job
     */
    public Job(final String className, final List<?> args) {
        this(className, args.toArray());
    }

    /**
     * Create a new Job with the given class name and arguments.
     * 
     * @param className
     *            the class name of the Job
     * @param args
     *            the arguments for the Job
     */
    public Job(final String className, final Object... args) {
        if (className == null || "".equals(className)) {
            throw new IllegalArgumentException("className must not be null or empty: " + className);
        }
        this.className = className;
        this.args = args;
    }

    /**
     * @return the name of the Job's class
     */
    public String getClassName() {
        return this.className;
    }

    /**
     * Set the class name.
     * 
     * @param className
     *            the new class name
     */
    public void setClassName(final String className) {
        this.className = className;
    }

    /**
     * @return the arguments for the job
     */
    public Object[] getArgs() {
        return this.args;
    }

    /**
     * Set the arguments.
     * 
     * @param args
     *            the new arguments
     */
    public void setArgs(final Object... args) {
        this.args = args;
    }

    /**
     * @return true if this Job has a valid class name and arguments
     */
    public boolean isValid() {
        return (this.args != null && this.className != null && !"".equals(this.className));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "<Job className=" + this.className + " args=" + Arrays.toString(this.args) + ">";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.args);
        result = prime * result + ((this.className == null) ? 0 : this.className.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Job)) {
            return false;
        }
        final Job other = (Job) obj;
        if (this.className == null) {
            if (other.className != null) {
                return false;
            }
        } else if (!this.className.equals(other.className)) {
            return false;
        }
        if (!Arrays.equals(this.args, other.args)) {
            return false;
        }
        return true;
    }
}
