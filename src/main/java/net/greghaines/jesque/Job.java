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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.greghaines.jesque.utils.JesqueUtils;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A simple class to describe a job to be run by a worker.
 * 
 * @author Greg Haines
 */
public class Job implements Serializable {
    
    private static final long serialVersionUID = -1523425239512691383L;

    private String className;
    private Object[] args;
    private Map<String,Object> vars;
    private Map<String,Object> unknownFields = new HashMap<String,Object>();
    private Double runAt; // only set if this job belongs to a delayed queue

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
        this.vars = (origJob.vars == null) ? null : new LinkedHashMap<String,Object>(origJob.vars);
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
     * Create a new Job with the given class name and named arguments.<br>
     * Resque 2.0-style Job.
     * 
     * @param className
     *            the class name of the Job
     * @param vars
     *            the named arguments for the Job
     */
    @SuppressWarnings("unchecked")
    public Job(final String className, final Map<String,? extends Object> vars) {
        if (className == null || "".equals(className)) {
            throw new IllegalArgumentException("className must not be null or empty: " + className);
        }
        this.className = className;
        this.vars = (Map<String, Object>)vars;
    }

    /**
     * Create a new Job with the given class name and both types of arguments.<br>
     * Resque 2.0-style Job.
     * 
     * @param className
     *            the class name of the Job
     * @param args
     *            the arguments for the Job
     * @param vars
     *            the named arguments for the Job
     */
    @SuppressWarnings("unchecked")
    @JsonCreator
    public Job(@JsonProperty("class") final String className, 
            @JsonProperty("args") final Object[] args, 
            @JsonProperty("vars") final Map<String,? extends Object> vars) {
        if (className == null || "".equals(className)) {
            throw new IllegalArgumentException("className must not be null or empty: " + className);
        }
        this.className = className;
        this.args = args;
        this.vars = (Map<String, Object>)vars;
    }

    /**
     * @return the name of the Job's class
     */
    @JsonProperty(value = "class", required = true)
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
    @JsonProperty
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
     * @return the named arguments for the job
     */
    @JsonProperty
    public Map<String, Object> getVars() {
        return this.vars;
    }

    /**
     * Set the named arguments.
     * 
     * @param vars
     *            the new named arguments
     */
    @SuppressWarnings("unchecked")
    public void setVars(final Map<String, ? extends Object> vars) {
        this.vars = (Map<String, Object>)vars;
    }

    @JsonIgnore
    public Double getRunAt() {
        return runAt;
    }

    @JsonIgnore
    public void setRunAt(Double runAt) {
        this.runAt = runAt;
    }

    /**
     * @return true if this Job has a valid class name and arguments
     */
    @JsonIgnore
    public boolean isValid() {
        return ((this.args != null || this.vars != null) && this.className != null && !"".equals(this.className));
    }

    /**
     * Get an unknown field.
     * @param fieldName the field name
     * @return the value
     */
    @JsonIgnore
    public Object getUnknownField(final String fieldName) {
        return this.unknownFields.get(fieldName);
    }

    /**
     * Get all unknown fields.
     * @return all unknown fields
     */
    @JsonAnyGetter
    public Map<String,Object> getUnknownFields() {
        return this.unknownFields;
    }

    /**
     * Set an unknown field.
     * @param name the unknown property name
     * @param value the unknown property value
     */
    @JsonAnySetter
    public void setUnknownField(final String name, final Object value) {
        this.unknownFields.put(name, value);
    }

    /**
     * Set all unknown fields
     * @param unknownFields the new unknown fields
     */
    @JsonIgnore
    public void setUnknownFields(final Map<String,Object> unknownFields) {
        this.unknownFields.clear();
        this.unknownFields.putAll(unknownFields);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Job [class=" + this.className + ", args=" + Arrays.toString(this.args) 
                + ", vars=" + this.vars + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.className == null) ? 0 : this.className.hashCode());
        result = prime * result + Arrays.hashCode(this.args);
        result = prime * result + ((this.vars == null) ? 0 : this.vars.hashCode());
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
        } else if (obj instanceof Job) {
            final Job other = (Job) obj;
            equal = (JesqueUtils.nullSafeEquals(this.className, other.className)
                    && Arrays.equals(this.args, other.args)
                    && JesqueUtils.nullSafeEquals(this.vars, other.vars));
        }
        return equal;
    }
}
