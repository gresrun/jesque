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
package net.greghaines.jesque.utils;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;

/**
 * Thrown when there is more than one matching Constructor is found.
 * 
 * @author Greg Haines
 */
public class AmbiguousConstructorException extends Exception {
    
    private static final long serialVersionUID = -5360734802682205116L;

    private final Class<?> type;
    private final Object[] args;
    private final Set<Constructor<?>> options;

    /**
     * Create a new AmbiguousConstructorException with only a message.
     * 
     * @param msg
     *            the detail message to show
     */
    public AmbiguousConstructorException(final String msg) {
        super(msg);
        this.type = null;
        this.args = null;
        this.options = null;
    }

    /**
     * Create a new AmbiguousConstructorException with the possible Constructor
     * options.
     * 
     * @param type
     *            the type of Object under construction
     * @param args
     *            the arguments given to match on
     * @param options
     *            the possible matching Constructors
     */
    public AmbiguousConstructorException(final Class<?> type, final Object[] args, final Set<Constructor<?>> options) {
        super("Found " + options.size() + " possible matches for class=" + type.getName() 
                + " args=" + Arrays.toString(args) + ": " + options);
        this.type = type;
        this.args = args.clone();
        this.options = options;
    }

    /**
     * @return the Class object searched
     */
    public Class<?> getType() {
        return this.type;
    }

    /**
     * @return the arguments that the Constructor needed to match
     */
    public Object[] getArgs() {
        return this.args;
    }

    /**
     * @return the possible Constructors that matched
     */
    public Set<Constructor<?>> getOptions() {
        return this.options;
    }
}
