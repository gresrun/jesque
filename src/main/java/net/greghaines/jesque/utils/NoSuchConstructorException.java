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

import java.util.Arrays;

/**
 * Thrown when the specified constructor could not be found.
 * 
 * @author Greg Haines
 */
public class NoSuchConstructorException extends Exception {
    
    private static final long serialVersionUID = 4960599901666926591L;

    private final Class<?> type;
    private final Object[] args;

    /**
     * Create a new NoSuchConstructorException with only a message.
     * 
     * @param msg
     *            the detail message to show
     */
    public NoSuchConstructorException(final String msg) {
        super(msg);
        this.type = null;
        this.args = null;
    }

    /**
     * Create a new NoSuchConstructorException with the type and arguments.
     * 
     * @param type
     *            the type of Object under construction
     * @param args
     *            the arguments given to match on
     */
    public NoSuchConstructorException(final Class<?> type, final Object... args) {
        super("class=" + type.getName() + " args=" + Arrays.toString(args));
        this.type = type;
        this.args = args;
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
}
