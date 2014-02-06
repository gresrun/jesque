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
import java.util.List;
import java.util.regex.Pattern;

import net.greghaines.jesque.utils.JesqueUtils;

/**
 * Information about a key in Redis.
 * 
 * @author Greg Haines
 */
public class KeyInfo implements Comparable<KeyInfo>, Serializable {
    
    private static final long serialVersionUID = 6243902746964006352L;
    private static final Pattern COLON_PATTERN = Pattern.compile(":");

    private String name;
    private String namespace;
    private KeyType type;
    private Long size;
    private List<String> arrayValue;

    /**
     * No-argument constructor.
     */
    public KeyInfo() {
        // Do nothing
    }

    /**
     * Constructor.
     * @param fullKey the full name of the key including the namespace
     * @param type the type of the key value
     */
    public KeyInfo(final String fullKey, final KeyType type) {
        if (fullKey == null) {
            throw new IllegalArgumentException("fullKey must not be null");
        }
        final String[] keyParts = COLON_PATTERN.split(fullKey, 2);
        if (keyParts.length != 2) {
            throw new IllegalArgumentException("Malformed fullKey: " + fullKey);
        }
        this.namespace = keyParts[0];
        this.name = keyParts[1];
        this.type = type;
    }

    /**
     * @return the name of the key
     */
    public String getName() {
        return this.name;
    }

    /**
     * @param name the name of the key
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return the namespace of the key
     */
    public String getNamespace() {
        return this.namespace;
    }

    /**
     * @param namespace the namespace of the key
     */
    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    /**
     * @return the type of the key value
     */
    public KeyType getType() {
        return this.type;
    }

    /**
     * @param type the type of the key value
     */
    public void setType(final KeyType type) {
        this.type = type;
    }

    /**
     * @return the size of the key value
     */
    public Long getSize() {
        return this.size;
    }

    /**
     * @param size the size of the key value
     */
    public void setSize(final Long size) {
        this.size = size;
    }

    /**
     * @return the value as an array
     */
    public List<String> getArrayValue() {
        return this.arrayValue;
    }

    /**
     * @param arrayValue the value as an array
     */
    public void setArrayValue(final List<String> arrayValue) {
        this.arrayValue = arrayValue;
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
        result = prime * result + ((this.arrayValue == null) ? 0 : this.arrayValue.hashCode());
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime * result + ((this.namespace == null) ? 0 : this.namespace.hashCode());
        result = prime * result + ((this.size == null) ? 0 : this.size.hashCode());
        result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
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
        } else if (obj instanceof KeyInfo) {
            final KeyInfo other = (KeyInfo) obj;
            equal = (JesqueUtils.nullSafeEquals(this.arrayValue, other.arrayValue)
                    && JesqueUtils.nullSafeEquals(this.name, other.name)
                    && JesqueUtils.nullSafeEquals(this.namespace, other.namespace)
                    && JesqueUtils.nullSafeEquals(this.size, other.size)
                    && JesqueUtils.nullSafeEquals(this.type, other.type));
        }
        return equal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(final KeyInfo other) {
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
