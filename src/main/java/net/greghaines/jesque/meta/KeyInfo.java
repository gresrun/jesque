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

/**
 * Encapsulates information about a key in Redis.
 * 
 * @author Greg Haines
 */
public class KeyInfo implements Comparable<KeyInfo>, Serializable {
    
    private static final long serialVersionUID = 6243902746964006352L;
    private static final Pattern colonPattern = Pattern.compile(":");

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

    public KeyInfo(final String fullKey, final KeyType type) {
        if (fullKey == null) {
            throw new IllegalArgumentException("fullKey must not be null");
        }
        final String[] keyParts = colonPattern.split(fullKey, 2);
        if (keyParts.length != 2) {
            throw new IllegalArgumentException("Malformed fullKey: " + fullKey);
        }
        this.namespace = keyParts[0];
        this.name = keyParts[1];
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    public KeyType getType() {
        return this.type;
    }

    public void setType(final KeyType type) {
        this.type = type;
    }

    public Long getSize() {
        return this.size;
    }

    public void setSize(final Long size) {
        this.size = size;
    }

    public List<String> getArrayValue() {
        return this.arrayValue;
    }

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
