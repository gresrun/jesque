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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * KeyTypes are the types of Redis keys that Jesque uses.
 * 
 * @author Greg Haines
 */
public enum KeyType {
    
    HASH("hash"), 
    LIST("list"), 
    NONE("none"), 
    SET("set"), 
    STRING("string"), 
    ZSET("zset");

    private final String val;

    private KeyType(final String val) {
        this.val = val;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.val;
    }

    private static final Map<String, KeyType> valTypeMap;

    static {
        final Map<String, KeyType> vtm = new HashMap<String, KeyType>();
        for (final KeyType keyType : KeyType.values()) {
            vtm.put(keyType.toString(), keyType);
        }
        valTypeMap = Collections.unmodifiableMap(vtm);
    }

    /**
     * @param val the Redis type name
     * @return the KeyType for the given type name or null if the type is unknown
     */
    public static KeyType getKeyTypeByValue(final String val) {
        return valTypeMap.get(val);
    }
}
