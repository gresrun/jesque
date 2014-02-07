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
package net.greghaines.jesque.meta.dao;

import java.util.List;
import java.util.Map;

import net.greghaines.jesque.meta.KeyInfo;

/**
 * KeysDAO provides access to available keys.
 * 
 * @author Greg Haines
 */
public interface KeysDAO {
    
    /**
     * Get basic key info.
     * @param key the key name
     * @return the key information or null if the key did not exist
     */
    KeyInfo getKeyInfo(String key);

    /**
     * Get basic key info plus a sub-list of the array value for the key, if applicable.
     * @param key the key name
     * @param offset the offset into the array
     * @param count the number of values to return
     * @return the key information or null if the key did not exist
     */
    KeyInfo getKeyInfo(String key, int offset, int count);

    /**
     * Get basic info on all keys.
     * @return a list of key informations
     */
    List<KeyInfo> getKeyInfos();

    /**
     * @return information about the backing Redis database
     */
    Map<String, String> getRedisInfo();
}
