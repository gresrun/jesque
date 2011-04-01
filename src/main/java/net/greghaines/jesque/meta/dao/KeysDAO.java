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

public interface KeysDAO
{	
	KeyInfo getKeyInfo(String key);
	
	KeyInfo getKeyInfo(String key, int offset, int count);
	
	List<KeyInfo> getKeyInfos();
	
	Map<String,String> getRedisInfo();
}
