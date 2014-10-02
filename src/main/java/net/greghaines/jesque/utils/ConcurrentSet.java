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

import java.util.Set;

/**
 * An interface that denotes the implementation of Set is thread-safe.<br>
 * It adds no new methods.
 * 
 * @author Greg Haines
 * 
 * @param <E>
 *            the type of elements maintained by this set
 */
public interface ConcurrentSet<E> extends Set<E> {
}
