/*
 * Copyright 2014 Greg Haines
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
package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * ReflectiveJobFactory assumes job names are fully-qualified class names.
 */
public class ReflectiveJobFactory implements JobFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public Object materializeJob(final Job job) throws Exception {
        return JesqueUtils.materializeJob(job);
    }
}
