/*
 * Copyright 2015 Greg Haines
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

import static net.greghaines.jesque.utils.ResqueConstants.FAILED;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * DefaultFailQueueStrategy puts all jobs in the standard Redis failure queue
 * and imposes no limits on the max number of failed items to keep.
 */
public class DefaultFailQueueStrategy implements FailQueueStrategy {

    private final String namespace;

    /**
     * Constructor.
     * @param namespace the Redis namespace.
     */
    public DefaultFailQueueStrategy(final String namespace) {
        this.namespace = namespace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFailQueueKey(final Throwable thrwbl, final Job job, final String curQueue) {
        return JesqueUtils.createKey(this.namespace, FAILED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFailQueueMaxItems(String curQueue) {
        return 0;
    }
}
