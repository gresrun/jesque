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
package net.greghaines.jesque.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoggingWorkerListener logs every event that it is registered for.
 */
public class LoggingWorkerListener implements WorkerListener {

    public static final LoggingWorkerListener INSTANCE = new LoggingWorkerListener();

    private static final Logger LOG = LoggerFactory.getLogger(LoggingWorkerListener.class);

    private LoggingWorkerListener() {
        // Singleton
    }

    /**
     * {@inheritDoc}
     * If there is a Throwable, it is logged as an error, otherwise it is logged as a debug message.
     */
    @Override
    public void onEvent(final WorkerEvent event, final Worker worker, final String queue,
            final net.greghaines.jesque.Job job, final Object runner, final Object result, final Throwable t) {
        if (t == null) {
            LOG.debug("{} {} {} {} {} {} {}", new Object[]{ event, worker, queue, job, runner, result, t });
        } else {
            LOG.error(event + " " + worker + " " + queue + " " + job + " " + runner + " " + result, t);
        }
    }
}
