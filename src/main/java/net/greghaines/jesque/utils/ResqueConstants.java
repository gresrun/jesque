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

/**
 * Constants used by Resque.
 * 
 * @author Greg Haines
 */
public interface ResqueConstants {
    
    /**
     * ISO-8601 compliant format
     */
    String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    /**
     * For interoperability with php-resque
     */
    String DATE_FORMAT_PHP = "EEE MMM dd HH:mm:ss z yyyy";
    /**
     * For interoperability with Resque (ruby)
     */
    String DATE_FORMAT_RUBY_V1 = "yyyy-MM-dd HH:mm:ss Z";
    /**
     * For interoperability with Resque (ruby)
     */
    String DATE_FORMAT_RUBY_V2 = "yyyy-MM-dd HH:mm:ss";
    /**
     * For interoperability with Resque (ruby)
     */
    String DATE_FORMAT_RUBY_V3 = "yyyy/MM/dd HH:mm:ss Z";
    /**
     * For interoperability with Resque (ruby)
     */
    String DATE_FORMAT_RUBY_V4 = "yyyy/MM/dd HH:mm:ss";

    String JAVA_DYNAMIC_QUEUES = "JAVA_DYNAMIC_QUEUES";

    String COLON = ":";
    String FAILED = "failed";
    String PROCESSED = "processed";
    String QUEUE = "queue";
    String QUEUES = "queues";
    String STARTED = "started";
    String STAT = "stat";
    String WORKER = "worker";
    String WORKERS = "workers";
    String CHANNEL = "channel";
    String INFLIGHT = "inflight";
    String FREQUENCY = "frequency";

    /**
     * Default channel for admin jobs
     */
    String ADMIN_CHANNEL = "admin";
}
