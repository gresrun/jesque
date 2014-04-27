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
public final class ResqueConstants {
    
    private ResqueConstants() {
        // restrict instantiation
    }
    
    /**
     * ISO-8601 compliant format
     */
    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    /**
     * For interoperability with php-resque
     */
    public static final String DATE_FORMAT_PHP = "EEE MMM dd HH:mm:ss z yyyy";
    /**
     * For interoperability with Resque (ruby)
     */
    public static final String DATE_FORMAT_RUBY_V1 = "yyyy-MM-dd HH:mm:ss Z";
    /**
     * For interoperability with Resque (ruby)
     */
    public static final String DATE_FORMAT_RUBY_V2 = "yyyy-MM-dd HH:mm:ss";
    /**
     * For interoperability with Resque (ruby)
     */
    public static final String DATE_FORMAT_RUBY_V3 = "yyyy/MM/dd HH:mm:ss Z";
    /**
     * For interoperability with Resque (ruby)
     */
    public static final String DATE_FORMAT_RUBY_V4 = "yyyy/MM/dd HH:mm:ss";

    public static final String JAVA_DYNAMIC_QUEUES = "JAVA_DYNAMIC_QUEUES";
    public static final String FAILOVER_QUEUES = "FAILOVER_QUEUES";

    public static final String COLON = ":";
    public static final String FAILED = "failed";
    public static final String PROCESSED = "processed";
    public static final String QUEUE = "queue";
    public static final String QUEUES = "queues";
    public static final String FOQUEUES = "FOqueues";
    public static final String STARTED = "started";
    public static final String STAT = "stat";
    public static final String WORKER = "worker";
    public static final String WORKERS = "workers";
    public static final String CHANNEL = "channel";
    public static final String INFLIGHT = "inflight";
    public static final String FAILOVER = "failover";    
    
    public static final String LPOP_FO = "lpop_fo";
    public static final String HEARTBEAT_FO = "heartbeat_fo";
    public static final String CLEANUP_FO = "cleanup_fo";
    public static final String FAILBACK = "reaper_fo";

    public static final String LPOP_FO_SCRIPT = new StringBuilderWithNewline()
            .appendLine("local queue = KEYS[1]")
            .appendLine("local foqueue = KEYS[2]")
            .appendLine("local foworker = KEYS[3]")
            .appendLine("local unixtime = KEYS[4]")
            .appendLine("local job = redis.call('LPOP', queue)")
            .appendLine("if job == false then\n  return nil;\nend")
            .appendLine("redis.call('ZADD', foqueue, unixtime, foworker)")
            .appendLine("redis.call('SET', foworker, job)")
            .appendLine("return job")
            .toString();

    public static final String CLEANUP_FO_SCRIPT = new StringBuilderWithNewline()
            .appendLine("local foqueue = KEYS[1]")
            .appendLine("local foworker = KEYS[2]")
            .appendLine("redis.call('ZREM', foqueue, foworker)")
            .appendLine("redis.call('DEL', foworker)")
            .toString();
    
    public static final String HEARTBEAT_FO_SCRIPT = new StringBuilderWithNewline()
            .appendLine("local foqueue = KEYS[1]")
            .appendLine("local foworker = KEYS[2]")
            .appendLine("local unixtime = KEYS[3]")
            .appendLine("local rank = redis.call('ZRANK', foqueue, foworker)")
            .appendLine("if rank == false then")
            .appendLine("  return nil;")
            .appendLine("else")
            .appendLine("  redis.call('ZADD', foqueue, unixtime, foworker)")
            .appendLine("  return 0;")
            .appendLine("end")
            .toString();
    
    public static final String FAILBACK_SCRIPT = new StringBuilderWithNewline()
            .appendLine("local foqueue = KEYS[1]")
            .appendLine("local unixtime = KEYS[2]")
            .appendLine("local foworker = redis.call('ZRANGEBYSCORE', foqueue, -1, unixtime, 'LIMIT', 0, 1)[1]")
            .appendLine("if foworker == nil then return nil; end")
            .appendLine("local job = redis.call('GET', foworker)")
            .appendLine("if job == false then")
            .appendLine("  redis.call('RPUSH', '#emptyfodata', foworker)")
            .appendLine("  redis.call('ZREM', foqueue, foworker)")
            .appendLine("  return 1;")
            .appendLine("end")
            .appendLine("local workerIdx = foworker:match('()'..','..'.*')")
            .appendLine("if workerIdx == nil then")
            .appendLine("  redis.call('RPUSH', '#wrongfodata', job)")
            .appendLine("  redis.call('ZREM', foqueue, foworker)")
            .appendLine("  redis.call('DEL', foworker)")
            .appendLine("  return 2;")
            .appendLine("end")
            .appendLine("local postfix = string.sub(foworker, workerIdx+1, -1)")
            .appendLine("local queueIdx = postfix:match('()'..':'..'.*')")    
            .appendLine("if queueIdx == nil then")
            .appendLine("  redis.call('RPUSH', '#wrongfodata', job)")
            .appendLine("  redis.call('ZREM', foqueue, foworker)")
            .appendLine("  redis.call('DEL', foworker)")
            .appendLine("  return 2;")
            .appendLine("end")
            .appendLine("local queuename = string.sub(postfix, queueIdx+1, -1)")
            .appendLine("redis.call('RPUSH', queuename, job)")
            .appendLine("redis.call('ZREM', foqueue, foworker)")
            .appendLine("redis.call('DEL', foworker)")
            .appendLine("return 0")
            .toString();
    
    /**
     * Default channel for admin jobs
     */
    public static final String ADMIN_CHANNEL = "admin";
    
    static class StringBuilderWithNewline {
        private StringBuilder sb;

        public StringBuilderWithNewline() {
            sb = new StringBuilder();
        }

        public StringBuilderWithNewline appendLine(String str) {
            sb.append(str).append(System.lineSeparator());
            return this;
        }
        
        public StringBuilderWithNewline append(String str) {
            sb.append(str);
            return this;
        }

        public String toString() {
            return sb.toString();
        }
    }
}
