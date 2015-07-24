/*
 * Copyright 2012 Greg Haines
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

import static net.greghaines.jesque.worker.RecoveryStrategy.PROCEED;
import static net.greghaines.jesque.worker.RecoveryStrategy.RECONNECT;
import static net.greghaines.jesque.worker.RecoveryStrategy.TERMINATE;

import com.fasterxml.jackson.core.JsonProcessingException;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * DefaultExceptionHandler reconnects if there is a connection exception, proceeds if the exception was 
 * JSON-related or a thread interrupt and terminates if the executor is shutdown.
 */
public class DefaultExceptionHandler implements ExceptionHandler {
    
    /**
     * {@inheritDoc}
     */
    @Override
	public RecoveryStrategy onException(final JobExecutor jobExecutor, final Exception exception, 
	        final String curQueue) {
		return (exception instanceof JedisConnectionException)
			? RECONNECT 
			: (((exception instanceof JsonProcessingException) || ((exception instanceof InterruptedException) 
			        && !jobExecutor.isShutdown()))
				? PROCEED
				: TERMINATE);
	}
}
