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

/**
 * ExceptionHandler allows for customized handling of exceptions received by a {@link Worker}.
 */
public interface ExceptionHandler {

    /**
     * Called when a worker encounters an exception.
     * @param jobExecutor the worker that encountered the exception
     * @param exception the exception
     * @param curQueue the current queue being processed
     * @return the {@link RecoveryStrategy} to use
     */
    RecoveryStrategy onException(JobExecutor jobExecutor, Exception exception, String curQueue);
}
