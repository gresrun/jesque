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

/**
 * A WorkerEventEmitter allows WorkerListeners to register for WorkerEvents.
 */
public interface WorkerEventEmitter {
    
    /**
     * Register a WorkerListener for all WorkerEvents. 
     * @param listener the WorkerListener to register
     */
    void addListener(WorkerListener listener);

    /**
     * Register a WorkerListener for the specified WorkerEvents.
     * @param listener the WorkerListener to register
     * @param events the WorkerEvents to be notified of
     */
    void addListener(WorkerListener listener, WorkerEvent... events);

    /**
     * Unregister a WorkerListener for all WorkerEvents.
     * @param listener the WorkerListener to unregister
     */
    void removeListener(WorkerListener listener);

    /**
     * Unregister a WorkerListener for the specified WorkerEvents.
     * @param listener the WorkerListener to unregister
     * @param events the WorkerEvents to no longer be notified of
     */
    void removeListener(WorkerListener listener, WorkerEvent... events);

    /**
     * Unregister all WorkerListeners for all WorkerEvents.
     */
    void removeAllListeners();

    /**
     * Unregister all WorkerListeners for the specified WorkerEvents.
     * @param events the WorkerEvents to no longer be notified of
     */
    void removeAllListeners(WorkerEvent... events);
}
