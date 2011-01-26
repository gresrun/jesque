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

import java.util.Arrays;
import java.util.Collection;

/**
 * A Worker polls for Jobs from a specified list of queues, executing them in sequence 
 * and notifying WorkerListeners in the process.
 * <p/>
 * Workers are designed to be run in a Thread or an ExecutorService. E.g.:
 * <pre>
 * final Worker worker = new WorkerImpl(config, Worker.ALL_QUEUES, Arrays.asList("TestAction"));
 * final Thread t = new Thread(worker);
 * t.start();
 * Thread.yield();
 * ...
 * worker.end();
 * t.join();
 * </pre>
 * 
 * @author Greg Haines
 */
public interface Worker extends Runnable, WorkerEventEmitter
{
	/**
	 * Special value to tell a Worker to listen to all currently available queues.
	 */
	Collection<String> ALL_QUEUES = Arrays.asList("*");
	
	/**
	 * Shutdown this Worker.
	 */
	void end();
	
	/**
	 * Returns the name of this Worker.
	 * 
	 * @return the name of this Worker
	 */
	String getName();
}
