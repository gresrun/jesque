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
import java.util.Collections;
import java.util.Map;

/**
 * A Worker polls for Jobs from a specified list of queues, executing 
 * them in sequence and notifying WorkerListeners in the process.
 * <p/>
 * Workers are designed to be run in a Thread or an ExecutorService. E.g.:
 * <pre>
 * final Worker worker = new WorkerImpl(config, Worker.ALL_QUEUES, 
 * 		Arrays.asList("TestAction"));
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
	 * Special value to tell a Worker to poll all currently available queues.
	 */
	Collection<String> ALL_QUEUES = Collections.unmodifiableList(Arrays.asList("*"));

	/**
	 * Returns the name of this Worker.
	 * 
	 * @return the name of this Worker
	 */
	String getName();

	/**
	 * Shutdown this Worker.
	 * 
	 * @param now if true, an effort will be made to stop any job in progress
	 */
	void end(boolean now);

	/**
	 * Returns whether this worker is either shutdown or in the process of shutting down.
	 * 
	 * @return whether this worker is either shutdown or in the process of shutting down.
	 */
	boolean isShutdown();

	/**
	 * Returns whether this worker is paused.
	 * 
	 * @return whether this worker is paused. If true, the worker is not processing any new jobs; 
	 * if false, the worker is processing new jobs
	 */
	boolean isPaused();

	/**
	 * Toggle whether this worker will process any new jobs.
	 * 
	 * @param paused if true, the worker will not process any new jobs; 
	 * if false, the worker will process new jobs
	 */
	void togglePause(boolean paused);

	/**
	 * The queues that this Worker will poll.
	 * 
	 * @return an unmodifiable view of the queues to be polled
	 */
	Collection<String> getQueues();

	/**
	 * Poll the given queue. If the queue exists multiple times, 
	 * it will be checked that many times per loop. This allows for a 
	 * queue to be given higher priority by checking it more often.
	 * 
	 * @param queueName the name of the queue to poll
	 */
	void addQueue(String queueName);

	/**
	 * Stop polling the given queue. If the <code>all</code> argument is 
	 * true, all instances of the queue will be removed, otherwise, only 
	 * one instance is removed.
	 * 
	 * @param queueName the queue to stop polling
	 * @param all whether to remove all or only one of the instances
	 */
	void removeQueue(String queueName, boolean all);

	/**
	 * Stop polling all queues.
	 */
	void removeAllQueues();

	/**
	 * Clear any current queues and poll the given queues.
	 * 
	 * @param queues the queues to poll
	 */
	void setQueues(Collection<String> queues);

	/**
	 * The allowed job types that this Worker will execute.
	 * 
	 * @return an unmodifiable view of the allowed job types
	 */
	Map<String,Class<?>> getJobTypes();

	/**
	 * Allow the given job type to be executed.
	 * 
	 * @param jobName the job name as seen
	 * @param jobType the job type to allow
	 */
	void addJobType(String jobName, Class<?> jobType);

	/**
	 * Disallow the job type from being executed.
	 * 
	 * @param jobType the job type to disallow
	 */
	void removeJobType(Class<?> jobType);
	
	/**
	 * Disallow the job name from being executed.
	 * 
	 * @param jobName the job name to disallow
	 */
	void removeJobName(String jobName);

	/**
	 * Clear any current allowed job types and use the given set.
	 * 
	 * @param jobTypes the job types to allow
	 */
	void setJobTypes(Map<String,? extends Class<?>> jobTypes);
	
	/**
	 * The current exception handler.
	 * 
	 * @return the current exception handler.
	 */
	WorkerExceptionHandler getExceptionHandler();
	
	/**
	 * Set this Worker's exception handler to the given handler.
	 * 
	 * @param exceptionHandler the exception handler to use
	 */
	void setExceptionHandler(WorkerExceptionHandler exceptionHandler);
}
