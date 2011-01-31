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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.greghaines.jesque.Job;

/**
 * This class keeps track of WorkerListeners and notifies each listener when fireEvent() is invoked.
 * 
 * @author Greg Haines
 */
public class WorkerListenerDelegate implements WorkerEventEmitter
{	
	private static final Logger log = LoggerFactory.getLogger(WorkerListenerDelegate.class);
	
	private final Map<WorkerEvent,Map<WorkerListener,WorkerEvent>> eventListenerMap;
	
	/**
	 * Create a new WorkerListenerDelegate.
	 */
	public WorkerListenerDelegate()
	{
		final Map<WorkerEvent,Map<WorkerListener,WorkerEvent>> elp = 
			new HashMap<WorkerEvent,Map<WorkerListener,WorkerEvent>>(WorkerEvent.values().length);
		for (final WorkerEvent event : WorkerEvent.values())
		{
			elp.put(event, Collections.synchronizedMap(new WeakHashMap<WorkerListener,WorkerEvent>()));
		}
		this.eventListenerMap = Collections.unmodifiableMap(elp);
	}
	
	public void addListener(final WorkerListener listener)
	{
		addListener(listener, WorkerEvent.values());
	}
	
	public void addListener(final WorkerListener listener, final WorkerEvent... events)
	{
		if (listener != null)
		{
			for (final WorkerEvent event : events)
			{
				final Map<WorkerListener,WorkerEvent> lMap = this.eventListenerMap.get(event);
				if (lMap != null)
				{
					lMap.put(listener, event);
				}
			}
		}
	}
	
	public void removeListener(final WorkerListener listener)
	{
		removeListener(listener, WorkerEvent.values());
	}
	
	public void removeListener(final WorkerListener listener, final WorkerEvent... events)
	{
		if (listener != null)
		{
			for (final WorkerEvent event : events)
			{
				final Map<WorkerListener,WorkerEvent> lMap = this.eventListenerMap.get(event);
				if (lMap != null)
				{
					lMap.remove(listener);
				}
			}
		}
	}
	
	public void removeAllListeners()
	{
		removeAllListeners(WorkerEvent.values());
	}
	
	public void removeAllListeners(final WorkerEvent... events)
	{
		for (final WorkerEvent event : events)
		{
			final Map<WorkerListener,WorkerEvent> lMap = this.eventListenerMap.get(event);
			if (lMap != null)
			{
				lMap.clear();
			}
		}
	}
	
	/**
	 * Notify all WorkerListeners currently registered for the given WorkerEvent.
	 * 
	 * @param event the WorkerEvent that occured
	 * @param worker the Worker that the event occured in
	 * @param queue the queue the Worker is processing
	 * @param job the Job related to the event (only supply for JOB_PROCESS, JOB_EXECUTE, JOB_SUCCESS, and JOB_FAILURE events)
	 * @param runner the materialized object that the Job specified (only supply for JOB_EXECUTE and JOB_SUCCESS events)
	 * @param result the result of the successful execution of the Job (only set for JOB_SUCCESS and if the Job was a Callable that returned a value)
	 * @param ex the Exception that caused the event (only supply for JOB_FAILURE and ERROR events)
	 */
	public void fireEvent(final WorkerEvent event, final Worker worker, final String queue, 
			final Job job, final Object runner, final Object result, final Exception ex)
	{
		final Map<WorkerListener,WorkerEvent> lMap = this.eventListenerMap.get(event);
		if (lMap != null)
		{
			final Set<WorkerListener> listeners = new HashSet<WorkerListener>(lMap.size());
			synchronized (lMap)
			{ // Still have to synch here because synchronizedMap()'s iterators aren't thread-safe
				listeners.addAll(lMap.keySet());
			}
			for (final WorkerListener listener : listeners)
			{
				if (listener != null)
				{
					try
					{
						listener.onEvent(event, worker, queue, job, runner, result, ex);
					}
					catch (Exception e)
					{
						log.error("Failure executing listener " + listener + " for event " + event + 
							" from queue " + queue + " on worker " + worker, e);
					}
				}
			}
		}
	}
}
