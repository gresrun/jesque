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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Creates a fixed number of identical <code>Workers</code>, each on a 
 * separate <code>Thread</code>.
 * 
 * @author Greg Haines
 */
public class WorkerPool implements Worker
{
	private final List<Worker> workers;
	private final List<Thread> threads;

	/**
	 * Create a WorkerPool with the given number of Workers and the
	 * default <code>ThreadFactory</code>.
	 * 
	 * @param workerFactory a Callable that returns an implementation of Worker
	 * @param numWorkers the number of Workers to create
	 */
	public WorkerPool(final Callable<? extends Worker> workerFactory, 
			final int numWorkers)
	{
		this(workerFactory, numWorkers, Executors.defaultThreadFactory());
	}

	/**
	 * Create a WorkerPool with the given number of Workers and the
	 * given <code>ThreadFactory</code>.
	 * 
	 * @param workerFactory a Callable that returns an implementation of Worker
	 * @param numWorkers the number of Workers to create
	 * @param threadFactory the factory to create pre-configured Threads
	 */
	public WorkerPool(final Callable<? extends Worker> workerFactory, 
			final int numWorkers, final ThreadFactory threadFactory)
	{
		this.workers = new ArrayList<Worker>(numWorkers);
		this.threads = new ArrayList<Thread>(numWorkers);
		for (int i = 0; i < numWorkers; i++)
		{
			try
			{
				final Worker worker = workerFactory.call();
				this.workers.add(worker);
				this.threads.add(threadFactory.newThread(worker));
			}
			catch (RuntimeException re)
			{
				throw re;
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Shutdown this pool and wait untill all threads are finished.
	 * 
	 * @param now if true, an effort will be made to stop any jobs in progress
	 * @throws InterruptedException
	 */
	public void endAndJoin(final boolean now)
	throws InterruptedException
	{
		end(now);
		for (final Thread thread : this.threads)
		{
			while (thread.isAlive())
			{
				thread.join();
			}
		}
	}

	/**
	 * Wait untill all threads are finished.
	 * 
	 * @throws InterruptedException
	 */
	public void join()
	throws InterruptedException
	{
		for (final Thread thread : this.threads)
		{
			while (thread.isAlive())
			{
				thread.join();
			}
		}
	}

	public String getName()
	{
		final StringBuilder sb = new StringBuilder(128 * this.threads.size());
		String prefix = "";
		for (final Worker worker : this.workers)
		{
			sb.append(prefix).append(worker.getName());
			prefix = " | ";
		}
		return sb.toString();
	}
	
	public void run()
	{
		for (final Thread thread : this.threads)
		{
			thread.start();
		}
		Thread.yield();
	}

	public void addListener(final WorkerListener listener)
	{
		for (final Worker worker : this.workers)
		{
			worker.addListener(listener);
		}
	}

	public void addListener(final WorkerListener listener, final WorkerEvent... events)
	{
		for (final Worker worker : this.workers)
		{
			worker.addListener(listener, events);
		}
	}

	public void removeListener(final WorkerListener listener)
	{
		for (final Worker worker : this.workers)
		{
			worker.removeListener(listener);
		}
	}

	public void removeListener(final WorkerListener listener, final WorkerEvent... events)
	{
		for (final Worker worker : this.workers)
		{
			worker.removeListener(listener, events);
		}
	}

	public void removeAllListeners()
	{
		for (final Worker worker : this.workers)
		{
			worker.removeAllListeners();
		}
	}

	public void removeAllListeners(final WorkerEvent... events)
	{
		for (final Worker worker : this.workers)
		{
			worker.removeAllListeners(events);
		}
	}

	public void end(final boolean now)
	{
		for (final Worker worker : this.workers)
		{
			worker.end(now);
		}
	}
	
	public boolean isShutdown()
	{
		return this.workers.get(0).isShutdown();
	}

	public boolean isPaused()
	{
		return this.workers.get(0).isPaused();
	}

	public void togglePause(final boolean paused)
	{
		for (final Worker worker : this.workers)
		{
			worker.togglePause(paused);
		}
	}

	public Collection<String> getQueues()
	{
		return this.workers.get(0).getQueues();
	}

	public void addQueue(final String queueName)
	{
		for (final Worker worker : this.workers)
		{
			worker.addQueue(queueName);
		}
	}

	public void removeQueue(final String queueName, final boolean all)
	{
		for (final Worker worker : this.workers)
		{
			worker.removeQueue(queueName, all);
		}
	}

	public void removeAllQueues()
	{
		for (final Worker worker : this.workers)
		{
			worker.removeAllQueues();
		}
	}

	public void setQueues(final Collection<String> queues)
	{
		for (final Worker worker : this.workers)
		{
			worker.setQueues(queues);
		}
	}

	public Set<Class<?>> getJobTypes()
	{
		return this.workers.get(0).getJobTypes();
	}

	public void addJobType(final Class<?> jobType)
	{
		for (final Worker worker : this.workers)
		{
			worker.addJobType(jobType);
		}
	}

	public void removeJobType(final Class<?> jobType)
	{
		for (final Worker worker : this.workers)
		{
			worker.removeJobType(jobType);
		}
	}

	public void setJobTypes(final Collection<? extends Class<?>> jobTypes)
	{
		for (final Worker worker : this.workers)
		{
			worker.setJobTypes(jobTypes);
		}
	}

	public WorkerExceptionHandler getExceptionHandler()
	{
		return this.workers.get(0).getExceptionHandler();
	}

	public void setExceptionHandler(final WorkerExceptionHandler exceptionHandler)
	{
		for (final Worker worker : this.workers)
		{
			worker.setExceptionHandler(exceptionHandler);
		}
	}
}
