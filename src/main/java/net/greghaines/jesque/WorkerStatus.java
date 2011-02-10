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
package net.greghaines.jesque;

import java.io.Serializable;
import java.util.Date;

/**
 * A bean to hold information about the status of a Worker.
 * 
 * @author Greg Haines
 */
public class WorkerStatus implements Serializable
{
	private static final long serialVersionUID = 1852915628988733048L;
	
	private Date runAt;
	private String queue;
	private Job payload;
	
	/**
	 * No-arg constructor
	 */
	public WorkerStatus(){}
	
	/**
	 * Cloning constructor.
	 * 
	 * @param origStatus the status to start from
	 * @throws IllegalArgumentException if the origStatus is null
	 */
	public WorkerStatus(final WorkerStatus origStatus)
	{
		if (origStatus == null)
		{
			throw new IllegalArgumentException("origStatus must not be null");
		}
		this.runAt = origStatus.runAt;
		this.queue = origStatus.queue;
		this.payload = origStatus.payload;
	}

	/**
	 * @return when the Worker started on the current job
	 */
	public Date getRunAt()
	{
		return this.runAt;
	}

	/**
	 * Set when the Worker started on the current job.
	 * @param runAt when the Worker started on the current job
	 */
	public void setRunAt(final Date runAt)
	{
		this.runAt = runAt;
	}

	/**
	 * @return which queue the current job came from
	 */
	public String getQueue()
	{
		return this.queue;
	}

	/**
	 * Set which queue the current job came from.
	 * @param queue which queue the current job came from
	 */
	public void setQueue(final String queue)
	{
		this.queue = queue;
	}

	/**
	 * @return the job
	 */
	public Job getPayload()
	{
		return this.payload;
	}

	/**
	 * Set the job.
	 * @param payload the job
	 */
	public void setPayload(final Job payload)
	{
		this.payload = payload;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.payload == null) ? 0 : this.payload.hashCode());
		result = prime * result + ((this.queue == null) ? 0 : this.queue.hashCode());
		result = prime * result + ((this.runAt == null) ? 0 : this.runAt.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final WorkerStatus other = (WorkerStatus) obj;
		if (this.queue == null)
		{
			if (other.queue != null)
			{
				return false;
			}
		}
		else if (!this.queue.equals(other.queue))
		{
			return false;
		}
		if (this.runAt == null)
		{
			if (other.runAt != null)
			{
				return false;
			}
		}
		else if (!this.runAt.equals(other.runAt))
		{
			return false;
		}
		if (this.payload == null)
		{
			if (other.payload != null)
			{
				return false;
			
			}
		}
		else if (!this.payload.equals(other.payload))
		{
			return false;
		}
		return true;
	}
}
