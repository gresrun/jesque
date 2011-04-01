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
package net.greghaines.jesque.meta;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import net.greghaines.jesque.WorkerStatus;

public class WorkerInfo implements Comparable<WorkerInfo>, Serializable
{
	private static final long serialVersionUID = 7780544212376833441L;

	public enum State
	{
		IDLE,
		WORKING;
	}

	private String name;
	private State state;
	private Date started;
	private Long processed;
	private Long failed;
	private String host;
	private String pid;
	private List<String> queues;
	private WorkerStatus status;
	
	public WorkerInfo(){}
	
	public State getState()
	{
		return this.state;
	}

	public void setState(final State state)
	{
		this.state = state;
	}

	public String getName()
	{
		return this.name;
	}

	public void setName(final String name)
	{
		this.name = name;
	}

	public Date getStarted()
	{
		return this.started;
	}

	public void setStarted(final Date started)
	{
		this.started = started;
	}

	public Long getProcessed()
	{
		return this.processed;
	}

	public void setProcessed(final Long processed)
	{
		this.processed = processed;
	}

	public Long getFailed()
	{
		return this.failed;
	}

	public void setFailed(final Long failed)
	{
		this.failed = failed;
	}

	public String getHost()
	{
		return this.host;
	}

	public void setHost(final String host)
	{
		this.host = host;
	}

	public String getPid()
	{
		return this.pid;
	}

	public void setPid(final String pid)
	{
		this.pid = pid;
	}

	public List<String> getQueues()
	{
		return this.queues;
	}

	public void setQueues(final List<String> queues)
	{
		this.queues = queues;
	}

	public WorkerStatus getStatus()
	{
		return this.status;
	}

	public void setStatus(final WorkerStatus status)
	{
		this.status = status;
	}

	@Override
	public String toString()
	{
		return this.name;
	}

	public int compareTo(final WorkerInfo other)
	{
		int retVal = 1;
		if (other != null)
		{
			if (this.status != null && other.status != null)
			{
				if (this.status.getRunAt() != null && other.status.getRunAt() != null)
				{
					retVal = this.status.getRunAt().compareTo(other.status.getRunAt());
				}
				else if (this.status.getRunAt() == null && other.status.getRunAt() == null)
				{
					retVal = 0;
				}
				else if (this.status.getRunAt() == null)
				{
					retVal = -1;
				}
			}
			else if (this.status == null && other.status == null)
			{
				retVal = 0;
			}
			else if (this.status == null)
			{
				retVal = -1;
			}
		}
		return retVal;
	}
}
