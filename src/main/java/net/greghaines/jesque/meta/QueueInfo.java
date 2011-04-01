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
import java.util.List;

import net.greghaines.jesque.Job;

public class QueueInfo implements Comparable<QueueInfo>, Serializable
{
	private static final long serialVersionUID = 562750483276247591L;
	
	private String name;
	private Long size;
	private List<Job> jobs;
	
	public QueueInfo(){}
	
	public String getName()
	{
		return this.name;
	}

	public void setName(final String name)
	{
		this.name = name;
	}

	public Long getSize()
	{
		return this.size;
	}

	public void setSize(final Long size)
	{
		this.size = size;
	}

	public List<Job> getJobs()
	{
		return this.jobs;
	}

	public void setJobs(final List<Job> jobs)
	{
		this.jobs = jobs;
	}

	@Override
	public String toString()
	{
		return this.name;
	}

	public int compareTo(final QueueInfo other)
	{
		int retVal = 1;
		if (other != null)
		{
			if (this.name != null && other.name != null)
			{
				retVal = this.name.compareTo(other.name);
			}
			else if (this.name == null && other.name == null)
			{
				retVal = 0;
			}
			else if (this.name == null)
			{
				retVal = -1;
			}
		}
		return retVal;
	}
}
