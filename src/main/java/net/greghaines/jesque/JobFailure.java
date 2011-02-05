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
import java.util.List;

/**
 * A bean to hold information about a job that failed.
 * 
 * @author Greg Haines
 */
public class JobFailure implements Serializable
{
	private static final long serialVersionUID = -2160045729341301316L;
	
	private String worker;
	private Job payload;
	private String exception;
	private String error;
	private List<String> backtrace;
	private Date failedAt;
	
	public JobFailure(){}

	/**
	 * @return the name of the worker where the job failed
	 */
	public String getWorker()
	{
		return this.worker;
	}

	/**
	 * Set the name of the worker where the job failed.
	 * @param worker the name of the worker
	 */
	public void setWorker(final String worker)
	{
		this.worker = worker;
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

	/**
	 * @return the kind of exception that occured
	 */
	public String getException()
	{
		return this.exception;
	}

	/**
	 * Set the kind of exception that occured.
	 * @param exception the kind of exception that occured
	 */
	public void setException(final String exception)
	{
		this.exception = exception;
	}

	/**
	 * @return the message given by the exception
	 */
	public String getError()
	{
		return this.error;
	}

	/**
	 * Set the message given by the exception.
	 * @param error the message given by the exception
	 */
	public void setError(String error)
	{
		this.error = error;
	}

	/**
	 * @return the stacktrace of the exception
	 */
	public List<String> getBacktrace()
	{
		return this.backtrace;
	}

	/**
	 * Set the stacktrace of the exception.
	 * @param backtrace the stacktrace of the exception
	 */
	public void setBacktrace(final List<String> backtrace)
	{
		this.backtrace = backtrace;
	}

	/**
	 * @return when the error occurred
	 */
	public Date getFailedAt()
	{
		return this.failedAt;
	}

	/**
	 * Set when the error occurred.
	 * @param failedAt when the error occurred
	 */
	public void setFailedAt(final Date failedAt)
	{
		this.failedAt = failedAt;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.backtrace == null) ? 0 : this.backtrace.hashCode());
		result = prime * result + ((this.error == null) ? 0 : this.error.hashCode());
		result = prime * result + ((this.exception == null) ? 0 : this.exception.hashCode());
		result = prime * result + ((this.failedAt == null) ? 0 : this.failedAt.hashCode());
		result = prime * result + ((this.payload == null) ? 0 : this.payload.hashCode());
		result = prime * result + ((this.worker == null) ? 0 : this.worker.hashCode());
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
		final JobFailure other = (JobFailure) obj;
		if (this.backtrace == null)
		{
			if (other.backtrace != null)
			{
				return false;
			}
		}
		else if (!this.backtrace.equals(other.backtrace))
		{
			return false;
		}
		if (this.error == null)
		{
			if (other.error != null)
			{
				return false;
			}
		}
		else if (!this.error.equals(other.error))
		{
			return false;
		}
		if (this.exception == null)
		{
			if (other.exception != null)
			{
				return false;
			}
		}
		else if (!this.exception.equals(other.exception))
		{
			return false;
		}
		if (this.failedAt == null)
		{
			if (other.failedAt != null)
			{
				return false;
			}
		}
		else if (!this.failedAt.equals(other.failedAt))
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
		if (this.worker == null)
		{
			if (other.worker != null)
			{
				return false;
			}
		}
		else if (!this.worker.equals(other.worker))
		{
			return false;
		}
		return true;
	}
}
