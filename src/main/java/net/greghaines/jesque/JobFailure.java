package net.greghaines.jesque;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

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

	public String getWorker()
	{
		return this.worker;
	}

	public void setWorker(final String worker)
	{
		this.worker = worker;
	}

	public Job getPayload()
	{
		return this.payload;
	}

	public void setPayload(final Job payload)
	{
		this.payload = payload;
	}

	public String getException()
	{
		return this.exception;
	}

	public void setException(final String exception)
	{
		this.exception = exception;
	}

	public String getError()
	{
		return this.error;
	}

	public void setError(String error)
	{
		this.error = error;
	}

	public List<String> getBacktrace()
	{
		return this.backtrace;
	}

	public void setBacktrace(final List<String> backtrace)
	{
		this.backtrace = backtrace;
	}

	public Date getFailedAt()
	{
		return this.failedAt;
	}

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
