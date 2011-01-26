package net.greghaines.jesque;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Job implements Serializable
{
	private static final long serialVersionUID = -1523425239512691383L;
	
	private final String className;
	private final Object[] args;
	
	public Job(final String className, final List<?> args)
	{
		this(className, args.toArray());
	}
	
	public Job(final String className, final Object... args)
	{
		if (className == null || "".equals(className))
		{
			throw new IllegalArgumentException("className must not be null or empty: " + className);
		}
		this.className = className;
		this.args = args;
	}

	public String getClassName()
	{
		return this.className;
	}

	public Object[] getArgs()
	{
		return this.args;
	}
	
	@Override
	public String toString()
	{
		return "<Job className=" + this.className + " args=" + Arrays.toString(this.args) + ">";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.args);
		result = prime * result + ((this.className == null) ? 0 : this.className.hashCode());
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
		final Job other = (Job) obj;
		if (!Arrays.equals(this.args, other.args))
		{
			return false;
		}
		if (this.className == null)
		{
			if (other.className != null)
			{
				return false;
			}
		}
		else if (!this.className.equals(other.className))
		{
			return false;
		}
		return true;
	}
}
