package net.greghaines.jesque.worker;

/**
 * Thrown by a Worker when it receives a Job that it is not allowed to run.
 * 
 * @author Greg Haines
 */
public class UnpermittedJobException extends Exception
{
	private static final long serialVersionUID = -5360734802682205116L;
	
	private final Class<?> type;

	public UnpermittedJobException(final Class<?> type)
	{
		super(type.getName());
		this.type = type;
	}

	/**
	 * @return the type of Job that was not permitted
	 */
	public Class<?> getType()
	{
		return this.type;
	}
}
