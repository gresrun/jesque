package net.greghaines.jesque.utils;

import java.util.Arrays;

/**
 * Thrown when the specified constructor could not be found.
 * 
 * @author Greg Haines
 */
public class NoSuchConstructorException extends Exception
{
	private static final long serialVersionUID = 4960599901666926591L;
	
	private final Class<?> type;
	private final Object[] args;

	public NoSuchConstructorException(final Class<?> type, final Object... args)
	{
		super("class=" + type.getName() + " args=" + Arrays.toString(args));
		this.type = type;
		this.args = args;
	}

	/**
	 * @return the Class object searched.
	 */
	public Class<?> getType()
	{
		return this.type;
	}

	/**
	 * @return the arguments that the Constructor needed to match.
	 */
	public Object[] getArgs()
	{
		return this.args;
	}
}
