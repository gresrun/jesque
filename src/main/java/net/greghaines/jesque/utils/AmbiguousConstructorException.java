package net.greghaines.jesque.utils;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;

/**
 * Thrown when there is more than one matching Constructor is found.
 * 
 * @author Greg Haines
 */
public class AmbiguousConstructorException extends Exception
{
	private static final long serialVersionUID = -5360734802682205116L;
	
	private final Class<?> type;
	private final Object[] args;
	private final Set<Constructor<?>> options;
	
	public AmbiguousConstructorException(final Class<?> type, final Object[] args, 
			final Set<Constructor<?>> options)
	{
		super("Found " + options.size() + " possible matches for class=" + 
			type.getName() + " args=" + Arrays.toString(args) + ": " + options);
		this.type = type;
		this.args = args;
		this.options = options;
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

	/**
	 * @return the possible Constructors that matched
	 */
	public Set<Constructor<?>> getOptions()
	{
		return this.options;
	}
}
