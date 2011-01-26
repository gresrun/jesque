package net.greghaines.jesque.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public final class JesqueUtils
{
	public static final String DATE_FORMAT = "EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)";
	
	/**
	 * Join the given strings, separated by the given separator.
	 * 
	 * @param sep the separator
	 * @param strs the strings to join
	 * @return the joined string
	 */
	public static String join(final String sep, final String... strs)
	{
		final StringBuilder sb = new StringBuilder();
		String s = "";
		for (final String str : strs)
		{
			sb.append(s).append(str);
			s = sep;
		}
		return sb.toString();
	}
	
	/**
	 * Join the given strings, separated by the given separator.
	 * 
	 * @param sep the separator
	 * @param strs the strings to join
	 * @return the joined string
	 */
	public static String join(final String sep, final Collection<String> strs)
	{
		final StringBuilder sb = new StringBuilder();
		String s = "";
		for (final String str : strs)
		{
			sb.append(s).append(str);
			s = sep;
		}
		return sb.toString();
	}
	
	/**
	 * Builds a namespaced Redis key with the given arguments.
	 * 
	 * @param namespace the namespace to use
	 * @param parts the key parts to be joined
	 * @return an assembled String key
	 */
	public static String createKey(final String namespace, final String... parts)
	{
		final List<String> list = new ArrayList<String>(parts.length + 1);
		list.add(namespace);
		list.addAll(Arrays.asList(parts));
		return join(":", list);
	}
	
	/**
	 * @param ex the Exception to use
	 * @return a list of strings that represent how the exception's stacktrace appears.
	 */
	public static List<String> createStackTrace(final Throwable ex)
	{
		final List<String> bTrace = new LinkedList<String>();
		for (final StackTraceElement ste : ex.getStackTrace())
		{
			bTrace.add("\tat " + ste.toString());
		}
		if (ex.getCause() != null)
		{
			addToBacktrace(ex.getCause(), bTrace);
		}
		return bTrace;
	}

	private static void addToBacktrace(final Throwable ex, final List<String> bTrace)
	{
		bTrace.add("Caused by: " + ex.getClass().getName() + ": " + ex.getMessage());
		for (final StackTraceElement ste : ex.getStackTrace())
		{
			bTrace.add("\tat " + ste.toString());
		}
		if (ex.getCause() != null)
		{
			addToBacktrace(ex.getCause(), bTrace);
		}
	}
	
	/**
	 * Sleep the current thread, ignoring any Exception that might occur.
	 * 
	 * @param millis the time to sleep for
	 */
	public static void sleepTight(final long millis)
	{
		try { Thread.sleep(millis); } catch (Exception e){} // Ignore
	}

	private JesqueUtils(){} // Utility class
}
