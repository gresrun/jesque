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
package net.greghaines.jesque.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;

/**
 * Miscellaneous utilities.
 * 
 * @author Greg Haines
 */
public final class JesqueUtils
{
	public static final String DATE_FORMAT = "EEE MMM dd yyyy HH:mm:ss 'GMT'Z (z)";
	private static final String bTracePrefix = "\tat ";
	private static final String btUnknownSource = "Unknown Source";
	private static final String btNativeMethod = "Native Method";
	private static final Pattern btPattern = Pattern.compile("[\\(\\):]");
	
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
			addCauseToBacktrace(ex.getCause(), bTrace);
		}
		return bTrace;
	}

	private static void addCauseToBacktrace(final Throwable ex, final List<String> bTrace)
	{
		bTrace.add("Caused by: " + ex.getClass().getName() + ": " + ex.getMessage());
		for (final StackTraceElement ste : ex.getStackTrace())
		{
			bTrace.add(bTracePrefix + ste.toString());
		}
		if (ex.getCause() != null)
		{
			addCauseToBacktrace(ex.getCause(), bTrace);
		}
	}
	
	public static Throwable recreateException(final String type, final String message, 
			final List<String> backtrace)
	{
		Throwable t = null;
		final List<String> bTrace = new LinkedList<String>(backtrace);
		
		return t;
	}
	
	private static StackTraceElement[] recreateStackTrace(final List<String> bTrace)
	{
		final List<StackTraceElement> stes = new LinkedList<StackTraceElement>();
		final ListIterator<String> iter = bTrace.listIterator(bTrace.size() - 1);
		while (iter.hasPrevious())
		{
			final String prev = iter.previous();
			if (prev.startsWith(bTracePrefix))
			{ // All stack trace elements start with bTracePrefix
				iter.remove();
				final String[] stParts = btPattern.split(prev.substring(bTracePrefix.length()));
			}
			else
			{ // Stop if it is not a stack trace element
				break;
			}
		}
		return stes.toArray(new StackTraceElement[stes.size()]);
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
	
	public static void main(final String[] args)
	{
		System.out.println(Arrays.toString(btPattern.split("\tat MyClass.mash(MyClass.java:9)".substring(bTracePrefix.length()))));
		System.out.println(Arrays.toString(btPattern.split("MyClass.mash(MyClass.java)")));
		System.out.println(Arrays.toString(btPattern.split("MyClass.mash(Unknown Source)")));
		System.out.println(Arrays.toString(btPattern.split("MyClass.mash(Native Method)")));
		System.out.println(Arrays.toString(btPattern.split("com.ndr.foo.MyClass$Bar.mash(MyClass.java:9)")));
	}

	private JesqueUtils(){} // Utility class
}
