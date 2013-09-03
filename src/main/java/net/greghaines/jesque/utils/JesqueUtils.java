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

import static net.greghaines.jesque.utils.ResqueConstants.COLON;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.UnpermittedJobException;

/**
 * Miscellaneous utilities.
 * 
 * @author Greg Haines
 */
public final class JesqueUtils {

    private static final String bTracePrefix = "\tat ";
    private static final String btCausedByPrefix = "Caused by: ";
    private static final String btUnknownSource = "Unknown Source";
    private static final String btNativeMethod = "Native Method";
    private static final Pattern btPattern = Pattern.compile("[\\(\\):]");
    private static final Pattern colonSpacePattern = Pattern.compile(":\\s");

    /**
     * Join the given strings, separated by the given separator.
     * 
     * @param sep
     *            the separator
     * @param strs
     *            the strings to join
     * @return the joined string
     */
    public static String join(final String sep, final String... strs) {
        return join(sep, Arrays.asList(strs));
    }

    /**
     * Join the given strings, separated by the given separator.
     * 
     * @param sep
     *            the separator
     * @param strs
     *            the strings to join
     * @return the joined string
     */
    public static String join(final String sep, final Iterable<String> strs) {
        final StringBuilder sb = new StringBuilder();
        String s = "";
        for (final String str : strs) {
            sb.append(s).append(str);
            s = sep;
        }
        return sb.toString();
    }

    /**
     * Builds a namespaced Redis key with the given arguments.
     * 
     * @param namespace
     *            the namespace to use
     * @param parts
     *            the key parts to be joined
     * @return an assembled String key
     */
    public static String createKey(final String namespace, final String... parts) {
        return createKey(namespace, Arrays.asList(parts));
    }

    /**
     * Builds a namespaced Redis key with the given arguments.
     * 
     * @param namespace
     *            the namespace to use
     * @param parts
     *            the key parts to be joined
     * @return an assembled String key
     */
    public static String createKey(final String namespace, final Iterable<String> parts) {
        final List<String> list = new LinkedList<String>();
        list.add(namespace);
        for (final String part : parts) {
            list.add(part);
        }
        return join(COLON, list);
    }

    /**
     * Creates a Resque backtrace from a Throwable's stack trace. Includes
     * causes.
     * 
     * @param t
     *            the Exception to use
     * @return a list of strings that represent how the exception's stacktrace
     *         appears.
     */
    public static List<String> createBacktrace(final Throwable t) {
        final List<String> bTrace = new LinkedList<String>();
        for (final StackTraceElement ste : t.getStackTrace()) {
            bTrace.add(bTracePrefix + ste.toString());
        }
        if (t.getCause() != null) {
            addCauseToBacktrace(t.getCause(), bTrace);
        }
        return bTrace;
    }

    /**
     * Add a cause to the backtrace.
     * 
     * @param cause
     *            the cause
     * @param bTrace
     *            the backtrace list
     */
    private static void addCauseToBacktrace(final Throwable cause, final List<String> bTrace) {
        if (cause.getMessage() == null) {
            bTrace.add(btCausedByPrefix + cause.getClass().getName());
        } else {
            bTrace.add(btCausedByPrefix + cause.getClass().getName() + ": " + cause.getMessage());
        }
        for (final StackTraceElement ste : cause.getStackTrace()) {
            bTrace.add(bTracePrefix + ste.toString());
        }
        if (cause.getCause() != null) {
            addCauseToBacktrace(cause.getCause(), bTrace);
        }
    }

    /**
     * Recreate an exception from a type name, a message and a backtrace
     * (created from <code>JesqueUtils.createBacktrace(Throwable)</code>).
     * <p/>
     * <b>Limitations:</b><br/>
     * This method cannot recreate Throwables with unusual/custom Constructors.
     * <ul>
     * <li>If the message is non-null and the cause is null, there must be a
     * Constructor with a single String as it's only parameter.</li>
     * <li>If the message is non-null and the cause is non-null, there must be a
     * Constructor with a single String as it's only parameter or a Constructor
     * with a String and a Throwable as its parameters.</li>
     * <li>If the message is null and the cause is null, there must be either a
     * no-arg Constructor or a Constructor with a single String as it's only
     * parameter.</li>
     * <li>If the message is null and the cause is non-null, there must be
     * either a no-arg Constructor, a Constructor with a single String as its
     * only parameter or a Constructor with a String and a Throwable as its
     * parameters.</li>
     * </ul>
     * 
     * @param type
     *            the String name of the Throwable type
     * @param message
     *            the message of the exception
     * @param backtrace
     *            the backtrace of the exception
     * @return a new Throwable of the given type with the given message and
     *         given backtrace/causes
     * @throws ParseException
     *             if there is a problem parsing the given backtrace
     * @throws ClassNotFoundException
     *             if the given type is not available
     * @throws NoSuchConstructorException
     *             if there is not a common constructor available for the given
     *             type
     * @throws AmbiguousConstructorException
     *             if there is more than one constructor that is viable
     * @throws InstantiationException
     *             if there is a problem instantiating the given type
     * @throws IllegalAccessException
     *             if the common constructor is not visible
     * @throws InvocationTargetException
     *             if the constructor threw an exception
     * @see JesqueUtils#createBacktrace(Throwable)
     */
    public static Throwable recreateThrowable(final String type, final String message, final List<String> backtrace) throws ParseException, ClassNotFoundException, NoSuchConstructorException,
            AmbiguousConstructorException, InstantiationException, IllegalAccessException, InvocationTargetException {
        final LinkedList<String> bTrace = new LinkedList<String>(backtrace);
        Throwable cause = null;
        StackTraceElement[] stes = null;
        while (!bTrace.isEmpty()) {
            stes = recreateStackTrace(bTrace);
            if (!bTrace.isEmpty()) {
                final String line = bTrace.removeLast().substring(btCausedByPrefix.length());
                final String[] classNameAndMsg = colonSpacePattern.split(line, 2);
                final String msg = (classNameAndMsg.length == 2) ? classNameAndMsg[1] : null;
                cause = instantiateThrowable(classNameAndMsg[0], msg, cause, stes);
            }
        }
        return instantiateThrowable(type, message, cause, stes);
    }

    private static Throwable instantiateThrowable(final String type, final String message, final Throwable cause, final StackTraceElement[] stes) throws ClassNotFoundException,
            AmbiguousConstructorException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchConstructorException {
        Throwable t = null;
        boolean causeInited = false;
        final Class<?> throwableType = ReflectionUtils.forName(type);
        if (message == null) {
            try {
                try {
                    t = (Throwable) ReflectionUtils.createObject(throwableType);
                } catch (NoSuchConstructorException nsce2) {
                    if (cause == null) {
                        throw nsce2;
                    }
                    causeInited = true;
                    t = (Throwable) ReflectionUtils.createObject(throwableType, cause);
                }
            } catch (NoSuchConstructorException nsce) {
                try {
                    t = (Throwable) ReflectionUtils.createObject(throwableType, (String) null);
                } catch (NoSuchConstructorException nsce3) {
                    if (cause == null) {
                        throw nsce3;
                    }
                    causeInited = true;
                    t = (Throwable) ReflectionUtils.createObject(throwableType, (String) null, cause);
                }
            }
        } else {
            try {
                t = (Throwable) ReflectionUtils.createObject(throwableType, message);
            } catch (NoSuchConstructorException nsce) {
                if (cause == null) {
                    throw nsce;
                }
                causeInited = true;
                t = (Throwable) ReflectionUtils.createObject(throwableType, message, cause);
            }
        }
        t.setStackTrace(stes);
        if (!causeInited && cause != null) {
            t.initCause(cause);
        }
        return t;
    }

    private static StackTraceElement[] recreateStackTrace(final List<String> bTrace) throws ParseException {
        final List<StackTraceElement> stes = new LinkedList<StackTraceElement>();
        final ListIterator<String> iter = bTrace.listIterator(bTrace.size());
        while (iter.hasPrevious()) {
            final String prev = iter.previous();
            if (prev.startsWith(bTracePrefix)) { // All stack trace elements
                                                 // start with bTracePrefix
                iter.remove();
                final String[] stParts = btPattern.split(prev.substring(bTracePrefix.length()));
                if (stParts.length < 2 || stParts.length > 3) {
                    throw new ParseException("Malformed stack trace element string: " + prev, 0);
                }
                final int periodPos = stParts[0].lastIndexOf('.');
                final String className = stParts[0].substring(0, periodPos);
                final String methodName = stParts[0].substring(periodPos + 1);
                final String fileName;
                final int lineNumber;
                if (btUnknownSource.equals(stParts[1])) {
                    fileName = null;
                    lineNumber = -1;
                } else if (btNativeMethod.equals(stParts[1])) {
                    fileName = null;
                    lineNumber = -2;
                } else {
                    fileName = stParts[1];
                    lineNumber = (stParts.length == 3) ? Integer.parseInt(stParts[2]) : -1;
                }
                stes.add(0, new StackTraceElement(className, methodName, fileName, lineNumber));
            } else { // Stop if it is not a stack trace element
                break;
            }
        }
        return stes.toArray(new StackTraceElement[stes.size()]);
    }

    /**
     * A convenient way of creating a map on the fly.
     * 
     * @param entries
     *            Map.Entry objects to be added to the map
     * @return a LinkedHashMap with the supplied entries
     */
    public static <K, V> Map<K, V> map(final Entry<? extends K, ? extends V>... entries) {
        final Map<K, V> map = new LinkedHashMap<K, V>(entries.length);
        for (final Entry<? extends K, ? extends V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    /**
     * Creates a Map.Entry out of the given key and value. Commonly used in
     * conjunction with map(Entry...)
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @return a Map.Entry object with the give key and value
     */
    public static <K, V> Entry<K, V> entry(final K key, final V value) {
        return new SimpleImmutableEntry<K, V>(key, value);
    }

    /**
     * Creates a Set out of the given keys
     * 
     * @param keys
     *            the keys
     * @return a Set containing the given keys
     */
    public static <K> Set<K> set(final K... keys) {
        return new LinkedHashSet<K>(Arrays.asList(keys));
    }

    /**
     * Materializes a job by assuming the {@link Job#getClassName()} is a
     * fully-qualified Java type.
     * 
     * @param job
     *            the job to materialize
     * @return the materialized job
     * @throws ClassNotFoundException
     *             if the class could not be found
     * @throws Exception
     *             if there was an exception creating the object
     */
    public static Object materializeJob(final Job job) throws ClassNotFoundException, Exception {
        final Class<?> clazz = ReflectionUtils.forName(job.getClassName());
        // A bit redundant since we check when the job type is added...
        if (!Runnable.class.isAssignableFrom(clazz) && !Callable.class.isAssignableFrom(clazz)) {
            throw new ClassCastException("jobs must be a Runnable or a Callable: " + clazz.getName() + " - " + job);
        }
        return ReflectionUtils.createObject(clazz, job.getArgs());
    }

    /**
     * Materializes a job by looking up {@link Job#getClassName()} in the
     * provided map of job types.
     * 
     * @param job
     *            the job to materialize
     * @param jobTypes
     *            a map of String names to Java types
     * @return the materialized job
     * @throws UnpermittedJobException
     *             if there was not a non-null mapping in jobTypes for the class
     *             name
     * @throws Exception
     *             if there was an exception creating the object
     */
    public static Object materializeJob(final Job job, final Map<String, Class<?>> jobTypes) throws UnpermittedJobException, Exception {
        final String className = job.getClassName();
        final Class<?> clazz = jobTypes.get(className);
        if (clazz == null) {
            throw new UnpermittedJobException(className);
        }
        // A bit redundant since we check when the job type is added...
        if (!Runnable.class.isAssignableFrom(clazz) && !Callable.class.isAssignableFrom(clazz)) {
            throw new ClassCastException("jobs must be a Runnable or a Callable: " + clazz.getName() + " - " + job);
        }
        return ReflectionUtils.createObject(clazz, job.getArgs());
    }

    private JesqueUtils() {
        // Utility class
    }
}
