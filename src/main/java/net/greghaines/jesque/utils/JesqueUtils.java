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
import static net.greghaines.jesque.utils.ResqueConstants.FREQUENCY;

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

    private static final String BT_PREFIX = "\tat ";
    private static final String BT_CAUSED_BY_PREFIX = "Caused by: ";
    private static final String BT_UNKNOWN_SOURCE = "Unknown Source";
    private static final String BT_NATIVE_METHOD = "Native Method";
    private static final Pattern BT_PATTERN = Pattern.compile("[\\(\\):]");
    private static final Pattern COLON_SPACE_PATTERN = Pattern.compile(":\\s");

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
        final StringBuilder buf = new StringBuilder();
        String prefix = "";
        for (final String str : strs) {
            buf.append(prefix).append(str);
            prefix = sep;
        }
        return buf.toString();
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

    public static String createRecurringHashKey(final String queue) {
        return createKey(queue, FREQUENCY);
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
        if (!"".equals(namespace)) {
            list.add(namespace);
        }
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
            bTrace.add(BT_PREFIX + ste.toString());
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
            bTrace.add(BT_CAUSED_BY_PREFIX + cause.getClass().getName());
        } else {
            bTrace.add(BT_CAUSED_BY_PREFIX + cause.getClass().getName() + ": " + cause.getMessage());
        }
        for (final StackTraceElement ste : cause.getStackTrace()) {
            bTrace.add(BT_PREFIX + ste.toString());
        }
        if (cause.getCause() != null) {
            addCauseToBacktrace(cause.getCause(), bTrace);
        }
    }

    /**
     * Recreate an exception from a type name, a message and a backtrace
     * (created from <code>JesqueUtils.createBacktrace(Throwable)</code>).
     * <p>
     * <b>Limitations:</b><br>
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
    public static Throwable recreateThrowable(final String type, final String message, final List<String> backtrace)
            throws ParseException, ClassNotFoundException, NoSuchConstructorException,
            AmbiguousConstructorException, ReflectiveOperationException {
        final LinkedList<String> bTrace = new LinkedList<String>(backtrace);
        Throwable cause = null;
        StackTraceElement[] stes = null;
        while (!bTrace.isEmpty()) {
            stes = recreateStackTrace(bTrace);
            if (!bTrace.isEmpty()) {
                final String line = bTrace.removeLast().substring(BT_CAUSED_BY_PREFIX.length());
                final String[] classNameAndMsg = COLON_SPACE_PATTERN.split(line, 2);
                final String msg = (classNameAndMsg.length == 2) ? classNameAndMsg[1] : null;
                cause = instantiateThrowable(classNameAndMsg[0], msg, cause, stes);
            }
        }
        return instantiateThrowable(type, message, cause, stes);
    }

    protected static Throwable instantiateThrowable(final String type, final String message, final Throwable cause,
            final StackTraceElement[] stes) throws ClassNotFoundException, AmbiguousConstructorException,
            ReflectiveOperationException, NoSuchConstructorException {
        Throwable throwable = null;
        boolean causeInited = false;
        final Class<?> throwableType = ReflectionUtils.forName(type);
        if (message == null) {
            try {
                try {
                    throwable = (Throwable) ReflectionUtils.createObject(throwableType);
                } catch (NoSuchConstructorException nsce2) {
                    if (cause == null) {
                        throw nsce2;
                    }
                    causeInited = true;
                    throwable = (Throwable) ReflectionUtils.createObject(throwableType, cause);
                }
            } catch (NoSuchConstructorException nsce) {
                try {
                    throwable = (Throwable) ReflectionUtils.createObject(throwableType, (String) null);
                } catch (NoSuchConstructorException nsce3) {
                    if (cause == null) {
                        throw nsce3;
                    }
                    causeInited = true;
                    throwable = (Throwable) ReflectionUtils.createObject(throwableType, (String) null, cause);
                }
            }
        } else {
            try {
                throwable = (Throwable) ReflectionUtils.createObject(throwableType, message);
            } catch (NoSuchConstructorException nsce) {
                if (cause == null) {
                    throw nsce;
                }
                causeInited = true;
                throwable = (Throwable) ReflectionUtils.createObject(throwableType, message, cause);
            }
        }
        throwable.setStackTrace(stes);
        if (!causeInited && cause != null) {
            throwable.initCause(cause);
        }
        return throwable;
    }

    protected static StackTraceElement[] recreateStackTrace(final List<String> bTrace) throws ParseException {
        final List<StackTraceElement> stes = new LinkedList<StackTraceElement>();
        if (bTrace != null) {
            final ListIterator<String> iter = bTrace.listIterator(bTrace.size());
            while (iter.hasPrevious()) {
                final String prev = iter.previous();
                if (prev.startsWith(BT_PREFIX)) { // All stack trace elements start with BT_PREFIX
                    iter.remove();
                    final String[] stParts = BT_PATTERN.split(prev.substring(BT_PREFIX.length()));
                    if (stParts.length < 2 || stParts.length > 3) {
                        throw new ParseException("Malformed stack trace element string: " + prev, 0);
                    }
                    final int periodPos = stParts[0].lastIndexOf('.');
                    final String className = stParts[0].substring(0, periodPos);
                    final String methodName = stParts[0].substring(periodPos + 1);
                    final String fileName;
                    final int lineNumber;
                    if (BT_UNKNOWN_SOURCE.equals(stParts[1])) {
                        fileName = null;
                        lineNumber = -1;
                    } else if (BT_NATIVE_METHOD.equals(stParts[1])) {
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
        }
        return stes.toArray(new StackTraceElement[stes.size()]);
    }

    /**
     * A convenient way of creating a map on the fly.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param entries
     *            Map.Entry objects to be added to the map
     * @return a LinkedHashMap with the supplied entries
     */
    @SafeVarargs
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
     * @param <K> the key type
     * @param <V> the value type
     * @param key
     *            the key
     * @param value
     *            the value
     * @return a Map.Entry object with the given key and value
     */
    public static <K, V> Entry<K, V> entry(final K key, final V value) {
        return new SimpleImmutableEntry<K, V>(key, value);
    }

    /**
     * Creates a Set out of the given keys
     *
     * @param <K> the key type
     * @param keys
     *            the keys
     * @return a Set containing the given keys
     */
    @SafeVarargs
    public static <K> Set<K> set(final K... keys) {
        return new LinkedHashSet<K>(Arrays.asList(keys));
    }

    /**
     * Test for equality.
     * @param obj1 the first object
     * @param obj2 the second object
     * @return true if both are null or the two objects are equal
     */
    public static boolean nullSafeEquals(final Object obj1, final Object obj2) {
        return ((obj1 == null && obj2 == null)
                || (obj1 != null && obj2 != null && obj1.equals(obj2)));
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
        return ReflectionUtils.createObject(clazz, job.getArgs(), job.getVars());
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
    public static Object materializeJob(final Job job, final Map<String, Class<?>> jobTypes)
            throws UnpermittedJobException, Exception {
        final String className = job.getClassName();
        final Class<?> clazz = jobTypes.get(className);
        if (clazz == null) {
            throw new UnpermittedJobException(className);
        }
        // A bit redundant since we check when the job type is added...
        if (!Runnable.class.isAssignableFrom(clazz) && !Callable.class.isAssignableFrom(clazz)) {
            throw new ClassCastException("jobs must be a Runnable or a Callable: " + clazz.getName() + " - " + job);
        }
        return ReflectionUtils.createObject(clazz, job.getArgs(), job.getVars());
    }

    /**
     * This is needed because Throwable doesn't override equals() and object
     * equality is not what we want to test.
     *
     * @param ex1
     *            first Throwable
     * @param ex2
     *            second Throwable
     * @return true if the two arguments are equal, as we define it.
     */
    public static boolean equal(final Throwable ex1, final Throwable ex2) {
        if (ex1 == ex2) {
            return true;
        }
        if (ex1 == null) {
            if (ex2 != null) {
                return false;
            }
        } else if (ex2 == null) {
            if (ex1 != null) {
                return false;
            }
        } else {
            if (ex1.getClass() != ex2.getClass()) {
                return false;
            }
            if (ex1.getMessage() == null) {
                if (ex2.getMessage() != null) {
                    return false;
                }
            } else if (!ex1.getMessage().equals(ex2.getMessage())) {
                return false;
            }
            if (!equal(ex1.getCause(), ex2.getCause())) {
                return false;
            }
        }
        return true;
    }

    private JesqueUtils() {
        // Utility class
    }
}
