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

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Reflection utilities.
 * 
 * @author Greg Haines
 */
@SuppressWarnings("unchecked")
public final class ReflectionUtils
{
	/** Suffix for array class names: "[]" */
	public static final String ARRAY_SUFFIX = "[]";
	/** Prefix for internal array class names: "[" */
	private static final String INTERNAL_ARRAY_PREFIX = "[";
	/** Prefix for internal non-primitive array class names: "[L" */
	private static final String NON_PRIMITIVE_ARRAY_PREFIX = "[L";
	/**
	 * Map with primitive wrapper type as key and corresponding primitive
	 * type as value, for example: Integer.class -> int.class.
	 */
	private static final Map<Class<?>,Class<?>> wrapperTypeToPrimitiveMap = new HashMap<Class<?>,Class<?>>(8);
	/**
	 * Map with primitive type as key and corresponding wrapper
	 * type as value, for example: int.class -> Integer.class.
	 */
	private static final Map<Class<?>,Class<?>> primitiveTypeToWrapperMap = new HashMap<Class<?>,Class<?>>(8);
	/**
	 * Map with primitive type name as key and corresponding primitive
	 * type as value, for example: "int" -> "int.class".
	 */
	private static final Map<String, Class<?>> primitiveTypeNameMap = new HashMap<String, Class<?>>(16);
	/**
	 * Map with common "java.lang" class name as key and corresponding Class as value.
	 * Primarily for efficient deserialization of remote invocations.
	 */
	private static final Map<String, Class<?>> commonClassCache = new HashMap<String, Class<?>>(32);

	static
	{
		wrapperTypeToPrimitiveMap.put(Boolean.class, boolean.class);
		wrapperTypeToPrimitiveMap.put(Byte.class, byte.class);
		wrapperTypeToPrimitiveMap.put(Character.class, char.class);
		wrapperTypeToPrimitiveMap.put(Double.class, double.class);
		wrapperTypeToPrimitiveMap.put(Float.class, float.class);
		wrapperTypeToPrimitiveMap.put(Integer.class, int.class);
		wrapperTypeToPrimitiveMap.put(Long.class, long.class);
		wrapperTypeToPrimitiveMap.put(Short.class, short.class);
		for (final Entry<Class<?>,Class<?>> e : wrapperTypeToPrimitiveMap.entrySet())
		{
			primitiveTypeToWrapperMap.put(e.getValue(), e.getKey());
		}
		final Set<Class<?>> primitiveTypes = new HashSet<Class<?>>(16);
		primitiveTypes.addAll(wrapperTypeToPrimitiveMap.values());
		primitiveTypes.addAll(Arrays.asList(
				boolean[].class, byte[].class, char[].class, double[].class,
				float[].class, int[].class, long[].class, short[].class));
		for (final Class<?> primitiveType : primitiveTypes)
		{
			primitiveTypeNameMap.put(primitiveType.getName(), primitiveType);
		}
		final List<Class<?>> commonClasses = Arrays.asList(Boolean[].class, Byte[].class, 
			Character[].class, Double[].class, Float[].class, Integer[].class, Long[].class, 
			Short[].class, Number.class, Number[].class, String.class, String[].class,
			Object.class, Object[].class, Class.class, Class[].class, Throwable.class, 
			Exception.class, RuntimeException.class, Error.class, StackTraceElement.class, 
			StackTraceElement[].class);
		for (final Class<?> clazz : commonClasses)
		{
			commonClassCache.put(clazz.getName(), clazz);
		}
	}
	
	/**
	 * Create an object of the given type using a constructor that matches the supplied arguments.
	 * 
	 * @param <T>
	 * @param clazz the type to create
	 * @param args the arguments to the constructor
	 * @return a new object of the given type, initialized with the given arguments
	 * @throws NoSuchConstructorException if there is not a constructor that matches the given arguments
	 * @throws AmbiguousConstructorException if there is more than one constructor that matches the given arguments
	 * @throws InstantiationException if the class that declares the underlying constructor represents an abstract class
	 * @throws IllegalAccessException if the Constructor object enforces Java language access control and the underlying constructor is inaccessible
	 * @throws InvocationTargetException if the underlying constructor throws an exception
	 */
	public static <T> T createObject(final Class<T> clazz, final Object... args)
	throws NoSuchConstructorException, AmbiguousConstructorException, 
			InstantiationException, IllegalAccessException, InvocationTargetException
	{
		return findConstructor(clazz, args).newInstance(args);
	}

	/**
	 * Find a Constructor on the given type that matches the given arguments.
	 * 
	 * @param <T>
	 * @param clazz the type to create
	 * @param args the arguments to the constructor
	 * @return a Constructor from the given type that matches the given arguments
	 * @throws NoSuchConstructorException if there is not a constructor that matches the given arguments
	 * @throws AmbiguousConstructorException if there is more than one constructor that matches the given arguments
	 */
	@SuppressWarnings("rawtypes")
	private static <T> Constructor<T> findConstructor(final Class<T> clazz, final Object... args)
	throws NoSuchConstructorException, AmbiguousConstructorException
	{
		Constructor<T> constructorToUse = null;
		final Constructor<?>[] candidates = clazz.getConstructors();
		Arrays.sort(candidates, ConstructorComparator.INSTANCE);
		int minTypeDiffWeight = Integer.MAX_VALUE;
		Set<Constructor<?>> ambiguousConstructors = null;
		for (int i = 0; i < candidates.length; i++)
		{
			final Constructor candidate = candidates[i];
			final Class[] paramTypes = candidate.getParameterTypes();
			if (constructorToUse != null && args.length > paramTypes.length)
			{
				// Already found greedy constructor that can be satisfied.
				// Do not look any further, there are only less greedy constructors left.
				break;
			}
			if (paramTypes.length < args.length)
			{
				continue;
			}
			if (paramTypes.length != args.length)
			{
				continue;
			}
			final int typeDiffWeight = getTypeDifferenceWeight(paramTypes, args);
			if (typeDiffWeight < minTypeDiffWeight)
			{ // Choose this constructor if it represents the closest match.
				constructorToUse = candidate;
				minTypeDiffWeight = typeDiffWeight;
				ambiguousConstructors = null;
			}
			else if (constructorToUse != null && typeDiffWeight == minTypeDiffWeight)
			{
				if (ambiguousConstructors == null)
				{
					ambiguousConstructors = new LinkedHashSet<Constructor<?>>();
					ambiguousConstructors.add(constructorToUse);
				}
				ambiguousConstructors.add(candidate);
			}
		}
		if (ambiguousConstructors != null && !ambiguousConstructors.isEmpty())
		{
			throw new AmbiguousConstructorException(clazz, args, ambiguousConstructors);
		}
		if (constructorToUse == null)
		{
			throw new NoSuchConstructorException(clazz, args);
		}
		return constructorToUse;
	}
	
	/**
	 * Algorithm that judges the match between the declared parameter types of a candidate method
	 * and a specific list of arguments that this method is supposed to be invoked with.
	 * <p>Determines a weight that represents the class hierarchy difference between types and
	 * arguments. A direct match, i.e. type Integer -> arg of class Integer, does not increase
	 * the result - all direct matches means weight 0. A match between type Object and arg of
	 * class Integer would increase the weight by 2, due to the superclass 2 steps up in the
	 * hierarchy (i.e. Object) being the last one that still matches the required type Object.
	 * Type Number and class Integer would increase the weight by 1 accordingly, due to the
	 * superclass 1 step up the hierarchy (i.e. Number) still matching the required type Number.
	 * Therefore, with an arg of type Integer, a constructor (Integer) would be preferred to a
	 * constructor (Number) which would in turn be preferred to a constructor (Object).
	 * All argument weights get accumulated.
	 * 
	 * @param paramTypes the parameter types to match
	 * @param args the arguments to match
	 * @return the accumulated weight for all arguments
	 */
	@SuppressWarnings("rawtypes")
	private static int getTypeDifferenceWeight(final Class[] paramTypes, final Object[] args)
	{
		int result = 0;
		for (int i = 0; i < paramTypes.length; i++)
		{
			if (!isAssignableValue(paramTypes[i], args[i]))
			{
				return Integer.MAX_VALUE;
			}
			if (args[i] != null)
			{
				final Class paramType = paramTypes[i];
				Class superClass = args[i].getClass().getSuperclass();
				while (superClass != null)
				{
					if (paramType.equals(superClass))
					{
						result = result + 2;
						superClass = null;
					}
					else if (isAssignable(paramType, superClass))
					{
						result = result + 2;
						superClass = superClass.getSuperclass();
					}
					else
					{
						superClass = null;
					}
				}
				if (paramType.isInterface())
				{
					result = result + 1;
				}
			}
		}
		return result;
	}
	
	/**
	 * Check if the right-hand side type may be assigned to the left-hand side
	 * type, assuming setting by reflection. Considers primitive wrapper
	 * classes as assignable to the corresponding primitive types.
	 * 
	 * @param lhsType the target type
	 * @param rhsType the value type that should be assigned to the target type
	 * @return if the target type is assignable from the value type
	 */
	public static boolean isAssignable(final Class<?> lhsType, final Class<?> rhsType)
	{
		if (lhsType.isAssignableFrom(rhsType))
		{
			return true;
		}
		if (lhsType.isPrimitive())
		{
			final Class<?> resolvedPrimitive = wrapperTypeToPrimitiveMap.get(rhsType);
			if (resolvedPrimitive != null && lhsType.equals(resolvedPrimitive))
			{
				return true;
			}
		}
		else
		{
			final Class<?> resolvedWrapper = primitiveTypeToWrapperMap.get(rhsType);
			if (resolvedWrapper != null && lhsType.isAssignableFrom(resolvedWrapper))
			{
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Determine if the given type is assignable from the given value,
	 * assuming setting by reflection. Considers primitive wrapper classes
	 * as assignable to the corresponding primitive types.
	 * 
	 * @param type the target type
	 * @param value the value that should be assigned to the type
	 * @return if the type is assignable from the value
	 */
	public static boolean isAssignableValue(final Class<?> type, final Object value)
	{
		return (value != null ? isAssignable(type, value.getClass()) : !type.isPrimitive());
	}
	
	private static final class ConstructorComparator implements Comparator<Constructor<?>>, Serializable
	{
		private static final long serialVersionUID = 1338239669376657022L;
		public static final ConstructorComparator INSTANCE = new ConstructorComparator();
		
		private ConstructorComparator(){} // Singleton
		
		public int compare(final Constructor<?> c1, final Constructor<?> c2)
		{
			final boolean p1 = Modifier.isPublic(c1.getModifiers());
			final boolean p2 = Modifier.isPublic(c2.getModifiers());
			if (p1 != p2)
			{
				return (p1 ? -1 : 1);
			}
			return (Integer.valueOf(c2.getParameterTypes().length)).compareTo(c1.getParameterTypes().length);
		}
	}
	
	/**
	 * Return the default ClassLoader to use: typically the thread context
	 * ClassLoader, if available; the ClassLoader that loaded the ReflectionUtils
	 * class will be used as fallback.
	 * <p>Call this method if you intend to use the thread context ClassLoader
	 * in a scenario where you absolutely need a non-null ClassLoader reference:
	 * for example, for class path resource loading (but not necessarily for
	 * <code>Class.forName</code>, which accepts a <code>null</code> ClassLoader
	 * reference as well).
	 * 
	 * @return the default ClassLoader (never <code>null</code>)
	 * @see java.lang.Thread#getContextClassLoader()
	 */
	public static ClassLoader getDefaultClassLoader()
	{
		ClassLoader cl = null;
		try
		{
			cl = Thread.currentThread().getContextClassLoader();
		}
		catch (Exception e) {} // Cannot access thread context ClassLoader - falling back to system class loader...
		if (cl == null)
		{ // No thread context class loader -> use class loader of this class.
			cl = ReflectionUtils.class.getClassLoader();
		}
		return cl;
	}
	
	/**
	 * Replacement for <code>Class.forName()</code> that also returns Class instances
	 * for primitives (like "int") and array class names (like "String[]").
	 * <p>Always uses the default class loader: that is, preferably the thread context
	 * class loader, or the ClassLoader that loaded the ClassUtils class as fallback.
	 * 
	 * @param name the name of the Class
	 * @return Class instance for the supplied name
	 * @throws ClassNotFoundException if the class was not found
	 * @see Class#forName(String, boolean, ClassLoader)
	 * @see #getDefaultClassLoader()
	 */
	public static Class<?> forName(final String name)
	throws ClassNotFoundException
	{
		return forName(name, getDefaultClassLoader());
	}

	/**
	 * Replacement for <code>Class.forName()</code> that also returns Class instances
	 * for primitives (e.g."int") and array class names (e.g. "String[]").
	 * Furthermore, it is also capable of resolving inner class names in Java source
	 * style (e.g. "java.lang.Thread.State" instead of "java.lang.Thread$State").
	 * 
	 * @param name the name of the Class
	 * @param classLoader the class loader to use
	 * (may be <code>null</code>, which indicates the default class loader)
	 * @return Class instance for the supplied name
	 * @throws ClassNotFoundException if the class was not found
	 * @see Class#forName(String, boolean, ClassLoader)
	 */
	public static Class<?> forName(final String name, final ClassLoader classLoader)
	throws ClassNotFoundException
	{
		if (name == null)
		{
			throw new IllegalArgumentException("name must not be null");
		}
		Class<?> clazz = resolvePrimitiveClassName(name);
		if (clazz == null)
		{
			clazz = commonClassCache.get(name);
		}
		if (clazz != null)
		{
			return clazz;
		}
		// "java.lang.String[]" style arrays
		if (name.endsWith(ARRAY_SUFFIX))
		{
			final String elementClassName = name.substring(0, name.length() - ARRAY_SUFFIX.length());
			final Class<?> elementClass = forName(elementClassName, classLoader);
			return Array.newInstance(elementClass, 0).getClass();
		}
		// "[Ljava.lang.String;" style arrays
		if (name.startsWith(NON_PRIMITIVE_ARRAY_PREFIX) && name.endsWith(";"))
		{
			final String elementName = name.substring(NON_PRIMITIVE_ARRAY_PREFIX.length(), name.length() - 1);
			final Class<?> elementClass = forName(elementName, classLoader);
			return Array.newInstance(elementClass, 0).getClass();
		}
		// "[[I" or "[[Ljava.lang.String;" style arrays
		if (name.startsWith(INTERNAL_ARRAY_PREFIX))
		{
			final String elementName = name.substring(INTERNAL_ARRAY_PREFIX.length());
			final Class<?> elementClass = forName(elementName, classLoader);
			return Array.newInstance(elementClass, 0).getClass();
		}
		ClassLoader classLoaderToUse = classLoader;
		if (classLoaderToUse == null)
		{
			classLoaderToUse = getDefaultClassLoader();
		}
		try
		{
			return classLoaderToUse.loadClass(name);
		}
		catch (ClassNotFoundException ex)
		{
			final int lastDotIndex = name.lastIndexOf('.');
			if (lastDotIndex != -1) {
				final String innerClassName = name.substring(0, lastDotIndex) + '$' + name.substring(lastDotIndex + 1);
				try
				{
					return classLoaderToUse.loadClass(innerClassName);
				}
				catch (ClassNotFoundException ex2){} // swallow - let original exception get through
			}
			throw ex;
		}
	}
	
	/**
	 * Resolve the given class name as primitive class, if appropriate,
	 * according to the JVM's naming rules for primitive classes.
	 * <p>Also supports the JVM's internal class names for primitive arrays.
	 * Does <i>not</i> support the "[]" suffix notation for primitive arrays;
	 * this is only supported by {@link #forName(String, ClassLoader)}.
	 * 
	 * @param name the name of the potentially primitive class
	 * @return the primitive class, or <code>null</code> if the name does not denote
	 * a primitive class or primitive array class
	 */
	private static Class<?> resolvePrimitiveClassName(final String name)
	{
		Class<?> result = null;
		// Most class names will be quite long, considering that they
		// SHOULD sit in a package, so a length check is worthwhile.
		if (name != null && name.length() <= 8)
		{ // Could be a primitive - likely.
			result = primitiveTypeNameMap.get(name);
		}
		return result;
	}

	private ReflectionUtils(){} // Utility class
}
