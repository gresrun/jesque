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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Reflection utilities.
 * 
 * @author Greg Haines
 */
public final class ReflectionUtils
{
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
	public static <T> T createObject(final Class<T> clazz, final Object[] args)
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
	@SuppressWarnings({"rawtypes", "unchecked"})
	private static <T> Constructor<T> findConstructor(final Class<T> clazz, final Object[] args)
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

	private ReflectionUtils(){} // Utility class
}
