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
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An implementation of ConcurrentSet that is backed by a ConcurrentHashMap.
 * 
 * @author Greg Haines
 * 
 * @param <E>
 *            the type of elements maintained by this set
 */
public class ConcurrentHashSet<E> implements ConcurrentSet<E>, Serializable {
    
    private static final long serialVersionUID = -7388600825502125963L;

    private enum Nothing {
        NOTHING;
    }

    private final ConcurrentMap<E, Nothing> delegate;

    /**
     * Creates a new, empty set with a default initial capacity (16), load
     * factor (0.75) and concurrencyLevel (16).
     */
    public ConcurrentHashSet() {
        this.delegate = new ConcurrentHashMap<E, Nothing>();
    }

    /**
     * Creates a new, empty set with the specified initial capacity, and with
     * default load factor (0.75) and concurrencyLevel (16).
     * 
     * @param initialCapacity
     *            the initial capacity. The implementation performs internal
     *            sizing to accommodate this many elements.
     */
    public ConcurrentHashSet(final int initialCapacity) {
        this.delegate = new ConcurrentHashMap<E, Nothing>(initialCapacity);
    }

    /**
     * Creates a new, empty set with the specified initial capacity and load
     * factor and with the default concurrencyLevel (16).
     * 
     * @param initialCapacity
     *            the initial capacity. The implementation performs internal
     *            sizing to accommodate this many elements.
     * @param loadFactor
     *            the load factor threshold, used to control resizing. Resizing
     *            may be performed when the average number of elements per bin
     *            exceeds this threshold.
     */
    public ConcurrentHashSet(final int initialCapacity, final float loadFactor) {
        this.delegate = new ConcurrentHashMap<E, Nothing>(initialCapacity, loadFactor);
    }

    /**
     * Creates a new, empty set with the specified initial capacity, load factor
     * and concurrency level.
     * 
     * @param initialCapacity
     *            the initial capacity. The implementation performs internal
     *            sizing to accommodate this many elements.
     * @param loadFactor
     *            the load factor threshold, used to control resizing. Resizing
     *            may be performed when the average number of elements per bin
     *            exceeds this threshold.
     * @param concurrencyLevel
     *            the estimated number of concurrently updating threads. The
     *            implementation performs internal sizing to try to accommodate
     *            this many threads.
     */
    public ConcurrentHashSet(final int initialCapacity, final float loadFactor, final int concurrencyLevel) {
        this.delegate = new ConcurrentHashMap<E, Nothing>(initialCapacity, loadFactor, concurrencyLevel);
    }

    /**
     * Creates a new set with the same entries as the given collection. The set
     * is created with a capacity of 1.5 times the number of entries in the
     * given collection or 16 (whichever is greater), and a default load factor
     * (0.75) and concurrencyLevel (16).
     * 
     * @param collection
     *            the collection
     */
    public ConcurrentHashSet(final Collection<? extends E> collection) {
        if (collection == null) {
            throw new IllegalArgumentException("collection must not be null");
        }
        this.delegate = new ConcurrentHashMap<E, Nothing>(Math.max(16, Math.round(collection.size() * 1.5f)));
        for (final E elem : collection) {
            this.delegate.put(elem, Nothing.NOTHING);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return this.delegate.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return this.delegate.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final Object obj) {
        return this.delegate.containsKey(obj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<E> iterator() {
        return this.delegate.keySet().iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        return this.delegate.keySet().toArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(final T[] array) {
        return this.delegate.keySet().toArray(array);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(final E elem) {
        return (this.delegate.put(elem, Nothing.NOTHING) == null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Object obj) {
        return (this.delegate.remove(obj) != null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(final Collection<?> collection) {
        return this.delegate.keySet().containsAll(collection);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(final Collection<? extends E> collection) {
        boolean changed = false;
        for (final E elem : collection) {
            changed |= add(elem);
        }
        return changed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(final Collection<?> collection) {
        return this.delegate.keySet().retainAll(collection);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(final Collection<?> collection) {
        return this.delegate.keySet().removeAll(collection);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        this.delegate.clear();
    }
}
