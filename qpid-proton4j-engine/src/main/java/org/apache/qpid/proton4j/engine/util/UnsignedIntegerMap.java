/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton4j.engine.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;

/**
 * A {@link Map} implementation that provides both <code>int</code> key access as well
 * as object based {@link UnsignedInteger} keyed access.  The {@link Map} implementation
 * behaves similarly to a {@link LinkedHashMap} implementation providing insertion order
 * access to the elements of the {@link Map}.
 *
 * @param <V> The type that this {@link Map} stores in its values.
 */
public final class UnsignedIntegerMap<V> implements Map<UnsignedInteger, V> {

    /**
     * Rehashing marker that indicates an immediate rehash is needed on next access.
     */
    private static final int REHASHING_THRESHOLD = -1;

    /**
     * Minimum capacity (other than zero) for a UnsignedIntegerMap. Must be a power of two
     * greater than 1 (and less than 1 << 30).
     */
    private static final int MINIMUM_CAPACITY = 4;

    /**
     * Max capacity for a UnsignedIntegerMap. Must be a power of two >= MINIMUM_CAPACITY.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * An empty table shared by all zero-capacity maps (typically from default
     * constructor). It is never written to, and replaced on first put. Its size
     * is set to half the minimum, so that the first resize will create a
     * minimum-sized table.
     */
    @SuppressWarnings("rawtypes")
    private static final Entry[] EMPTY_TABLE = new UnsignedIntegerEntry[MINIMUM_CAPACITY >>> 1];

    /**
     * The default load factor. Note that this implementation ignores the load factor.
     */
    static final float DEFAULT_LOAD_FACTOR = .75F;

    /**
     * The hash table used to access elements from a hashed unsigned integer value.
     */
    private transient UnsignedIntegerEntry<V>[] table;

    /**
     * A dummy entry in the circular linked list of entries in the map.
     * The first real entry is root.next, and the last is header.pervious.
     * If the map is empty, root.next == root && root.previous == root.
     */
    private final transient UnsignedIntegerEntry<V> root = new UnsignedIntegerEntry<>();

    // Views - lazily initialized
    private transient Set<UnsignedInteger> keySet;
    private transient Set<Entry<UnsignedInteger, V>> entrySet;
    private transient Collection<V> values;

    /**
     * The table is rehashed when its size exceeds this threshold.
     * The value of this field is generally .75 * capacity, except when
     * the capacity is zero, as described in the EMPTY_TABLE declaration
     * above.
     */
    private transient int threshold;

    private int size;
    private int modCount;

    /**
     * Creates an empty {@link UnsignedIntegerMap} with default initial capacity sizing.
     */
    @SuppressWarnings("unchecked")
    public UnsignedIntegerMap() {
        table = (UnsignedIntegerEntry<V>[]) EMPTY_TABLE;
        threshold = REHASHING_THRESHOLD;
    }

    /**
     * Creates an empty {@link UnsignedIntegerMap} with this given initial capacity value.
     *
     * @param initialCapacity
     *      The initial capacity that the internal data structure of this mapping should use.
     */
    public UnsignedIntegerMap(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial Map Capacity cannot be negative: " + initialCapacity);
        }

        // TODO
    }

    /**
     * Creates a new {@link UnsignedIntegerMap} which will be filled using the entries from the given {@link Map}
     * instance.  The ordering in the new {@link Map} with match the iteration order of the given {@link Map}.
     *
     * @param source
     *      The {@link Map} instance that will be copied into this instance.
     *
     * @throws NullPointerException if the given {@link Map} instance is null.
     */
    public UnsignedIntegerMap(Map<? extends UnsignedInteger, ? extends V> source) {
        this(source.size());
        // TODO
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key != null) {
            final int keyValue = Number.class.cast(key).intValue();
        }

        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value != null) {
            // Use the linked list of entries to avoid checking every table element when many
            // will be null and instead only check the current contents.
            UnsignedIntegerEntry<V> root = this.root;
            for (UnsignedIntegerEntry<V> entry = root.next; entry != root; entry = entry.next) {
                if (value.equals(entry.value)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public V get(Object key) {
        if (key != null) {
            final int keyValue = Number.class.cast(key).intValue();

            // TODO
        }

        return null;
    }

    @Override
    public V put(UnsignedInteger key, V value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V remove(Object key) {
        if (key != null) {
            final int keyValue = Number.class.cast(key).intValue();

            // TODO
        }

        return null;
    }

    @Override
    public void putAll(Map<? extends UnsignedInteger, ? extends V> m) {
        // TODO Auto-generated method stub

    }

    @Override
    public void clear() {
        if (size != 0) {
            Arrays.fill(table, null);
            modCount++;
            size = 0;

            // Unlink all nodes for safely using a local reference to the list root.
            UnsignedIntegerEntry<V> root = this.root;
            for (UnsignedIntegerEntry<V> current = root.next; current != root; ) {
                UnsignedIntegerEntry<V> next = current.next;
                current.next = current.prev = null;
                current = next;
            }

            root.next = root.prev = root;
        }
    }

    // Once requested we will create an store a single instance to a collection
    // with no state for each of the key, values and entries types.  Since the
    // types are stateless the trivial race on create is not important to the
    // eventual outcome of having a cached instance.

    @Override
    public Set<UnsignedInteger> keySet() {
        if (keySet == null) {
            keySet = new UnsignedIntegerMapKeySet();
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (values == null) {
            values = new UnsignedIntegerMapValues();
        }
        return values;
    }

    @Override
    public Set<Entry<UnsignedInteger, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new UnsignedIntegerMapEntrySet();
        }
        return entrySet;
    }

    //----- Map Iterator implementation for EntrySet, KeySet and Values collections

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class UnsignedIntegerMapIterator<T> implements Iterator<T> {

        private UnsignedIntegerEntry<V> nextNode;
        private UnsignedIntegerEntry<V> lastReturned;

        private int expectedModCount;

        public UnsignedIntegerMapIterator(UnsignedIntegerEntry<V> startAt) {
            this.nextNode = startAt;
            this.expectedModCount = UnsignedIntegerMap.this.modCount;
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        protected UnsignedIntegerEntry<V> nextNode() {
            UnsignedIntegerEntry<V> entry = nextNode;
            if (nextNode == root) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != UnsignedIntegerMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = entry.next;

            return lastReturned = entry;
        }

        // Unused as of now but can be used for NavigableMap amongst other things
        @SuppressWarnings("unused")
        protected UnsignedIntegerEntry<V> previousNode() {
            UnsignedIntegerEntry<V> entry = nextNode;
            if (nextNode == root) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != UnsignedIntegerMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = entry.prev;

            return lastReturned = entry;
        }

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }

            delete(lastReturned);

            expectedModCount = modCount;
            lastReturned = null;
        }
    }

    private class UnsignedIntegerMapEntryIterator extends UnsignedIntegerMapIterator<Entry<UnsignedInteger, V>> {

        public UnsignedIntegerMapEntryIterator(UnsignedIntegerEntry<V> startAt) {
            super(startAt);
        }

        @Override
        public Entry<UnsignedInteger, V> next() {
            return nextNode();
        }
    }

    private class UnsignedIntegerMapKeyIterator extends UnsignedIntegerMapIterator<UnsignedInteger> {

        public UnsignedIntegerMapKeyIterator(UnsignedIntegerEntry<V> startAt) {
            super(startAt);
        }

        @Override
        public UnsignedInteger next() {
            return nextNode().getKey();
        }
    }

    private class UnsignedIntegerMapValueIterator extends UnsignedIntegerMapIterator<V> {

        public UnsignedIntegerMapValueIterator(UnsignedIntegerEntry<V> startAt) {
            super(startAt);
        }

        @Override
        public V next() {
            return nextNode().getValue();
        }
    }

    //----- Splay Map Collection types

    private final class UnsignedIntegerMapValues extends AbstractCollection<V> {

        @Override
        public Iterator<V> iterator() {
            return new UnsignedIntegerMapValueIterator(firstEntry());
        }

        @Override
        public int size() {
            return UnsignedIntegerMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return UnsignedIntegerMap.this.containsValue(o);
        }

        @Override
        public boolean remove(Object target) {
            final UnsignedIntegerEntry<V> root = UnsignedIntegerMap.this.root;

            for (UnsignedIntegerEntry<V> entry = root.next; entry != root; entry = entry.next) {
                if (entry.valueEquals(target)) {
                    delete(entry);
                    return true;
                }
            }

            return false;
        }

        @Override
        public void clear() {
            UnsignedIntegerMap.this.clear();
        }
    }

    private final class UnsignedIntegerMapKeySet extends AbstractSet<UnsignedInteger> {

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return new UnsignedIntegerMapKeyIterator(firstEntry());
        }

        @Override
        public int size() {
            return UnsignedIntegerMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return UnsignedIntegerMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            final UnsignedIntegerEntry<V> root = UnsignedIntegerMap.this.root;

            for (UnsignedIntegerEntry<V> entry = root.next; entry != root; entry = entry.next) {
                if (entry.keyEquals(target)) {
                    delete(entry);
                    return true;
                }
            }

            return false;
        }

        @Override
        public void clear() {
            UnsignedIntegerMap.this.clear();
        }
    }

    private final class UnsignedIntegerMapEntrySet extends AbstractSet<Entry<UnsignedInteger, V>> {

        @Override
        public Iterator<Entry<UnsignedInteger, V>> iterator() {
            return new UnsignedIntegerMapEntryIterator(firstEntry());
        }

        @Override
        public int size() {
            return UnsignedIntegerMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return UnsignedIntegerMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            if (!(target instanceof Entry)) {
                throw new IllegalArgumentException("value provided is not an Entry type.");
            }

            final UnsignedIntegerEntry<V> root = UnsignedIntegerMap.this.root;

            for (UnsignedIntegerEntry<V> entry = root.next; entry != root; entry = entry.next) {
                if (entry.equals(target)) {
                    delete(entry);
                    return true;
                }
            }

            return false;
        }

        @Override
        public void clear() {
            UnsignedIntegerMap.this.clear();
        }
    }

    //----- Map Entry node for the UnsignedInteger Map

    static class UnsignedIntegerEntry<V> implements Entry<UnsignedInteger, V> {

        final int key;
        final int hash;

        V value;
        UnsignedIntegerEntry<V> next;
        UnsignedIntegerEntry<V> prev;

        UnsignedIntegerEntry() {
            this(0, null, 0, null, null);
        }

        UnsignedIntegerEntry(int key, V value, int hash, UnsignedIntegerEntry<V> next, UnsignedIntegerEntry<V> previous) {
            this.key = key;
            this.value = value;
            this.hash = hash;
            this.next = next;
            this.prev = previous;
        }

        @Override
        public final UnsignedInteger getKey() {
            return UnsignedInteger.valueOf(key);
        }

        public int getIntKey() {
            return key;
        }

        @Override
        public final V getValue() {
            return value;
        }

        @Override
        public final V setValue(V value) {
            V oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            Map.Entry<?,?> e = (Map.Entry<?,?>)o;

            return keyEquals(e.getKey()) && valueEquals(e.getValue());
        }

        @Override
        public int hashCode() {
            return key ^ (value == null ? 0 : value.hashCode());
        }

        @Override
        public String toString() {
            return "Node:{" + key + "," + value + "}";
        }

        boolean keyEquals(Object other) {
            if (!(other instanceof Number)) {
                return false;
            }

            return key == ((Number) other).intValue();
        }

        boolean valueEquals(Object other) {
            return value != null ? value.equals(other) : other == null;
        }
    }

    public class ImmutableUnsignedIntegerEntry implements Map.Entry<UnsignedInteger, V> {

        private final UnsignedIntegerEntry<V> entry;

        public ImmutableUnsignedIntegerEntry(UnsignedIntegerEntry<V> entry) {
            this.entry = entry;
        }

        @Override
        public UnsignedInteger getKey() {
            return entry.getKey();
        }

        public int getPrimitiveKey() {
            return entry.getIntKey();
        }

        @Override
        public V getValue() {
            return entry.getValue();
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }

    private ImmutableUnsignedIntegerEntry export(UnsignedIntegerEntry<V> entry) {
        return entry == null ? null : new ImmutableUnsignedIntegerEntry(entry);
    }

    //----- Internal Hash Mapping support methods

    private UnsignedIntegerEntry<V> firstEntry() {
        return root.next;
    }

    private UnsignedIntegerEntry<V> lastEntry() {
        return root.prev;
    }

    private void delete(UnsignedIntegerEntry<V> entry) {
        // TODO - Remove from list and from tabular storage.
    }
}
