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
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;

/**
 * A {@link Map} implementation that provides both <code>int</code> key access as well
 * as object based {@link Number} keyed access which is limited to the range of an unsigned
 * integer value.  The {@link Map} implementation behaves similarly to a {@link LinkedHashMap}
 * implementation providing insertion order access to the elements of the {@link Map}.
 *
 * @param <V> The type that this {@link Map} stores in its values.
 */
public final class SequenceNumberMap<V> implements Map<UnsignedInteger, V> {

    /**
     * The default table size used to hold subsets of the sequence keyed values in the map
     */
    private static final int DEFAULT_TABLE_SIZE = 256;

    /**
     * The minimum number of entries that is allow to be used for sequence number entry table.
     */
    private static final int MINIMUM_TABLE_SIZE = 16;

    /**
     * A dummy entry in the circular linked list of entries in the map.
     * The first real entry is root.next, and the last is header.pervious.
     * If the map is empty, root.next == root && root.previous == root.
     */
    private final transient SequenceEntry<V> entries = new SequenceEntry<>();

    /**
     * The hashed table of {@link SequenceEntry} elements which will be chained as the hashed
     * keys collide to form a layered chain of sequence numbers.
     */
    private final transient SequenceEntry<V>[] entriesTable;

    /**
     * The sequence number bucket size used to allocate the bucket arrays.
     */
    private final int tableSize;

    // Singleton Views - lazily initialized
    private transient Set<UnsignedInteger> keySet;
    private transient Set<Entry<UnsignedInteger, V>> entrySet;
    private transient Collection<V> values;

    private int size;
    private transient int modCount;

    /**
     * Creates an empty {@link SequenceNumberMap} with default initial capacity sizing.
     */
    public SequenceNumberMap() {
        this(DEFAULT_TABLE_SIZE);
    }

    /**
     * Creates an empty {@link SequenceNumberMap} with this given bucket size value.
     *
     * @param tableSize
     *      The size of the sequence number table that hold subsets of the Map values.
     */
    @SuppressWarnings("unchecked")
    public SequenceNumberMap(int tableSize) {
        if (tableSize <= 0) {
            throw new IllegalArgumentException("Initial Map Capacity cannot be negative: " + tableSize);
        }

        this.tableSize = Math.max(tableSize, MINIMUM_TABLE_SIZE);
        this.entriesTable = new SequenceEntry[tableSize];
    }

    /**
     * Creates a new {@link SequenceNumberMap} which will be filled using the entries from the given {@link Map}
     * instance.  The ordering in the new {@link Map} with match the iteration order of the given {@link Map}.
     *
     * @param source
     *      The {@link Map} instance that will be copied into this instance.
     *
     * @throws NullPointerException if the given {@link Map} instance is null.
     */
    public SequenceNumberMap(Map<? extends UnsignedInteger, ? extends V> source) {
        this(DEFAULT_TABLE_SIZE);

        source.forEach((key, value) -> put(key, value));
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
            return containsKey(Number.class.cast(key).intValue());
        } else {
            return false;
        }
    }

    public boolean containsKey(int key) {
        SequenceEntry<V> entry = findEntry(Math.floorMod(key, tableSize), key);
        return entry != null;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value != null) {
            // Use the linked list of entries to avoid checking every table element when many
            // will be null and instead only check the current contents.
            SequenceEntry<V> root = this.entries;
            for (SequenceEntry<V> entry = root.linkNext; entry != root; entry = entry.linkNext) {
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
            return get(Number.class.cast(key).intValue());
        } else {
            return null;
        }
    }

    public V get(int key) {
        SequenceEntry<V> entry = findEntry(Math.floorMod(key, tableSize), key);
        return entry == null ? null : entry.value;
    }

    @Override
    public V put(UnsignedInteger key, V value) {
        if (key != null) {
            return put(key.intValue(), value);
        } else {
            return null;
        }
    }

    public V put(int key, V value) {
        final int tableIndex = Math.floorMod(key, tableSize);

        final SequenceEntry<V>[] table = this.entriesTable;
        SequenceEntry<V> entry = table[tableIndex];
        SequenceEntry<V> predecessor = entry;

        if (entry != null && entry.key != key && (entry = entry.nextEntry) != null) {
            do {
                if (entry.key == key) {
                    break;
                } else {
                    predecessor = entry;
                }
            } while ((entry = entry.nextEntry) != null);
        }

        if (entry == null) {
            entry = insertNewEntry(tableIndex, key, predecessor);
            size++;
        }

        modCount++;

        return entry.setValue(value);
    }

    private final SequenceEntry<V> insertNewEntry(int tableIndex, int key, SequenceEntry<V> predecessor) {
        final SequenceEntry<V>[] table = this.entriesTable;
        SequenceEntry<V> entry = new SequenceEntry<>(tableIndex, key);

        if (predecessor == null) {
            table[tableIndex] = entry;
        } else {
            predecessor.nextEntry = entry;
            entry.prevEntry = predecessor;
        }

        // Insertion ordering of the sequence number entries recorded here
        // and the list of entries doesn't change until an entry is removed.
        entry.linkNext = entries;
        entry.linkPrev = entries.linkPrev;
        entries.linkPrev.linkNext = entry;
        entries.linkPrev = entry;

        return entry;
    }

    @Override
    public V remove(Object key) {
        if (key != null) {
            return remove(Number.class.cast(key).intValue());
        } else {
            return null;
        }
    }

    public V remove(int key) {
        V oldValue = null;

        SequenceEntry<V> entry = findEntry(Math.floorMod(key, tableSize), key);
        if (entry != null) {
            oldValue = entry.value;
            delete(entry);
        }

        return oldValue;
    }

    @Override
    public void putAll(Map<? extends UnsignedInteger, ? extends V> source) {
        source.forEach((key, value) -> put(key, value));
    }

    @Override
    public void clear() {
        if (size != 0) {
            modCount++;
            size = 0;

            // Unlink all buckets and reset to no buckets state.
            Arrays.fill(entriesTable, null);

            // Unlink all the entries and reset to no insertions state
            entries.linkNext = entries.linkPrev = entries;
        }
    }

    // Once requested we will create an store a single instance to a collection
    // with no state for each of the key, values and entries types.  Since the
    // types are stateless the trivial race on create is not important to the
    // eventual outcome of having a cached instance.

    @Override
    public Set<UnsignedInteger> keySet() {
        if (keySet == null) {
            keySet = new SeqeuenceNumberMapKeySet();
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (values == null) {
            values = new SequenceNumberMapValues();
        }
        return values;
    }

    @Override
    public Set<Entry<UnsignedInteger, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new SequenceNumberMapEntrySet();
        }
        return entrySet;
    }

    @Override
    public void forEach(BiConsumer<? super UnsignedInteger, ? super V> action) {
        Objects.requireNonNull(action);

        SequenceEntry<V> root = this.entries;
        for (SequenceEntry<V> entry = root.linkNext; entry != root; entry = entry.linkNext) {
            final UnsignedInteger key;
            final V value;

            try {
                key = entry.getKey();
                value = entry.getValue();
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                throw new ConcurrentModificationException(ise);
            }

            action.accept(key, value);
        }
    }

    /**
     * A specialized forEach implementation that accepts a {@link Consumer} function that will
     * be called for each value in the {@link SequenceNumberMap}.  This method can save overhead
     * as it does not need to box the primitive key values into an object for the call to the
     * provided function.
     *
     * @param action
     *      The action to be performed for each of the values in the {@link SequenceNumberMap}.
     */
    public void forEach(Consumer<? super V> action) {
        Objects.requireNonNull(action);

        SequenceEntry<V> root = this.entries;
        for (SequenceEntry<V> entry = root.linkNext; entry != root; entry = entry.linkNext) {
            final V value;

            try {
                value = entry.getValue();
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                throw new ConcurrentModificationException(ise);
            }

            action.accept(value);
        }
    }

    //----- Internal utility methods

    private void delete(SequenceEntry<V> entry) {
        int tableIndex = entry.tableIndex;

        size--;
        modCount++;

        // Remove the entry from the table
        if (entry.prevEntry != null) {
            entry.prevEntry.nextEntry = entry.nextEntry;
        }

        if (entry.nextEntry != null) {
            entry.nextEntry.prevEntry = entry.prevEntry;
        }

        // No previous entry indicates entry was at the table index root so
        // pull up the next entry (if there is one) to the root.
        if (entry.prevEntry == null) {
            entriesTable[tableIndex] = entry.nextEntry;
        }

        // Remove the entry from the insertion ordered entry list.
        entry.linkNext.linkPrev = entry.linkPrev;
        entry.linkPrev.linkNext = entry.linkNext;
        entry.linkNext = entry.linkPrev = null;
    }

    private SequenceEntry<V> firstEntry() {
        return entries.linkNext;
    }

    private final SequenceEntry<V> findEntry(int tableIndex, int key) {
        final SequenceEntry<V>[] table = this.entriesTable;
        SequenceEntry<V> entry = table[tableIndex];

        if (entry != null && entry.key != key && (entry = entry.nextEntry) != null) {
            do {
                if (entry.key == key) {
                    break;
                }
            } while ((entry = entry.nextEntry) != null);
        }

        return entry;
    }

    //----- Map Entry node for the SeqeuenceNumberMap

    private static class SequenceEntry<V> implements Entry<UnsignedInteger, V> {

        final int key;
        final int tableIndex;

        V value;

        // Insertion order chain
        SequenceEntry<V> linkNext;
        SequenceEntry<V> linkPrev;

        // Table layered chain
        SequenceEntry<V> nextEntry;
        SequenceEntry<V> prevEntry;

        SequenceEntry() {
            this(-1, -1);

            // Node is circular list to start.
            this.linkNext = this;
            this.linkPrev = this;
        }

        SequenceEntry(int tableIndex, int key) {
            this.tableIndex = tableIndex;
            this.key = key;
        }

        @Override
        public final UnsignedInteger getKey() {
            return UnsignedInteger.valueOf(key);
        }

        @SuppressWarnings("unused")
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
            return "SequenceEntry:{" + key + "," + value + "}";
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

    //----- Map Iterator implementation for EntrySet, KeySet and Values collections

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class SequenceNumberMapIterator<T> implements Iterator<T> {

        private SequenceEntry<V> nextEntry;
        private SequenceEntry<V> lastReturned;

        private int expectedModCount;

        public SequenceNumberMapIterator(SequenceEntry<V> startAt) {
            this.nextEntry = startAt;
            this.expectedModCount = SequenceNumberMap.this.modCount;
        }

        @Override
        public boolean hasNext() {
            return nextEntry != entries;
        }

        protected SequenceEntry<V> nextNode() {
            SequenceEntry<V> entry = nextEntry;
            if (nextEntry == entries) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != SequenceNumberMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextEntry = entry.linkNext;

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

    private class SequenceNumberMapEntryIterator extends SequenceNumberMapIterator<Entry<UnsignedInteger, V>> {

        public SequenceNumberMapEntryIterator(SequenceEntry<V> startAt) {
            super(startAt);
        }

        @Override
        public Entry<UnsignedInteger, V> next() {
            return nextNode();
        }
    }

    private class SequenceNumberMapKeyIterator extends SequenceNumberMapIterator<UnsignedInteger> {

        public SequenceNumberMapKeyIterator(SequenceEntry<V> startAt) {
            super(startAt);
        }

        @Override
        public UnsignedInteger next() {
            return nextNode().getKey();
        }
    }

    private class SequenceNumberMapValueIterator extends SequenceNumberMapIterator<V> {

        public SequenceNumberMapValueIterator(SequenceEntry<V> startAt) {
            super(startAt);
        }

        @Override
        public V next() {
            return nextNode().getValue();
        }
    }

    //----- Splay Map Collection types

    private final class SequenceNumberMapValues extends AbstractCollection<V> {

        @Override
        public Iterator<V> iterator() {
            return new SequenceNumberMapValueIterator(firstEntry());
        }

        @Override
        public int size() {
            return SequenceNumberMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return SequenceNumberMap.this.containsValue(o);
        }

        @Override
        public boolean remove(Object target) {
            final SequenceEntry<V> root = SequenceNumberMap.this.entries;

            for (SequenceEntry<V> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.valueEquals(target)) {
                    delete(entry);
                    return true;
                }
            }

            return false;
        }

        @Override
        public void clear() {
            SequenceNumberMap.this.clear();
        }
    }

    private final class SeqeuenceNumberMapKeySet extends AbstractSet<UnsignedInteger> {

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return new SequenceNumberMapKeyIterator(firstEntry());
        }

        @Override
        public int size() {
            return SequenceNumberMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return SequenceNumberMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            final SequenceEntry<V> root = SequenceNumberMap.this.entries;

            for (SequenceEntry<V> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.keyEquals(target)) {
                    delete(entry);
                    return true;
                }
            }

            return false;
        }

        @Override
        public void clear() {
            SequenceNumberMap.this.clear();
        }
    }

    private final class SequenceNumberMapEntrySet extends AbstractSet<Entry<UnsignedInteger, V>> {

        @Override
        public Iterator<Entry<UnsignedInteger, V>> iterator() {
            return new SequenceNumberMapEntryIterator(firstEntry());
        }

        @Override
        public int size() {
            return SequenceNumberMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return SequenceNumberMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            if (!(target instanceof Entry)) {
                throw new IllegalArgumentException("value provided is not an Entry type.");
            }

            final SequenceEntry<V> root = SequenceNumberMap.this.entries;

            for (SequenceEntry<V> entry = root.linkNext; entry != root; entry = entry.linkNext) {
                if (entry.equals(target)) {
                    delete(entry);
                    return true;
                }
            }

            return false;
        }

        @Override
        public void clear() {
            SequenceNumberMap.this.clear();
        }
    }
}
