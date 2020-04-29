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
     * The default bucket size used to hold subsets of the sequence keyed values in the map
     */
    private static final int DEFAULT_BUCKET_SIZE = 256;

    /**
     * The minimum number of entries that is allow to be used for sequence number entry buckets.
     */
    private static final int MINIMUM_BUCKET_SIZE = 16;

    /**
     * A dummy entry in the circular linked list of entries in the map.
     * The first real entry is root.next, and the last is header.pervious.
     * If the map is empty, root.next == root && root.previous == root.
     */
    private final transient SequenceEntry<V> entries = new SequenceEntry<>();

    /**
     * A dummy entry in the chain of map buckets that provides a constant fixed
     * starting point for searches or other access operations that need bucket
     * iteration.  The real first bucket is located at buckets.next.
     */
    private final transient SequenceNumberBucket<V> buckets = new SequenceNumberBucket<>();

    /**
     * When a bucket is emptied it can be stored here so that the next put operations can try and
     * use an already allocated empty bucket instead of creating a new one as the progression of
     * sequence number usage moves forward.
     */
    private transient SequenceNumberBucket<V> spareBucket;

    /**
     * The sequence number bucket size used to allocate the bucket arrays.
     */
    private final int bucketSize;

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
        this(DEFAULT_BUCKET_SIZE);
    }

    /**
     * Creates an empty {@link SequenceNumberMap} with this given bucket size value.
     *
     * @param bucketSize
     *      The size of the sequence number buckets that hold subsets of the Map values.
     */
    public SequenceNumberMap(int bucketSize) {
        if (bucketSize <= 0) {
            throw new IllegalArgumentException("Initial Map Capacity cannot be negative: " + bucketSize);
        }

        this.bucketSize = Math.max(bucketSize, MINIMUM_BUCKET_SIZE);

        // Allocate a spare to be use to house the initial put operations which can start at any
        // point in the sequence number range but will likely move forward from there.
        this.spareBucket = new SequenceNumberBucket<>(-1, this.bucketSize);
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
        this(DEFAULT_BUCKET_SIZE);

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
        final long longKey = Integer.toUnsignedLong(key);
        final int bucketIndex = (int) (longKey / bucketSize);
        final int buecketOffset = (int) (longKey % bucketSize);

        final SequenceNumberBucket<V> bucket = findBucketBackward(bucketIndex);
        if (bucket != null) {
            final SequenceEntry<V> entry = bucket.get(buecketOffset);

            if (entry != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value != null) {
            // Use the linked list of entries to avoid checking every table element when many
            // will be null and instead only check the current contents.
            SequenceEntry<V> root = this.entries;
            for (SequenceEntry<V> entry = root.next; entry != root; entry = entry.next) {
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
        final int bucketIndex = Integer.divideUnsigned(key, bucketSize);
        final int bucketOffset = Math.floorMod(key, bucketSize);

        final SequenceNumberBucket<V> bucket = findBucketBackward(bucketIndex);
        if (bucket != null) {
            final SequenceEntry<V> entry = bucket.get(bucketOffset);

            if (entry != null) {
                return entry.getValue();
            }
        }

        return null;
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
        final int bucketIndex = Integer.divideUnsigned(key, bucketSize);
        final int bucketOffset = Math.floorMod(key, bucketSize);

        final SequenceNumberBucket<V> bucket = findOrCreateBucket(bucketIndex);

        SequenceEntry<V> entry = bucket.get(bucketOffset);
        if (entry == null) {
            entry = new SequenceEntry<>(key, bucket, bucketOffset);
            bucket.put(bucketOffset, entry);
            size++;
            // Insertion ordering of the sequence number entries recorded here
            // and the list of entries doesn't change until an entry is removed.
            entry.next = entries;
            entry.prev = entries.prev;
            entries.prev.next = entry;
            entries.prev = entry;
        }

        modCount++;

        return entry.setValue(value);
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
        final int bucketIndex = Integer.divideUnsigned(key, bucketSize);
        final int bucketOffset = Math.floorMod(key, bucketSize);

        V oldValue = null;

        final SequenceNumberBucket<V> bucket = findBucketForward(bucketIndex);
        if (bucket != null) {
            final SequenceEntry<V> entry = bucket.get(bucketOffset);
            if (entry != null) {
                oldValue = entry.getValue();
                delete(entry);
            }
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

            // When there isn't a spare already try and reclaim one before unlinking
            // the buckets from the list.
            if (spareBucket == null && buckets.next != buckets) {
                SequenceNumberBucket<V> hotSpare = buckets.next;
                Arrays.fill(hotSpare.entries, null);

                hotSpare.next = hotSpare.prev = hotSpare;
                hotSpare.bucketIndex = -1;

                this.spareBucket = hotSpare;
            }

            // Unlink all buckets and reset to no buckets state.
            buckets.next = buckets.prev = buckets;

            // Unlink all the entries and reset to no insertions state
            entries.next = entries.prev = entries;
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
        for (SequenceEntry<V> entry = root.next; entry != root; entry = entry.next) {
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
        for (SequenceEntry<V> entry = root.next; entry != root; entry = entry.next) {
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
        final SequenceNumberBucket<V> bucket = entry.bucket;

        // Remove the entry from the bucket
        bucket.remove(entry.bucketOffset);

        size--;
        modCount++;

        if (bucket.isEmpty()) {
            bucket.bucketIndex = -1;
            // Unlink this bucket from the chain
            bucket.next.prev = bucket.prev;
            bucket.prev.next = bucket.next;
            // Clear old links to avoid retention
            bucket.next = bucket.prev = bucket;

            if (spareBucket == null) {
                spareBucket = bucket;
            }
        }

        // Remove the entry from the insertion ordered entry list.
        entry.next.prev = entry.prev;
        entry.prev.next = entry.next;
        entry.next = entry.prev = entry;
    }

    private SequenceNumberBucket<V> findBucketBackward(int bucketIndex) {
        for (SequenceNumberBucket<V> current = buckets.prev; current != buckets; current = current.prev) {
            if (current.index() == bucketIndex) {
                return current;
            } else if (current.index() < bucketIndex) {
                break;
            }
         }

        return null;
    }

    private SequenceNumberBucket<V> findBucketForward(int bucketIndex) {
        for (SequenceNumberBucket<V> current = buckets.next; current != buckets; current = current.next) {
            if (current.index() == bucketIndex) {
                return current;
            } else if (current.index() > bucketIndex) {
                break;
            }
         }

        return null;
    }

    private SequenceNumberBucket<V> findOrCreateBucket(int bucketIndex) {
        SequenceNumberBucket<V> successor = buckets;

        for (SequenceNumberBucket<V> current = buckets.prev; current != buckets; current = current.prev) {
            if (current.index() == bucketIndex) {
                return current;
            } else if (current.index() < bucketIndex) {
                break;
            }

            successor = current;
         }

        final SequenceNumberBucket<V> bucket;
        if (spareBucket != null) {
            bucket = spareBucket;
            bucket.bucketIndex = bucketIndex;
            spareBucket = null;
        } else {
            bucket = new SequenceNumberBucket<>(bucketIndex, bucketSize);
        }

        // insert this new bucket into the chain updating the links on both sides..
        bucket.next = successor;
        bucket.prev = successor.prev;
        successor.prev.next = bucket;
        successor.prev = bucket;

        return bucket;
    }

    private SequenceEntry<V> firstEntry() {
        return entries.next;
    }

    //----- Map bucket for a fixed chunk of the entries in the SequenceNumberMap

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static class SequenceNumberBucket<V> {

        private static final SequenceEntry[] EMPTY_BUCKET = new SequenceEntry[0];

        private final SequenceEntry<V>[] entries;

        private int bucketIndex;

        private SequenceNumberBucket<V> next;
        private SequenceNumberBucket<V> prev;

        private int size;

        public SequenceNumberBucket() {
            this(-1, 0);
        }

        public SequenceNumberBucket(int bucketIndex, int bucketSize) {
            // Empty node is circular list to start.
            this.next = this;
            this.prev = this;

            this.bucketIndex = bucketIndex;

            if (bucketSize != 0) {
                this.entries = new SequenceEntry[bucketSize];
            } else {
                this.entries = EMPTY_BUCKET;
            }
        }

        public boolean isEmpty() {
            return size == 0;
        }

        public void put(int key, SequenceEntry<V> value) {
            entries[key] = value;
            size++;
        }

        public SequenceEntry<V> get(int key) {
            return entries[key];
        }

        public SequenceEntry<V> remove(int key) {
            final SequenceEntry<V> entry = entries[key];

            if (entry != null) {
                size--;
            }

            return entry;
        }

        public int index() {
            return bucketIndex;
        }

        @Override
        public String toString() {
            return "SequenceNumberBucket:{" + bucketIndex + ", size=" + size + "}";
        }
    }

    //----- Map Entry node for the SeqeuenceNumberMap

    private static class SequenceEntry<V> implements Entry<UnsignedInteger, V> {

        final int key;

        V value;
        SequenceEntry<V> next;
        SequenceEntry<V> prev;

        // Locator data for faster access from the buckets
        final SequenceNumberBucket<V> bucket;
        final int bucketOffset;

        SequenceEntry() {
            this(-1, null, -1);

            // Empty node is circular list to start.
            this.next = this;
            this.prev = this;
        }

        SequenceEntry(int key, SequenceNumberBucket<V> bucket, int bucketOffset) {
            this.key = key;
            this.bucket = bucket;
            this.bucketOffset = bucketOffset;
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

        private SequenceEntry<V> nextNode;
        private SequenceEntry<V> lastReturned;

        private int expectedModCount;

        public SequenceNumberMapIterator(SequenceEntry<V> startAt) {
            this.nextNode = startAt;
            this.expectedModCount = SequenceNumberMap.this.modCount;
        }

        @Override
        public boolean hasNext() {
            return nextNode != entries;
        }

        protected SequenceEntry<V> nextNode() {
            SequenceEntry<V> entry = nextNode;
            if (nextNode == entries) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != SequenceNumberMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            nextNode = entry.next;

            return lastReturned = entry;
        }

        // Unused as of now but can be used for NavigableMap amongst other things
        @SuppressWarnings("unused")
        protected SequenceEntry<V> previousNode() {
            SequenceEntry<V> entry = nextNode;
            if (nextNode == entries) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != SequenceNumberMap.this.modCount) {
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

            for (SequenceEntry<V> entry = root.next; entry != root; entry = entry.next) {
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

            for (SequenceEntry<V> entry = root.next; entry != root; entry = entry.next) {
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

            for (SequenceEntry<V> entry = root.next; entry != root; entry = entry.next) {
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
