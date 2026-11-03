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

package org.apache.qpid.protonj2.engine.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.types.UnsignedInteger;

/**
 * A specialized collection like entity that is used to keep track of unsettled
 * incoming and outgoing deliveries for AMQP links and the sessions that manage
 * those links.
 * <p>
 * While this class implements the {@link Map} interface it does not fully behave
 * as a standard map as it solves the issue of tracking delivery IDs which always
 * increase until such time as the unsigned integer value used to represent them
 * overflows. It is possible (although unlikely) for the same ID to exist for multiple
 * deliveries if an older delivery exists in the Map and the delivery IDs overflow and
 * the same value is produced once again.
 *
 * @param <Delivery> The delivery type being tracked (incoming or outgoing)
 */
public class UnsettledMap<Delivery> implements Map<UnsignedInteger, Delivery> {

    public interface UnsettledGetDeliveryId<Delivery> {
        int getDeliveryId(Delivery delivery);
    }

    private static final double BUCKET_LOAD_FACTOR_MULTIPLIER = 0.25;

    private static final int UNSETTLED_INITIAL_BUCKETS = 2;
    private static final int UNSETTLED_BUCKET_SIZE = 256;

    // Allow the free list to grow as needed but establish a maximum size before
    // recycled buckets are allowed to be garbage collected once no longer used.
    private static final int FREE_LIST_GROWTH_AMOUNT = 2;
    private static final int FREE_LIST_SIZE_LIMIT = 8;

    // Always full buckets used in operations that needs unwritable bounds
    private final UnsettledBucket<Delivery> ALWAYS_FULL_BUCKET = new UnsettledBucket<>();

    private final UnsettledGetDeliveryId<Delivery> deliveryIdSupplier;
    private final int bucketCapacity;
    private final int bucketLowWaterMark;

    private int size;
    private int modCount;
    private int generations;
    private int freeListSize;

    private UnsettledBucket<Delivery> head; // Where new puts begin from
    private UnsettledBucket<Delivery> tail; // Where all gets start searching from
    private UnsettledBucket<Delivery> free; // Stack of free buckets

    public UnsettledMap(UnsettledGetDeliveryId<Delivery> idSupplier) {
        this(idSupplier, UNSETTLED_INITIAL_BUCKETS, UNSETTLED_BUCKET_SIZE);
    }

    public UnsettledMap(UnsettledGetDeliveryId<Delivery> idSupplier, int initialBuckets) {
        this(idSupplier, initialBuckets, UNSETTLED_BUCKET_SIZE);
    }

    public UnsettledMap(UnsettledGetDeliveryId<Delivery> idSupplier, int initialBuckets, int bucketSize) {
        this.deliveryIdSupplier = idSupplier;
        this.bucketCapacity = bucketSize;
        this.bucketLowWaterMark = (int) (bucketSize * BUCKET_LOAD_FACTOR_MULTIPLIER);
        this.freeListSize = initialBuckets;

        if (bucketSize < 1) {
            throw new IllegalArgumentException("The bucket size must be greater than zero");
        }

        if (initialBuckets < 1) {
            throw new IllegalArgumentException("The initial number of buckets must be at least 1");
        }

        // All initial buckets go onto the free list
        free = new UnsettledBucket<Delivery>(bucketCapacity);
        for (int i = 1; i < initialBuckets; ++i) {
            UnsettledBucket<Delivery> newFree = new UnsettledBucket<>(bucketCapacity);
            newFree.prev = free;
            free = newFree;
        }
    }

    @Override
    public void putAll(Map<? extends UnsignedInteger, ? extends Delivery> source) {
        source.entrySet().forEach(entry -> put(entry.getKey(), entry.getValue()));
    }

    @Override
    public void clear() {
        for (UnsettledBucket<Delivery> bucket = tail; bucket != null; bucket = bucket.next) {
            bucket.clear();
            if (freeListSize < FREE_LIST_SIZE_LIMIT) {
                bucket.prev = free;
                free = bucket;
                freeListSize++;
            }
        }

        head = tail = null;
        size = 0;
        modCount++;
    }

    @Override
    public Delivery put(UnsignedInteger key, Delivery value) {
        return put(key.intValue(), value);
    }

    /**
     * Adds the given key and value pair in this tracking structure at the end of the current series.
     * <p>
     * If the map previously contained a mapping for the key, the old value is not replaced by the specified
     * value unlike a traditional map as this structure is tracking the running series of values. This would
     * imply that duplicates can exist in the tracker, however given the likelihood of this occurring in the
     * normal flow of deliveries should be considered extremely low.
     *
     * @param deliveryId
     * 		The delivery ID of the delivery being added to this tracker.
     * @param delivery
     * 		The delivery that is being added to the tracker
     *
     * @return <code>null</code> in all cases as this Map type does not check for duplicates.
     */
    public Delivery put(int deliveryId, Delivery delivery) {
        UnsettledBucket<Delivery> bucket = this.head;

        if (bucket != null) {
            if (Integer.compareUnsigned(deliveryId, bucket.highestDeliveryId) <= 0) {
                generations = Math.max(0, generations + 1);
                bucket = advanceHead();
            } else if (bucket.getFreeSpace() == 0) {
                bucket = advanceHead();
            }
        } else {
            bucket = advanceHead();
        }

        bucket.generation = generations;
        bucket.put(deliveryId, delivery);
        size++;
        modCount++;

        return null;
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
    public Delivery get(Object key) {
        if (key instanceof Number) {
            return get(((Number) key).intValue());
        } else {
            return null;
        }
    }

    public Delivery get(UnsignedInteger key) {
        return get(key.intValue());
    }

    public Delivery get(int deliveryId) {
        return findDelivery(deliveryId, false);
    }

    @Override
    public Delivery remove(Object key) {
        if (key instanceof Number) {
            return remove(((Number) key).intValue());
        } else {
            return null;
        }
    }

    public Delivery remove(UnsignedInteger key) {
        return remove(key.intValue());
    }

    public Delivery remove(int deliveryId) {
        return findDelivery(deliveryId, true);
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof Number) {
            return containsKey(((Number) key).intValue());
        } else {
            return false;
        }
    }

    public boolean containsKey(UnsignedInteger key) {
        return containsKey(key.intValue());
    }

    public boolean containsKey(int deliveryId) {
        return findDelivery(deliveryId, false) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value != null && size > 0) {
            for (UnsettledBucket<Delivery> bucket = tail; bucket != null; bucket = bucket.next) {
                final int writeOffset = bucket.writeOffset;
                final Delivery[] deliveries = bucket.deliveries;

                for (int j = bucket.readOffset; j < writeOffset; ++j) {
                    if (value.equals(deliveries[j])) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Visits each entry within the {@link UnsettledMap} and invokes the provided action
     * on each delivery in the tracker.
     *
     * @param action
     * 		The action to invoke on each visited entry.
     */
    public void forEach(Consumer<Delivery> action) {
        Objects.requireNonNull(action);

        if (size == 0) {
            return;
        }

        for (UnsettledBucket<Delivery> bucket = tail; bucket != null; bucket = bucket.next) {
            final int writeOffset = bucket.writeOffset;
            final Delivery[] deliveries = bucket.deliveries;

            for (int j = bucket.readOffset; j < writeOffset; ++j) {
                action.accept(deliveries[j]);
            }
        }
    }

    /**
     * Visits each entry within the given range and invokes the provided action on
     * each delivery in the tracker. The lower and upper bounds are limits which are
     * not exclusive meaning entries within this range are visited regardless of their
     * being an exact match on the lower and upper boundaries.
     *
     * @param first
     * 		The lower bound of the first entry to visit.
     * @param last
     * 		The upper bound of the last entry to visit.
     * @param action
     * 		The action to invoke on each visited entry.
     */
    public void forEach(int first, int last, Consumer<Delivery> action) {
        Objects.requireNonNull(action);

        if (size == 0) {
            return;
        }

        // There are two cases here, either first <= last or first > last which
        // involves two cycles one for each range before and after the overflow.
        if (Integer.compareUnsigned(first, last) <= 0) {
            forEachInRange(tail, false, first, last, action);
        } else {
            final UnsettledBucket<Delivery> nextGeneration =
                forEachInRange(tail, false, first, UnsignedInteger.MAX_VALUE.intValue(), action);

            if (nextGeneration != null) {
                forEachInRange(nextGeneration, true, 0, last, action);
            }
        }
    }

    private UnsettledBucket<Delivery> forEachInRange(UnsettledBucket<Delivery> tail, boolean continuation, int first, int last, Consumer<Delivery> action) {
        boolean foundFirst = false;
        boolean foundLast = false;
        int inGeneration = continuation ? tail.generation : -1;

        UnsettledBucket<Delivery> bucket = tail;

        for (; bucket != null && !foundLast; bucket = bucket.next) {
            final int writeOffset = bucket.writeOffset;

            int readStart = bucket.readOffset;

            // We are locked to a ranged search in a single generation so if values
            // jump we can stop and return where the next generation starts in case
            // the caller has a second range to check.
            if (inGeneration >= 0 && bucket.generation != inGeneration) {
                return bucket;
            }

            if (!foundFirst) {
                if (bucket.isCapturedByRange(first, last)) {
                    final int result = bucket.search(first);
                    final int ceiling = result >= 0 ? result : ~result;

                    // We either found the actual first or we found the location where it would
                    // be inserted at which is the next logical value we will use as the start
                    // and proceed with all values from that point until we find the end or an
                    // element greater than the last value.

                    if (ceiling < writeOffset) {
                        foundFirst = true;
                        readStart = ceiling;
                        inGeneration = bucket.generation;
                    }
                } else if (continuation && bucket.lowestDeliveryId > first) {
                    // If continued from a range that include overflow and we find values ahead of
                    // the lowest point in this sequence we know there can be no matches so stop now.
                    return null;
                }
            }

            if (foundFirst) {
                final Delivery[] deliveries = bucket.deliveries;
                final int[] deliveryIds = bucket.deliveryIds;

                for (int j = readStart; j < writeOffset && !foundLast;) {
                    final int candidate = deliveryIds[j];
                    final int comparison = Integer.compareUnsigned(candidate, last);

                    if (comparison <= 0) {
                        action.accept(deliveries[j++]);
                    }

                    foundLast = comparison >= 0;
                }
            }
        }

        return bucket;
    }

    /**
     * Remove each entry within the given range of delivery IDs. For each entry
     * removed the provided action is triggered allowing the caller to be notified
     * of each removal.
     *
     * @param first
     * 		The first entry to remove
     * @param last
     *      The last entry to remove
     * @param action
     * 		The action to invoke on each remove.
     */
    public void removeEach(int first, int last, Consumer<Delivery> action) {
        Objects.requireNonNull(action);

        if (size == 0) {
            return;
        }

        // There are two cases here, either first <= last or first > last which
        // involves two cycles one for each range before and after the overflow.
        if (Integer.compareUnsigned(first, last) <= 0) {
            removeEachInRange(tail, false, first, last, action);
        } else {
            final UnsettledBucket<Delivery> nextGeneration =
                removeEachInRange(tail, false, first, UnsignedInteger.MAX_VALUE.intValue(), action);

            if (nextGeneration != null) {
                removeEachInRange(nextGeneration, true, 0, last, action);
            }
        }
    }

    private UnsettledBucket<Delivery> removeEachInRange(UnsettledBucket<Delivery> tail, boolean continuation, int first, int last, Consumer<Delivery> action) {
        boolean foundFirst = false;
        boolean foundLast = false;
        int removeStart = 0;
        int removeEnd = 0;
        int inGeneration = continuation ? tail.generation : -1;

        UnsettledBucket<Delivery> bucket = tail;

        for (; bucket != null && !foundLast; ) {
            final int writeOffset = bucket.writeOffset;

            removeStart = bucket.readOffset;

            // We are locked to a ranged search in a single generation so if values
            // jump we can stop and return where the next generation starts in case
            // the caller has a second range to check.
            if (inGeneration >= 0 && bucket.generation != inGeneration) {
                return bucket;
            }

            if (!foundFirst) {
                if (bucket.isCapturedByRange(first, last)) {
                    final int result = bucket.search(first);
                    final int ceiling = result >= 0 ? result : ~result;

                    // We either found the actual first or we found the location where it would
                    // be inserted at which is the next logical value we will use as the start
                    // and proceed with all values from that point until we find the end or an
                    // element greater than the last value.

                    if (ceiling < writeOffset) {
                        foundFirst = true;
                        removeStart = ceiling;
                        inGeneration = bucket.generation;
                    }
                } else if (continuation && bucket.lowestDeliveryId > first) {
                    // If continued from a range that include overflow and we find values ahead of
                    // the lowest point in this sequence we know there can be no matches so stop now.
                    return null;
                }
            }

            if (foundFirst) {
                final Delivery[] deliveries = bucket.deliveries;
                final int[] deliveryIds = bucket.deliveryIds;

                for (removeEnd = removeStart; removeEnd < writeOffset && !foundLast;) {
                    final int candidate = deliveryIds[removeEnd];
                    final int comparison = Integer.compareUnsigned(candidate, last);

                    if (comparison <= 0) {
                        action.accept(deliveries[removeEnd++]);
                    }

                    foundLast = comparison >= 0;
                }

                bucket = removeRange(bucket, removeStart, removeEnd, foundLast);
            } else {
                bucket = bucket.next;
            }
        }

        return bucket;
    }

    @Override
    public void forEach(BiConsumer<? super UnsignedInteger, ? super Delivery> action) {
        Objects.requireNonNull(action);

        if (size == 0) {
            return;
        }

        for (UnsettledBucket<Delivery> bucket = tail; bucket != null; bucket = bucket.next) {
            final int writeOffset = bucket.writeOffset;
            final Delivery[] deliveries = bucket.deliveries;
            final int[] deliveryIds = bucket.deliveryIds;

            for (int j = bucket.readOffset; j < writeOffset; ++j) {
                action.accept(UnsignedInteger.valueOf(deliveryIds[j]), deliveries[j]);
            }
        }
    }

    @Override
    public Collection<Delivery> values() {
        if (values == null) {
            values = new UnsettledTackingMapValues();
        }

        return values;
    }

    @Override
    public Set<UnsignedInteger> keySet() {
        if (keySet == null) {
            keySet = new UnsettledTackingMapKeys();
        }

        return this.keySet;
    }

    @Override
    public Set<Entry<UnsignedInteger, Delivery>> entrySet() {
        if (entrySet == null) {
            entrySet = new UnsettledTackingMapEntries();
        }

        return this.entrySet;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }

        Map<?,?> m = (Map<?,?>) o;
        if (m.size() != size()) {
            return false;
        }

        try {
            for (UnsettledBucket<Delivery> bucket = tail; bucket != null; bucket = bucket.next) {
                for (int j = bucket.readOffset; j < bucket.writeOffset; ++j) {
                    final Delivery delivery = bucket.deliveries[j];
                    if (!delivery.equals(m.get(bucket.deliveryIds[j]))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException ignored) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "UnsettledMap: { size=" + size +
                              " bucket-capacity=" + bucketCapacity + " }";
    }

    //----- Internal UnsettledMap API

    private Delivery findDelivery(int deliveryId, boolean remove) {
        if (size == 0) {
            return null;
        }

        final boolean hasNotOverflowed = generations == 0;
        final int globalLow = tail.lowestDeliveryId;
        final int globalHigh = head.highestDeliveryId;

        if (hasNotOverflowed) {
            // When there is no overflow in the map we can fast path check for the delivery
            // being outside the global range and exit early. All other cases of overflow
            // requires some caution and we search all the buckets for a match.
            if (Integer.compareUnsigned(deliveryId, globalLow) < 0 ||
                Integer.compareUnsigned(deliveryId, globalHigh) > 0) {
                return null;
            }
        }

        final UnsettledBucket<Delivery> tail = this.tail;

        if (tail.isInRange(deliveryId)) {
            final int deliveryIndex = tail.search(deliveryId);
            if (deliveryIndex >= 0) {
                return remove ? getAndRemove(tail, deliveryIndex) : tail.deliveries[deliveryIndex];
            }
        }

        if (head == tail) {
            return null;
        }

        if (hasNotOverflowed) {
            final int distFromTail = deliveryId - globalLow;
            final int distToHead = globalHigh - deliveryId;

            // We can only search from head if there are no active overflows, which is indicated
            // by generations being greater than zero.
            if (Integer.compareUnsigned(distFromTail, distToHead) > 0) {
                return searchBackwards(deliveryId, head, remove);
            }
        }

        return searchForwards(deliveryId, tail.next, remove, hasNotOverflowed);
    }

    private Delivery searchForwards(int deliveryId, UnsettledBucket<Delivery> target, boolean remove, boolean canStopEarly) {
        for (; target != null; target = target.next) {
            if (canStopEarly && Integer.compareUnsigned(target.lowestDeliveryId, deliveryId) > 0) {
                break;
            }

            if (Integer.compareUnsigned(deliveryId, target.lowestDeliveryId) < 0) {
                continue;
            }

            if (Integer.compareUnsigned(deliveryId, target.highestDeliveryId) > 0) {
                continue;
            }

            final int index = target.search(deliveryId);

            if (index >= 0) {
                return remove ? getAndRemove(target, index) : target.deliveries[index];
            }
        }

        return null;
    }

    private Delivery searchBackwards(int deliveryId, UnsettledBucket<Delivery> target, boolean remove) {
        for (; target != null; target = target.prev) {
            if (Integer.compareUnsigned(target.highestDeliveryId, deliveryId) < 0) {
                break;
            }

            if (Integer.compareUnsigned(deliveryId, target.lowestDeliveryId) < 0) {
                continue;
            }

            if (Integer.compareUnsigned(deliveryId, target.highestDeliveryId) > 0) {
                continue;
            }

            final int index = target.search(deliveryId);

            if (index >= 0) {
                return remove ? getAndRemove(target, index) : target.deliveries[index];
            }
        }

        return null;
    }

    private Delivery getAndRemove(UnsettledBucket<Delivery> bucket, int index) {
        final Delivery delivery = bucket.deliveries[index];

        bucket.removeIndex(index);

        size--;
        modCount++;

        if (bucket.entries <= bucketLowWaterMark) {
            tryCompact(bucket);
        }

        return delivery;
    }

    private UnsettledBucket<Delivery> advanceHead() {
        if (free == null) {
            for (int i = 0; i < FREE_LIST_GROWTH_AMOUNT; i++) {
                UnsettledBucket<Delivery> bucket = new UnsettledBucket<Delivery>(bucketCapacity);

                bucket.prev = free;
                free = bucket;
            }

            freeListSize = FREE_LIST_GROWTH_AMOUNT;
        }

        // Pop a bucket off the free list
        UnsettledBucket<Delivery> popped = free;
        free = popped.prev;
        freeListSize--;

        if (head == null) {
            popped.prev = null;
            head = popped;
            tail = popped;
        } else if (head == tail) {
            head = popped;
            head.prev = tail;
            tail.next = head;
        } else {
            head.next = popped;
            popped.prev = head;
            head = popped;
        }

        return head;
    }

    private UnsettledBucket<Delivery> removeRange(UnsettledBucket<Delivery> bucket, int start, int end, boolean compact) {
        final int removals = end - start;
        final UnsettledBucket<Delivery> next = bucket.next;

        this.size -= removals;
        this.modCount++;

        if (removals == bucket.entries) {
            recycleBucket(bucket);
        } else {
            System.arraycopy(bucket.deliveries, end, bucket.deliveries, start, bucket.writeOffset - end);
            System.arraycopy(bucket.deliveryIds, end, bucket.deliveryIds, start, bucket.writeOffset - end);
            Arrays.fill(bucket.deliveries, bucket.writeOffset - removals, bucket.writeOffset, null);

            bucket.writeOffset = bucket.writeOffset - removals;
            bucket.entries -= removals;
            bucket.highestDeliveryId = bucket.deliveryIds[bucket.writeOffset - 1];
            bucket.lowestDeliveryId = bucket.deliveryIds[bucket.readOffset];

            if (compact) {
                tryCompact(bucket);
            }
        }

        return next;
    }

    private void recycleBucket(UnsettledBucket<Delivery> bucket) {
        final UnsettledBucket<Delivery> bucketNext = bucket.next;
        final UnsettledBucket<Delivery> bucketPrev = bucket.prev;

        final int nextGeneration = bucketNext == null ? -1 : bucketNext.generation;
        final int prevGeneration = bucketPrev == null ? -1 : bucketPrev.generation;

        // This is the last of its generation so we decrease the tracked generations which can
        // allow search operations to optimize if they know there isn't any overflow in the map
        // when they are running.
        if (prevGeneration != bucket.generation && nextGeneration != bucket.generation) {
            generations = Math.max(0, generations - 1);
        }

        if (bucket == head) {
            head = bucketPrev;
        }

        if (bucket == tail) {
            tail = bucketNext;
        }

        if (bucketNext != null) {
            bucketNext.prev = bucketPrev;
        }

        if (bucketPrev != null) {
            bucketPrev.next = bucketNext;
        }

        bucket.clear();  // Drop all content and reset as empty bucket

        if (freeListSize < FREE_LIST_SIZE_LIMIT) {
            bucket.prev = free;
            free = bucket;
            freeListSize++;
        }
    }

    // Called from iteration APIs which requires the method to return the location of the next
    // entry once removal and possible bucket compaction is completed.
    private IteratorRemoveResult<Delivery> removeAt(UnsettledBucket<Delivery> bucket, int bucketEntry) {
        final int entriesRead = bucketEntry - bucket.readOffset; // compute now before entries shift

        bucketEntry = bucket.removeIndex(bucketEntry);
        size--;
        modCount++;

        IteratorRemoveResult<Delivery> result = null;

        if (bucket.isReadable()) {
            final UnsettledBucket<Delivery> next = bucket.next;
            final UnsettledBucket<Delivery> prev = bucket.prev;

            final UnsettledBucket<Delivery> nextBucket = (bucket == head || next == null) ||
                Integer.compareUnsigned(bucket.highestDeliveryId, next.lowestDeliveryId) >= 0 ? ALWAYS_FULL_BUCKET : next;
            final UnsettledBucket<Delivery> prevBucket = (bucket == tail || prev == null) ||
                Integer.compareUnsigned(bucket.lowestDeliveryId, prev.highestDeliveryId) <= 0 ? ALWAYS_FULL_BUCKET : prev;

            // As soon as compaction is possible move elements from this bucket into previous and next
            // which reduces search times as there are fewer buckets to traverse/
            if (nextBucket.getFreeSpace() + prevBucket.getFreeSpace() >= bucket.entries) {
                final int toCopyBackward = Math.min(prevBucket.getFreeSpace(), bucket.entries);
                final int readOffset = toCopyBackward - entriesRead;

                doCompaction(bucket, prevBucket, nextBucket);

                recycleBucket(bucket);

                if (readOffset > 0) {
                    result = new IteratorRemoveResult<Delivery>(prevBucket, prevBucket.writeOffset - readOffset);
                } else {
                    result = new IteratorRemoveResult<Delivery>(nextBucket, nextBucket.readOffset - readOffset);
                }
            } else {
                // If there is more to read in the current bucket we can just choose the
                // next element to read, otherwise we must move into the next bucket assuming
                // we aren't at the end of the head bucket since the next bucket could be the
                // tail bucket. We need to check though that the next bucket is readable as it
                // could be the head bucket but there could be nothing in there.
                if (bucketEntry < bucket.writeOffset) {
                    result = new IteratorRemoveResult<Delivery>(bucket, bucketEntry);
                } else if (bucket != head) {
                    result = new IteratorRemoveResult<Delivery>(nextBucket, nextBucket.readOffset);
                }
            }
        } else {
            final UnsettledBucket<Delivery> next = bucket.next;

            // The bucket wasn't head so we either need to move ahead one slot or
            // stay where we are depending on the outcome of recycle. The only case
            // where we stay on the current index is if head retracts as the next
            // bucket is now in the slot slot we just recycle.
            recycleBucket(bucket);

            if (next != null) {
                result = new IteratorRemoveResult<Delivery>(next, next.readOffset);
            }
        }

        return result;
    }

    // This method is called knowing that there is enough space either in front of, or behind the target
    // bucket to accommodate all its entries so there are no checks here to validate that assumption.
    // We do not clear or update the actual bucket state here but instead allow the natural cleanup
    // of the recycle method handle that for us.
    private final void doCompaction(UnsettledBucket<Delivery> bucket, UnsettledBucket<Delivery> prev, UnsettledBucket<Delivery> next) {
        final Object[] srcDeliveries = bucket.deliveries;
        final int[] srcDeliveryIds = bucket.deliveryIds;

        int srcEntries = bucket.entries;
        int srcReadOffset = bucket.readOffset;

        if (prev.getFreeSpace() > 0) {
            final int toCopy = Math.min(srcEntries, prev.getFreeSpace());
            final int prevTailSpace = prev.deliveries.length - prev.writeOffset;

            if (prevTailSpace < toCopy && prev.readOffset != 0) {
                // Not enough space at the end of the previous bucket arrays so we will compact to
                // zero if not already there and then copy what we can into that bucket.
                System.arraycopy(prev.deliveries, prev.readOffset, prev.deliveries, 0, prev.entries);
                System.arraycopy(prev.deliveryIds, prev.readOffset, prev.deliveryIds, 0, prev.entries);
                if (prev.writeOffset > prev.entries + toCopy) {
                    // Ensure no dangling entries after compaction
                    Arrays.fill(prev.deliveries, prev.entries + toCopy, prev.writeOffset, null);
                }

                prev.writeOffset -= prev.readOffset;
                prev.readOffset = 0;
            }

            System.arraycopy(srcDeliveries, srcReadOffset, prev.deliveries, prev.writeOffset, toCopy);
            System.arraycopy(srcDeliveryIds, srcReadOffset, prev.deliveryIds, prev.writeOffset, toCopy);

            prev.entries += toCopy;
            prev.writeOffset += toCopy;
            prev.highestDeliveryId = prev.deliveryIds[prev.writeOffset - 1];

            srcEntries -= toCopy;
            srcReadOffset += toCopy;
        }

        // We didn't get them all into the previous bucket but we know that if we are
        // here then there must be space ahead to accept the rest as we already checked.
        if (srcEntries > 0) {
            if (next.entries != 0) {
                if (next.readOffset < srcEntries) {
                    System.arraycopy(next.deliveries, next.readOffset, next.deliveries, srcEntries, next.entries);
                    System.arraycopy(next.deliveryIds, next.readOffset, next.deliveryIds, srcEntries, next.entries);

                    next.readOffset = 0;
                    next.writeOffset = srcEntries + next.entries;
                } else {
                    next.readOffset -= srcEntries;
                }
            } else {
                next.writeOffset = srcEntries;
            }

            System.arraycopy(srcDeliveries, srcReadOffset, next.deliveries, next.readOffset, srcEntries);
            System.arraycopy(srcDeliveryIds, srcReadOffset, next.deliveryIds, next.readOffset, srcEntries);

            next.entries += srcEntries;
            next.lowestDeliveryId = next.deliveryIds[next.readOffset];
            next.highestDeliveryId = next.deliveryIds[next.writeOffset - 1];
        }
    }

    private void tryCompact(UnsettledBucket<Delivery> bucket) {
        final UnsettledBucket<Delivery> next = bucket.next;
        final UnsettledBucket<Delivery> prev = bucket.prev;

        if (bucket.isReadable()) {
            final UnsettledBucket<Delivery> nextBucket =
                (next == null || bucket == head) ||
                Integer.compareUnsigned(bucket.highestDeliveryId, next.lowestDeliveryId) >= 0 ? ALWAYS_FULL_BUCKET : next;
            final UnsettledBucket<Delivery> prevBucket =
                (prev == null || bucket == tail) ||
                Integer.compareUnsigned(bucket.lowestDeliveryId, prev.highestDeliveryId) <= 0 ? ALWAYS_FULL_BUCKET : prev;

            // As soon as compaction is possible move elements from this bucket into previous and next
            // which reduces search times as there are fewer buckets to traverse/
            if (nextBucket.getFreeSpace() + prevBucket.getFreeSpace() >= bucket.entries) {
                doCompaction(bucket, prevBucket, nextBucket);
                recycleBucket(bucket);
            }
        } else {
            recycleBucket(bucket);
        }
    }

    //----- Internal bucket of delivery sequence

    @SuppressWarnings("unchecked")
    private static final class UnsettledBucket<Delivery> {

        private UnsettledBucket<Delivery> next;
        private UnsettledBucket<Delivery> prev;

        private int readOffset;
        private int writeOffset;
        private int entries;
        private int lowestDeliveryId = UnsignedInteger.MAX_VALUE.intValue();
        private int highestDeliveryId;
        private int generation = 0; // Tracks which ID overflow this belongs to

        private final Delivery[] deliveries;
        private final int[] deliveryIds;

        private UnsettledBucket() {
            this.deliveries = (Delivery[]) new Object[0];
            this.deliveryIds = new int[0];
            this.highestDeliveryId = UnsignedInteger.MAX_VALUE.intValue();
        }

        public UnsettledBucket(int bucketCapacity) {
            this.deliveries = (Delivery[]) new Object[bucketCapacity];
            this.deliveryIds = new int[bucketCapacity];
        }

        public boolean isReadable() {
            return entries > 0;
        }

        public int getFreeSpace() {
            return deliveries.length - entries;
        }

        /**
         * Checks if the delivery ID is likely in this bucket by comparing the lowest and
         * highest delivery IDs in this bucket to the given delivery ID value, if the ID is
         * between the highest and lowest value known to be in this bucket it is assumed that
         * it is likely in this bucket.
         *
         * @param deliveryId
         * 	The delivery ID to check for possible existence in this bucket.
         *
         * @return <code>true</code> if the delivery ID might be in this bucket.
         */
        public boolean isInRange(int deliveryId) {
            return Integer.compareUnsigned(deliveryId, highestDeliveryId) <= 0 &&
                   Integer.compareUnsigned(deliveryId, lowestDeliveryId) >= 0;
        }

        /**
         * Checks if the given range of delivery IDs potentially captures any entries in
         * this bucket by checking if the opposing ends of the ranges overlap.
         *
         * @param lowest
         * 		The lowest value that is being searched for.
         * @param highest
         * 		The highest value that is being searched for.
         *
         * @return <code>true</code> if the bucket contains some entries in the given range.
         */
        public boolean isCapturedByRange(int lowest, int highest) {
            return Integer.compareUnsigned(lowestDeliveryId, highest) <= 0 &&
                   Integer.compareUnsigned(highestDeliveryId, lowest) >= 0;
        }

        public void put(int deliveryId, Delivery delivery) {
            if (writeOffset == deliveryIds.length) {
                compact();
            }

            if (entries == 0) {
                lowestDeliveryId = deliveryId;
            }

            highestDeliveryId = deliveryId;
            deliveryIds[writeOffset] = deliveryId;
            deliveries[writeOffset++] = delivery;
            entries++;
        }

        /**
         * Remove the entry at the given bucket entry and return the index of the logical
         * next element in the bucket that takes its place. Depending on the direction the
         * bucket elements are shifted this could be the same value or it might be a value
         * one larger which can become the write offset in either case so the caller should
         * check that the value is less than write offset before using it.
         *
         * @param deliveryIndex
         * 		The index in the bucket that is being removed.
         *
         * @return the next value following the bucket index which could be the write offset
         */
        public int removeIndex(int deliveryIndex) {
            entries--;

            // If not the readOffset we compact the entries to avoid null gaps in the entries
            // which complicates searches and makes bulk assignments or copies impossible. If
            // at the read offset then we either advance the lowest Id seen or we've consumed
            // all the entries and we reset the value to ensure range checks fail
            if (deliveryIndex == readOffset) {
                deliveries[readOffset++] = null;
                deliveryIndex++;
                // We removed the first element meaning we now must increase the lowest entry to
                // avoid false positives when accessing randomly unless unordered since there
                // could be duplicates
                if (entries > 0) {
                    lowestDeliveryId = deliveryIds[readOffset];
                } else {
                    lowestDeliveryId = UnsignedInteger.MAX_VALUE.intValue();
                    highestDeliveryId = 0;
                    readOffset = 0;
                    writeOffset = 0;
                }
            } else if (deliveryIndex == writeOffset - 1) {
                deliveries[--writeOffset] = null;
                // If we remove the last entry then we can reduce the highest delivery ID in this
                // bucket to avoid false positive matches when randomly accessing elements unless
                // unordered in which case there could be duplicate entries
                highestDeliveryId = deliveryIds[writeOffset - 1];
            } else {
                final int prefixSize = deliveryIndex - readOffset;
                final int suffixSize = (writeOffset - 1) - deliveryIndex;

                if (prefixSize <= suffixSize) {
                    System.arraycopy(deliveries, readOffset, deliveries, readOffset + 1, prefixSize);
                    System.arraycopy(deliveryIds, readOffset, deliveryIds, readOffset + 1, prefixSize);
                    deliveries[readOffset++] = null;
                    deliveryIndex++;
                } else {
                    System.arraycopy(deliveries, deliveryIndex + 1, deliveries, deliveryIndex, suffixSize);
                    System.arraycopy(deliveryIds, deliveryIndex + 1, deliveryIds, deliveryIndex, suffixSize);
                    deliveries[--writeOffset] = null;
                }
            }

            return deliveryIndex;
        }

        public void clear() {
            if (entries != 0) {
                Arrays.fill(deliveries, null);
            }

            // Ensures the first put always assigns this
            lowestDeliveryId = UnsignedInteger.MAX_VALUE.intValue();
            highestDeliveryId = writeOffset = readOffset = entries = 0;
            next = prev = null;
        }

        @Override
        public String toString() {
            return "UnsettledBucket { size=" + entries +
                                    " roff=" + readOffset +
                                    " woff=" + writeOffset +
                                    " lowID=" + lowestDeliveryId +
                                    " highID=" + highestDeliveryId + " }";
        }

        private static final int BINARY_SEARCH_THRESHOLD = 16;

        /**
         * Search the bucket to find a delivery with the matching delivery ID and
         * return this index in the deliveries array where that delivery is located.
         *
         * @param deliveryId
         * 		The target delivery ID to find in the set of tracked deliveries.
         *
         * @return the index in the deliveries array where the found delivery lives, or the insertion point as
         *         a negative value e.g. -(index - 1).
         */
        public int search(int deliveryId) {
            if (deliveryIds[readOffset] == deliveryId) {
                return readOffset;
            } else if (entries < BINARY_SEARCH_THRESHOLD) {
                return linearSearch(deliveryId, readOffset, writeOffset);
            } else {
                return binarySearch(deliveryId, readOffset, writeOffset);
            }
         }

        // Must use our own until moving onto a JDK that adds the unsigned binary
        // search API since delivery IDs are unsigned integers
        private int binarySearch(int deliveryId, int fromIndex, int toIndex) {
            final int[] ids = deliveryIds;

            int low = fromIndex;
            int high = toIndex - 1;

            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final int midDeliveryId = ids[mid];
                final int cmp = Integer.compareUnsigned(midDeliveryId, deliveryId);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }

            return ~low; // signal that delivery ID is not in this bucket also gives insertion point
        }

        // Must use our own until moving onto a JDK that adds the unsigned binary
        // search API since delivery IDs are unsigned integers
        private int linearSearch(int deliveryId, int fromIndex, int toIndex) {
            final int[] ids = deliveryIds;

            for (int i = fromIndex; i < toIndex; ++i) {
                final int idAtIndex = ids[i];
                final int comp = Integer.compareUnsigned(idAtIndex, deliveryId);

                if (comp == 0) {
                    return i;
                } else if (comp > 0) {
                    // Can't be in this bucket because we already found a larger value
                    // so return the insertion point where it would go
                    return ~i;
                }
            }

            return ~toIndex;
        }

        private void compact() {
            if (readOffset != 0) {
                System.arraycopy(deliveries, readOffset, deliveries, 0, entries);
                System.arraycopy(deliveryIds, readOffset, deliveryIds, 0, entries);
                Arrays.fill(deliveries, entries, writeOffset, null);

                writeOffset = entries;
                readOffset = 0;
            } else {
                throw new IllegalStateException("Put called when no space in the bucket for new entries");
            }
        }
    }

    //----- Internal cached values for the various collection type access objects

    // Once requested we will create and store a single instance to a collection
    // with no state for each of the key, values and entries types. Since the types
    // do not have state the trivial race on create is not important to the eventual
    // outcome of having a cached instance.

    protected Set<UnsignedInteger> keySet;
    protected Collection<Delivery> values;
    protected Set<Entry<UnsignedInteger, Delivery>> entrySet;

    //----- Unsettled Tracking Map Collection types

    private final class UnsettledTackingMapValues extends AbstractCollection<Delivery> {

        @Override
        public Iterator<Delivery> iterator() {
            return new UnsettledTrackingMapValuesIterator(tail);
        }

        @Override
        public int size() {
            return UnsettledMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return UnsettledMap.this.containsValue(o);
        }

        @Override
        public boolean remove(Object target) {
            @SuppressWarnings("unchecked")
            final int targetId = UnsettledMap.this.deliveryIdSupplier.getDeliveryId((Delivery) target);

            return UnsettledMap.this.remove(targetId) != null;
        }

        @Override
        public void clear() {
            UnsettledMap.this.clear();
        }
    }

    private final class UnsettledTackingMapKeys extends AbstractSet<UnsignedInteger> {

        @Override
        public Iterator<UnsignedInteger> iterator() {
            return new UnsettledTrackingMapKeysIterator(tail);
        }

        @Override
        public int size() {
            return UnsettledMap.this.size;
        }

        @Override
        public boolean contains(Object o) {
            return UnsettledMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            if (target instanceof Number) {
                return UnsettledMap.this.remove(((Number) target).intValue()) != null;
            }
            return false;
        }

        @Override
        public void clear() {
            UnsettledMap.this.clear();
        }
    }

    private final class UnsettledTackingMapEntries extends AbstractSet<Map.Entry<UnsignedInteger, Delivery>> {

        @Override
        public Iterator<Map.Entry<UnsignedInteger, Delivery>> iterator() {
            return new UnsettledTrackingMapEntryIterator(tail);
        }

        @Override
        public int size() {
            return UnsettledMap.this.size;
        }

        @Override
        public boolean contains(Object target) {
            if (target instanceof Map.Entry) {
                @SuppressWarnings("unchecked")
                final Entry<? extends UnsignedInteger, ? extends Delivery> entry =
                    (Entry<? extends UnsignedInteger, ? extends Delivery>) target;
                return UnsettledMap.this.containsKey(entry.getKey());
            }

            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean remove(Object target) {
            if (target instanceof Map.Entry) {
                final Entry<? extends UnsignedInteger, ? extends Delivery> entry =
                    (Entry<? extends UnsignedInteger, ? extends Delivery>) target;
                return UnsettledMap.this.remove(entry.getKey()) != null;
            }

            return false;
        }

        @Override
        public void clear() {
            UnsettledMap.this.clear();
        }
    }

    //----- Map Iterator implementation for EntrySet, KeySet and Values collections

    private static final class IteratorRemoveResult<Delivery> {

        public final UnsettledBucket<Delivery> bucket;
        public final int readOffset;

        public IteratorRemoveResult(UnsettledBucket<Delivery> bucket, int readOffset) {
            this.bucket = bucket;
            this.readOffset = readOffset;
        }
    }

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class UnsettledTrackingMapIterator<T> implements Iterator<T> {

        protected UnsettledBucket<Delivery> currentBucket;
        protected int readOffset;

        protected T lastReturned;
        protected int lastReturnedBucketIndex;
        protected UnsettledBucket<Delivery> lastReturnedBucket;

        protected int expectedModCount;

        public UnsettledTrackingMapIterator(UnsettledBucket<Delivery> bucket) {
            this.currentBucket = bucket;
            this.readOffset = currentBucket == null ? -1 : currentBucket.readOffset;
            this.expectedModCount = UnsettledMap.this.modCount;
        }

        @Override
        public boolean hasNext() {
            return readOffset >= 0;
        }

        @Override
        public T next() {
            if (readOffset == -1) {
                throw new NoSuchElementException();
            }
            if (expectedModCount != UnsettledMap.this.modCount) {
                throw new ConcurrentModificationException();
            }

            lastReturnedBucket = currentBucket;
            lastReturnedBucketIndex = readOffset;
            lastReturned = entryAt(currentBucket, readOffset);
            successor();

            return lastReturned;
        }

        protected abstract T entryAt(UnsettledBucket<Delivery> bucket, int bucketEntry);

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException("Cannot remove entry when next has not been called");
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }

            final IteratorRemoveResult<Delivery> result = UnsettledMap.this.removeAt(lastReturnedBucket, lastReturnedBucketIndex);

            if (result != null) {
                currentBucket = result.bucket;
                readOffset = result.readOffset;
            } else {
                currentBucket = null;
                readOffset = -1;
            }

            expectedModCount = modCount;
            lastReturned = null;
        }

        private void successor() {
            if (++readOffset == currentBucket.writeOffset) {
                currentBucket = currentBucket.next;
                if (currentBucket != null && currentBucket.isReadable()) {
                    readOffset = currentBucket.readOffset;
                } else {
                    readOffset = -1;
                }
            }
        }
    }

    private final class UnsettledTrackingMapValuesIterator extends UnsettledTrackingMapIterator<Delivery> {

        public UnsettledTrackingMapValuesIterator(UnsettledBucket<Delivery> bucket) {
            super(bucket);
        }

        @Override
        protected Delivery entryAt(UnsettledBucket<Delivery> bucket, int bucketEntry) {
            return bucket.deliveries[bucketEntry];
        }
    }

    private final class UnsettledTrackingMapKeysIterator extends UnsettledTrackingMapIterator<UnsignedInteger> {

        public UnsettledTrackingMapKeysIterator(UnsettledBucket<Delivery> bucket) {
            super(bucket);
        }

        @Override
        protected UnsignedInteger entryAt(UnsettledBucket<Delivery> bucket, int bucketEntry) {
            return UnsignedInteger.valueOf(bucket.deliveryIds[bucketEntry]);
        }
    }

    private final class UnsettledTrackingMapEntryIterator extends UnsettledTrackingMapIterator<Entry<UnsignedInteger, Delivery>> {

        public UnsettledTrackingMapEntryIterator(UnsettledBucket<Delivery> bucket) {
            super(bucket);
        }

        @Override
        protected Entry<UnsignedInteger, Delivery> entryAt(UnsettledBucket<Delivery> bucket, int bucketEntry) {
            return new ImmutableUnsettledTrackingkMapEntry<Delivery>(
                    bucket.deliveryIds[bucketEntry], bucket.deliveries[bucketEntry]);
        }
    }

    /**
     * An immutable {@link Map} entry that can be used when exposing raw entry mappings
     * via the {@link Map} API.
     *
     * @param <Delivery> Type of the value portion of this immutable entry.
     */
    public static class ImmutableUnsettledTrackingkMapEntry<Delivery> implements Map.Entry<UnsignedInteger, Delivery> {

        private final int key;
        private final Delivery value;

        /**
         * Create a new immutable {@link Map} entry.
         *
         * @param key
         * 		The inner {@link Map} key that is wrapped.
         * @param value
         * 		The inner {@link Map} value that is wrapped.
         */
        public ImmutableUnsettledTrackingkMapEntry(int key, Delivery value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public UnsignedInteger getKey() {
            return UnsignedInteger.valueOf(key);
        }

        /**
         * @return the primitive integer view of the unsigned key.
         */
        public int getPrimitiveKey() {
            return key;
        }

        @Override
        public Delivery getValue() {
            return value;
        }

        @Override
        public Delivery setValue(Delivery value) {
            throw new UnsupportedOperationException();
        }
    }
}