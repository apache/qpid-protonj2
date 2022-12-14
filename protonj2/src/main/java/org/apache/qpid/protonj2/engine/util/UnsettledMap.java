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
 *
 * @param <Delivery> The delivery type being tracker (incoming or outgoing)
 */
public class UnsettledMap<Delivery> implements Map<UnsignedInteger, Delivery> {

    public interface UnsettledGetDeliveryId<Delivery> {
        int getDeliveryId(Delivery delivery);
    }

    private final double BUCKET_LOAD_FACTOR_MULTIPLIER = 0.30;

    private static final int UNSETTLED_INITIAL_BUCKETS = 2;
    private static final int UNSETTLED_BUCKET_SIZE = 256;

    // Always full buckets used in operations that needs unwritable bounds
    private final UnsettledBucket<Delivery> ALWAYS_FULL_BUCKET = new UnsettledBucket<>();

    private final UnsettledGetDeliveryId<Delivery> deliveryIdSupplier;
    private final int bucketCapacity;
    private final int bucketLowWaterMark;

    private int totalEntries;
    private int modCount;

    private int current;
    private UnsettledBucket<Delivery>[] buckets;

    public UnsettledMap(UnsettledGetDeliveryId<Delivery> idSupplier) {
        this(idSupplier, UNSETTLED_INITIAL_BUCKETS, UNSETTLED_BUCKET_SIZE);
    }

    public UnsettledMap(UnsettledGetDeliveryId<Delivery> idSupplier, int initialBuckets) {
        this(idSupplier, initialBuckets, UNSETTLED_BUCKET_SIZE);
    }

    @SuppressWarnings("unchecked")
    public UnsettledMap(UnsettledGetDeliveryId<Delivery> idSupplier, int initialBuckets, int bucketSize) {
        this.deliveryIdSupplier = idSupplier;
        this.bucketCapacity = bucketSize;
        this.bucketLowWaterMark = (int) (bucketSize * BUCKET_LOAD_FACTOR_MULTIPLIER);

        if (bucketSize < 1) {
            throw new IllegalArgumentException("The bucket size must be greater than zero");
        }

        if (initialBuckets < 1) {
            throw new IllegalArgumentException("The initial number of buckets must be at least 1");
        }

        buckets = new UnsettledBucket[initialBuckets];
        for (int i = 0; i < buckets.length; ++i) {
            buckets[i] = new UnsettledBucket<>(bucketSize, deliveryIdSupplier);
        }
    }

    @Override
    public void putAll(Map<? extends UnsignedInteger, ? extends Delivery> source) {
        for (Entry<? extends UnsignedInteger, ? extends Delivery> entry : source.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        for (int i = 0; i < buckets.length && buckets[i].isReadable(); ++i) {
            buckets[i].clear(); // Ensure any referenced deliveries are cleared
        }

        current = totalEntries = 0;
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
     * @return null in all cases as this tracker does not check for duplicates.
     */
    public Delivery put(int deliveryId, Delivery delivery) {
        if (!buckets[current].put(deliveryId, delivery)) {
            // Always move to next bucket or create one so that current is always
            // position on a writable bucket.
            if (++current == buckets.length) {
                // Create a new bucket of entries since we don't have any more space free yet
                // and update the chain with the newly create bucket. We disregard the max segments
                // here since we always need the extra space regardless of how many pending unsettled
                // deliveries there are.
                buckets = Arrays.copyOf(buckets, current + 1);
                buckets[current] = new UnsettledBucket<>(bucketCapacity, deliveryIdSupplier);
            }

            // Moved on after overflow so we know this one will work.
            buckets[current].put(deliveryId, delivery);
        }

        totalEntries++;
        modCount++;

        return null;
    }

    @Override
    public int size() {
        return totalEntries;
    }

    @Override
    public boolean isEmpty() {
        return totalEntries == 0;
    }

    @Override
    public Delivery get(Object key) {
        return get(Number.class.cast(key).intValue());
    }

    public Delivery get(int deliveryId) {
        if (totalEntries == 0) {
            return null;
        }

        // Search every bucket because delivery IDs can wrap around, but we can
        // stop at the first empty bucket as all buckets following it must also
        // be empty buckets.
        for (int i = 0; i <= current; ++i) {
            if (buckets[i].isInRange(deliveryId)) {
                final Delivery result = buckets[i].get(deliveryId);
                if (result != null) {
                    return result;
                }
            }
        }

        return null;
    }

    @Override
    public Delivery remove(Object key) {
        return removeValue(Number.class.cast(key).intValue());
    }

    public Delivery remove(int deliveryId) {
        return removeValue(deliveryId);
    }

    @Override
    public boolean containsKey(Object key) {
        return containsKey(Number.class.cast(key).intValue());
    }

    public boolean containsKey(int key) {
        if (totalEntries > 0) {
            for (int i = 0; i <= current; ++i) {
                if (buckets[i].isInRange(key)) {
                    if (buckets[i].get(key) != null) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        if (totalEntries > 0 && value != null) {
            for (int i = 0; i <= current; ++i) {
                for (int j = buckets[i].readOffset; j < buckets[i].writeOffset; ++j) {
                    if (value.equals(buckets[i].entryAt(j))) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public void forEach(Consumer<Delivery> action) {
        Objects.requireNonNull(action);

        if (totalEntries == 0) {
            return;
        }

        for (int i = 0; i <= current; ++i) {
            for (int j = buckets[i].readOffset; j < buckets[i].writeOffset; ++j) {
                action.accept(buckets[i].entryAt(j));
            }
        }
    }

    /**
     * Visits each entry within the given range and invokes the provided action
     * on each delivery in the tracker.
     *
     * @param first
     * 		The first entry to visit.
     * @param last
     * 		The last entry to visit.
     * @param action
     * 		The action to invoke on each visited entry.
     */
    public void forEach(int first, int last, Consumer<Delivery> action) {
        Objects.requireNonNull(action);

        if (totalEntries == 0) {
            return;
        }

        boolean foundFirst = false;
        boolean foundLast = false;

        for (int i = 0; i <= current && !foundLast; ++i) {
            if (!foundFirst && !buckets[i].isInRange(first)) {
                continue;
            }

            for (int j = buckets[i].readOffset; j < buckets[i].writeOffset && !foundLast; ++j) {
                final Delivery delivery = buckets[i].entryAt(j);
                final int deliveryId = deliveryIdSupplier.getDeliveryId(delivery);

                foundFirst = foundFirst || deliveryId == first;
                foundLast = deliveryId == last;

                if (foundFirst) {
                    action.accept(delivery);
                }
            }
        }
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

        if (totalEntries == 0) {
            return;
        }

        boolean foundFirst = false;
        boolean foundLast = false;
        int removeStart = 0;
        int removeEnd = 0;

        for (int i = 0; i <= current && !foundLast; ++i) {
            if (!foundFirst && !buckets[i].isInRange(first)) {
                continue;
            }

            final UnsettledBucket<Delivery> bucket = buckets[i];

            for (int j = removeStart = removeEnd = bucket.readOffset; j < bucket.writeOffset && !foundLast; ) {
                final Delivery delivery = bucket.entryAt(j);
                final int deliveryId = deliveryIdSupplier.getDeliveryId(delivery);

                foundFirst = foundFirst || deliveryId == first;
                foundLast = deliveryId == last;

                if (foundFirst) {
                    action.accept(delivery);
                    removeEnd = ++j;
                } else {
                    removeStart = removeEnd = ++j;
                }
            }

            // We found first so this iteration did clear some elements from the current bucket
            // and we need to check that this removal cleared a full bucket which means the index
            // needs to shift back one since we will have recycled the empty bucket. When the last
            // index is found we should also attempt to compact the bucket with the previous one
            // to reduce fragmentation.
            if (foundFirst) {
                i = removeRange(i, removeStart, removeEnd, foundLast) ? --i : i;
            }
        }
    }

    private boolean removeRange(int bucketIndex, int start, int end, boolean compact) {
        final UnsettledBucket<Delivery> bucket = buckets[bucketIndex];
        final int removals = end - start;

        this.totalEntries -= removals;
        this.modCount++;

        if (removals == bucket.entries) {
            return recycleBucket(bucketIndex);
        } else {
            System.arraycopy(bucket.deliveries, end, bucket.deliveries, start, bucket.writeOffset - end);
            Arrays.fill(bucket.deliveries, bucket.writeOffset - removals, bucket.writeOffset, null);

            bucket.writeOffset = bucket.writeOffset - removals;
            bucket.entries -= removals;
            bucket.highestDeliveryId = bucket.entryIdAt(bucket.writeOffset - 1);

            if (compact) {
                return tryCompact(bucketIndex);
            }
        }

        return false;
    }

    @Override
    public void forEach(BiConsumer<? super UnsignedInteger, ? super Delivery> action) {
        Objects.requireNonNull(action);

        if (totalEntries == 0) {
            return;
        }

        for (int i = 0; i <= current; ++i) {
            for (int j = buckets[i].readOffset; j < buckets[i].writeOffset; ++j) {
                final Delivery delivery = buckets[i].entryAt(j);
                action.accept(UnsignedInteger.valueOf(deliveryIdSupplier.getDeliveryId(delivery)), delivery);
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
            for (int i = 0; i <= current; ++i) {
                for (int j = buckets[i].readOffset; j < buckets[i].writeOffset; ++j) {
                    final Delivery delivery = buckets[i].entryAt(j);
                    if (!delivery.equals(m.get(deliveryIdSupplier.getDeliveryId(delivery)))) {
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
        return "UnsettledMap: { size=" + totalEntries +
                              " buckets=" + buckets.length +
                              " bucket-capacity=" + bucketCapacity + " }";
    }

    //----- Internal UnsettledMap API

    private boolean recycleBucket(int index) {
        buckets[index].clear();  // Drop all content and reset as empty bucket

        // If the current bucket is cleared then we can just start writing in it again
        if (index < current) {
            current--;
            final int next = index + 1;
            UnsettledBucket<Delivery> recycled = buckets[index];
            System.arraycopy(buckets, next, buckets, index, buckets.length - next);
            buckets[buckets.length - 1] = recycled;
            return true;
        }

        return false;
    }

    private Delivery removeValue(int deliveryId) {
        if (totalEntries > 0) {
            Delivery result = null;

            for (int i = 0; i <= current; ++i) {
                if (buckets[i].isInRange(deliveryId) && (result = buckets[i].remove(deliveryId)) != null) {
                    totalEntries--;
                    modCount++;

                    if (buckets[i].entries <= bucketLowWaterMark) {
                        tryCompact(i);
                    }

                    return result;
                }
            }
        }

        return null;
    }

    // Called from iteration APIs which requires the method to return the location of the next
    // entry once removal and possible bucket compaction is completed.
    private long removeAt(int bucketIndex, int bucketEntry) {
        if (bucketIndex >= buckets.length || bucketEntry >= UNSETTLED_BUCKET_SIZE) {
            throw new IndexOutOfBoundsException(String.format(
                "Cannot remove an entry from segment %d at index %d which is outside the tracked bounds", bucketIndex, bucketEntry));
        }

        final UnsettledBucket<Delivery> bucket = buckets[bucketIndex];

        bucket.removeAt(bucketEntry);
        totalEntries--;
        modCount++;

        long result = -1;

        if (bucket.isReadable()) {
            final int nextBucketIndex = bucketIndex + 1;
            final int prevBucketIndex = bucketIndex - 1;

            final UnsettledBucket<Delivery> nextBucket =
                bucketIndex == current || Integer.compareUnsigned(bucket.highestDeliveryId, buckets[nextBucketIndex].lowestDeliveryId) > 0 ?
                    ALWAYS_FULL_BUCKET : buckets[nextBucketIndex];
            final UnsettledBucket<Delivery> prevBucket =
                bucketIndex == 0 || Integer.compareUnsigned(bucket.lowestDeliveryId, buckets[prevBucketIndex].highestDeliveryId) < 0 ?
                    ALWAYS_FULL_BUCKET : buckets[prevBucketIndex];

            // As soon as compaction is possible move elements from this bucket into previous and next
            // which reduces search times as there are fewer buckets to traverse/
            if (nextBucket.getFreeSpace() + prevBucket.getFreeSpace() >= bucket.entries) {
                final int toCopyBackward = Math.min(prevBucket.getFreeSpace(), bucket.entries);
                final int nextEntryOffset = ++bucketEntry - (bucket.readOffset + toCopyBackward);

                if (nextEntryOffset < 0) {
                    // Moved into the previous bucket so the index being negative
                    // give us the located when added to the previous write offset
                    result = (long) (prevBucketIndex) << 32;
                    result = prevBucket.writeOffset + nextEntryOffset;
                } else if (nextBucket.entries + (bucket.entries - toCopyBackward) > 0) {
                    // Moved into the next bucket gives of the raw index so long
                    // as we compact entries to zero (otherwise it is the read offset
                    result = (long) bucketIndex << 32;
                    result |= nextEntryOffset;
                }

                doCompaction(bucket, prevBucket, nextBucket);
                recycleBucket(bucketIndex);
            } else {
                // If the element removed is not the last in this bucket then the next is just
                // the next element otherwise it is the first element of the next bucket if it
                // non-empty otherwise we reached the end of the elements.
                if (++bucketEntry < bucket.writeOffset) {
                    result = (long) bucketIndex << 32;
                    result |= bucketEntry;
                } else if (nextBucketIndex <= current && buckets[nextBucketIndex].entries > 0) {
                    result = (long) nextBucketIndex << 32;
                    result |= buckets[nextBucketIndex].readOffset;
                }
            }
        } else {
            recycleBucket(bucketIndex);

            // The bucket was empty and will be recycled shifting down all buckets following it and if
            // there is a next non-empty bucket then the next entry is the first entry in that bucket.
            if (bucketIndex <= current && buckets[bucketIndex].entries > 0) {
                result = (long) bucketIndex << 32;
                result |= buckets[bucketIndex].readOffset;
            }
        }

        return result;
    }

    private final void doCompaction(UnsettledBucket<Delivery> bucket, UnsettledBucket<Delivery> prev, UnsettledBucket<Delivery> next) {
        if (prev.getFreeSpace() > 0) {
            final int toCopy = Math.min(bucket.entries, prev.getFreeSpace());

            // We always compact to zero which makes some of the other updates simpler and
            // can allow for easier compaction in the future.
            if (prev.readOffset != 0) {
                System.arraycopy(prev.deliveries, prev.readOffset, prev.deliveries, 0, prev.entries);
                if (prev.writeOffset > prev.entries + toCopy) {
                    // Ensure no dangling entries after compaction
                    Arrays.fill(prev.deliveries, prev.entries, prev.writeOffset, null);
                }

                prev.writeOffset -= prev.readOffset;
                prev.readOffset = 0;
            }

            System.arraycopy(bucket.deliveries, bucket.readOffset, prev.deliveries, prev.writeOffset, toCopy);

            prev.entries += toCopy;
            prev.writeOffset = prev.entries;
            prev.highestDeliveryId = prev.entryIdAt(prev.writeOffset - 1);

            bucket.entries -= toCopy;
            bucket.writeOffset -= toCopy;
            bucket.readOffset += toCopy;
        }

        // We didn't get them all into the previous bucket but we know that if we are
        // here then there must be space ahead to accept the rest as we already checked.
        if (bucket.entries > 0) {
            if (next.readOffset != bucket.entries) {
                System.arraycopy(next.deliveries, next.readOffset, next.deliveries, bucket.entries, next.entries);
                if (next.readOffset < bucket.entries) {
                    // Ensure no dangling entries after compaction
                    Arrays.fill(next.deliveries, bucket.entries + next.entries, next.deliveries.length, null);
                }
            }

            System.arraycopy(bucket.deliveries, bucket.readOffset, next.deliveries, 0, bucket.entries);

            next.readOffset = 0;
            next.entries += bucket.entries;
            next.writeOffset = next.entries;
            next.lowestDeliveryId = next.entryIdAt(0);
            // We set this since the next bucket could be empty in some cases so it would
            // need to be initialized.
            next.highestDeliveryId = next.entryIdAt(next.writeOffset - 1);
        }
    }

    // This variant is called from the Map API remove methods and doesn't need to track bucket
    // compaction results or next element locations which increases performance in this case.
    private boolean tryCompact(int bucketIndex) {
        final UnsettledBucket<Delivery> bucket = buckets[bucketIndex];

        final int nextBucketIndex = bucketIndex + 1;
        final int prevBucketIndex = bucketIndex - 1;

        if (bucket.isReadable()) {
            final UnsettledBucket<Delivery> nextBucket =
                bucketIndex == current || Integer.compareUnsigned(bucket.highestDeliveryId, buckets[nextBucketIndex].lowestDeliveryId) > 0 ?
                    ALWAYS_FULL_BUCKET : buckets[nextBucketIndex];
            final UnsettledBucket<Delivery> prevBucket =
                bucketIndex == 0 || Integer.compareUnsigned(bucket.lowestDeliveryId, buckets[prevBucketIndex].highestDeliveryId) < 0 ?
                    ALWAYS_FULL_BUCKET : buckets[prevBucketIndex];

            // As soon as compaction is possible move elements from this bucket into previous and next
            // which reduces search times as there are fewer buckets to traverse/
            if (nextBucket.getFreeSpace() + prevBucket.getFreeSpace() >= bucket.entries) {
                doCompaction(bucket, prevBucket, nextBucket);
                return recycleBucket(bucketIndex);
            }
        } else {
            return recycleBucket(bucketIndex);
        }

        return false;
    }

    //----- Internal bucket of delivery sequence

    private static class UnsettledBucket<Delivery> {

        private int readOffset;
        private int writeOffset;
        private int entries;
        private int lowestDeliveryId = 0;
        private int highestDeliveryId = 0;

        private final Object[] deliveries;
        private final UnsettledGetDeliveryId<Delivery> deliveryIdSupplier;

        private UnsettledBucket() {
            this.deliveryIdSupplier = null;
            this.deliveries = new Object[0];
            this.highestDeliveryId = UnsignedInteger.MAX_VALUE.intValue();
        }

        public UnsettledBucket(int bucketCapacity, UnsettledGetDeliveryId<Delivery> idSupplier) {
            this.deliveryIdSupplier = idSupplier;
            this.deliveries = new Object[bucketCapacity];
        }

        public boolean isReadable() {
            return entries > 0;
        }

        public int getFreeSpace() {
            return deliveries.length - entries;
        }

        public boolean isFull() {
            return writeOffset == deliveries.length;
        }

        public boolean isInRange(int deliveryId) {
            return Integer.compareUnsigned(deliveryId, lowestDeliveryId) >= 0 &&
                   Integer.compareUnsigned(deliveryId, highestDeliveryId) <= 0;
        }

        public boolean put(int deliveryId, Delivery delivery) {
            // Reject an addition if full or if the delivery ID to be added is less that
            // the highest id we have tracked as that likely indicates a roll-over of the
            // IDs and must go into the next bucket so that this bucket is kept in order.
            if (isFull() || (Integer.compareUnsigned(deliveryId, highestDeliveryId) <= 0 && entries > 0)) {
                return false;
            }

            if (entries == 0) {
                lowestDeliveryId = deliveryId;
            }

            highestDeliveryId = deliveryId;
            deliveries[writeOffset++] = delivery;
            entries++;

            return true;
        }

        @SuppressWarnings("unchecked")
        public Delivery get(int deliveryId) {
            Delivery delivery;

            // Be optimistic and assume the result is in the first entry and then if not search
            // beyond that entry for the result.
            if (deliveryIdSupplier.getDeliveryId(delivery = (Delivery) deliveries[readOffset]) == deliveryId) {
                return delivery;
            } else {
                final int location = search(deliveryId, readOffset + 1, writeOffset);
                if (location >= 0) {
                    return (Delivery) deliveries[location];
                }
            }

            return null;
        }

        @SuppressWarnings("unchecked")
        public Delivery entryAt(int index) {
            return (Delivery) deliveries[index];
        }

        @SuppressWarnings("unchecked")
        public int entryIdAt(int index) {
            return deliveryIdSupplier.getDeliveryId((Delivery) deliveries[index]);
        }

        @SuppressWarnings("unchecked")
        public Delivery remove(int deliveryId) {
            // Be optimistic and assume the result is in the first entry and then if not search
            // beyond that entry for the result.
            if (deliveryIdSupplier.getDeliveryId((Delivery) deliveries[readOffset]) == deliveryId) {
                return removeAt(readOffset);
            } else {
                final int location = search(deliveryId, readOffset + 1, writeOffset);
                if (location >= 0) {
                    return removeAt(location);
                }
            }

            return null;
        }

        @SuppressWarnings("unchecked")
        public Delivery removeAt(int bucketEntry) {
            final Delivery delivery = (Delivery) deliveries[bucketEntry];

            entries--;

            // If not the readOffset we compact the entries to avoid null gaps in the entries
            // which complicates searches and makes bulk assignments or copies impossible.
            if (bucketEntry != readOffset) {
                System.arraycopy(deliveries, readOffset, deliveries, readOffset + 1, bucketEntry - readOffset);
                deliveries[readOffset++] = null;
                // If we remove the last entry then we can reduce the highest delivery ID in this
                // bucket to avoid false positive matches when randomly accessing elements unless
                // unordered in which case there could be duplicate entries
                if (bucketEntry == writeOffset) {
                    highestDeliveryId = deliveryIdSupplier.getDeliveryId((Delivery) deliveries[writeOffset - 1]);
                }
            } else {
                deliveries[readOffset++] = null;
                // We removed the first element meaning we now must increase the lowest entry to
                // avoid false positives when accessing randomly unless unordered since there could
                // be duplicates
                if (entries > 0) {
                    lowestDeliveryId = deliveryIdSupplier.getDeliveryId((Delivery) deliveries[readOffset]);
                }
            }

            return delivery;
        }

        public void clear() {
            if (entries != 0) {
                Arrays.fill(deliveries, null);
                entries = 0;
            }

            // Ensures the first put always assigns this
            lowestDeliveryId = UnsignedInteger.MAX_VALUE.intValue();
            highestDeliveryId = 0;
            writeOffset = 0;
            readOffset = 0;
        }

        @Override
        public String toString() {
            return "UnsettledBucket { size=" + entries +
                                    " roff=" + readOffset +
                                    " woff=" + writeOffset +
                                    " lowID=" + lowestDeliveryId +
                                    " highID=" + highestDeliveryId + " }";
        }

        private static int BINARY_SEARCH_THRESHOLD = 64;

        // Given the behavior of binary search it doesn't make sense to employ
        // it on spans under a certain number if elements
        private int search(int deliveryId, int fromIndex, int toIndex) {
            if ((toIndex - fromIndex) < BINARY_SEARCH_THRESHOLD) {
                return linearSearch(deliveryId, fromIndex, toIndex);
            } else {
                return binarySearch(deliveryId, fromIndex, toIndex);
            }
         }

        // Must use our own to avoid boxing for unsigned integer comparison.
        // fromIndex is inclusive since we search from readOffset normally
        // toIndex is exclusive since we search to writeOffset normally.
        @SuppressWarnings("unchecked")
        private int binarySearch(int deliveryId, int fromIndex, int toIndex) {
            int low = fromIndex;
            int high = toIndex - 1;

            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final int midDeliveryId = deliveryIdSupplier.getDeliveryId((Delivery) deliveries[mid]);
                final int cmp = UnsignedInteger.compare(midDeliveryId, deliveryId);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid;  // first matching delivery
                }
            }

            return -1; // signal that delivery ID is not in this bucket
        }

        // Must use our own to avoid boxing for unsigned integer comparison.
        // fromIndex is inclusive since we search from readOffset normally
        // toIndex is exclusive since we search to writeOffset normally.
        @SuppressWarnings("unchecked")
        private int linearSearch(int deliveryId, int fromIndex, int toIndex) {
            for (int i = fromIndex; i < toIndex; ++i) {
                final int idAtIndex = deliveryIdSupplier.getDeliveryId((Delivery) deliveries[i]);
                final int comp = UnsignedInteger.compare(idAtIndex, deliveryId);

                if (comp == 0) {
                    return i;
                } else if (comp > 0) {
                    // Can't be in this bucket because we already found a larger value
                    break;
                }
            }

            return -1;
        }
    }

    //----- Internal cached values for the various collection type access objects

    // Once requested we will create an store a single instance to a collection
    // with no state for each of the key, values ,entries types. Since the types do
    // not have state the trivial race on create is not important to the eventual
    // outcome of having a cached instance.

    protected Set<UnsignedInteger> keySet;
    protected Collection<Delivery> values;
    protected Set<Entry<UnsignedInteger, Delivery>> entrySet;

    //----- Unsettled Tracking Map Collection types

    private final class UnsettledTackingMapValues extends AbstractCollection<Delivery> {

        @Override
        public Iterator<Delivery> iterator() {
            return new UnsettledTrackingMapValuesIterator(0);
        }

        @Override
        public int size() {
            return UnsettledMap.this.totalEntries;
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
            return new UnsettledTrackingMapKeysIterator(0);
        }

        @Override
        public int size() {
            return UnsettledMap.this.totalEntries;
        }

        @Override
        public boolean contains(Object o) {
            return UnsettledMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object target) {
            return UnsettledMap.this.remove(Number.class.cast(target).intValue()) != null;
        }

        @Override
        public void clear() {
            UnsettledMap.this.clear();
        }
    }

    private final class UnsettledTackingMapEntries extends AbstractSet<Map.Entry<UnsignedInteger, Delivery>> {

        @Override
        public Iterator<Map.Entry<UnsignedInteger, Delivery>> iterator() {
            return new UnsettledTrackingMapEntryIterator(0);
        }

        @Override
        public int size() {
            return UnsettledMap.this.totalEntries;
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

    // Base class iterator that can be used for the collections returned from the Map
    private abstract class UnsettledTrackingMapIterator<T> implements Iterator<T> {

        protected int currentBucket;
        protected int readOffset;

        protected T lastReturned;
        protected int lastReturnedbucket;
        protected int lastReturnedbucketIndex;

        protected int expectedModCount;

        public UnsettledTrackingMapIterator(int startAt) {
            this.currentBucket = buckets[startAt].isReadable() ? startAt : -1;
            this.readOffset = buckets[startAt].isReadable() ? buckets[startAt].readOffset : -1;
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

            lastReturnedbucket = currentBucket;
            lastReturnedbucketIndex = readOffset;
            lastReturned = entryAt(currentBucket, readOffset);
            successor();

            return lastReturned;
        }

        protected abstract T entryAt(int bucketIndex, int bucketEntry);

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException("Cannot remove entry when next has not been called");
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }

            long next = UnsettledMap.this.removeAt(lastReturnedbucket, lastReturnedbucketIndex);
            if (next >= 0) {
                currentBucket = (int) (next >> 32);
                readOffset = (int) (next & 0xFFFFFFFF);
            } else {
                readOffset = -1;
            }

            expectedModCount = modCount;
            lastReturned = null;
        }

        private void successor() {
            if (++readOffset == buckets[currentBucket].writeOffset) {
                currentBucket++;
                if (currentBucket < buckets.length && buckets[currentBucket].isReadable()) {
                    readOffset = buckets[currentBucket].readOffset;
                } else {
                    readOffset = -1;
                }
            }
        }
    }

    private final class UnsettledTrackingMapValuesIterator extends UnsettledTrackingMapIterator<Delivery> {

        public UnsettledTrackingMapValuesIterator(int startAt) {
            super(startAt);
        }

        @Override
        protected Delivery entryAt(int bucketIndex, int bucketEntry) {
            return buckets[currentBucket].entryAt(bucketEntry);
        }
    }

    private final class UnsettledTrackingMapKeysIterator extends UnsettledTrackingMapIterator<UnsignedInteger> {

        public UnsettledTrackingMapKeysIterator(int startAt) {
            super(startAt);
        }

        @Override
        protected UnsignedInteger entryAt(int bucketIndex, int bucketEntry) {
            return UnsignedInteger.valueOf(deliveryIdSupplier.getDeliveryId(buckets[currentBucket].entryAt(bucketEntry)));
        }
    }

    private final class UnsettledTrackingMapEntryIterator extends UnsettledTrackingMapIterator<Entry<UnsignedInteger, Delivery>> {

        public UnsettledTrackingMapEntryIterator(int startAt) {
            super(startAt);
        }

        @Override
        protected Entry<UnsignedInteger, Delivery> entryAt(int bucketIndex, int bucketEntry) {
            final Delivery delivery = buckets[currentBucket].entryAt(bucketEntry);

            return new ImmutableUnsettledTrackingkMapEntry<Delivery>(deliveryIdSupplier.getDeliveryId(delivery), delivery);
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
