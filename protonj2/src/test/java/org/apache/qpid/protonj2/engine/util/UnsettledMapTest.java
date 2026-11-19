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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the unsettled delivery tacker
 */
public class UnsettledMapTest {

    protected static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(UnsettledMapTest.class);

    protected long seed;
    protected Random random;
    protected UnsignedInteger uintArray[] = new UnsignedInteger[1000];
    protected DeliveryType objArray[] = new DeliveryType[1000];
    protected UnsettledMap<DeliveryType> tracker;

    @BeforeEach
    public void setUp() {
        seed = System.nanoTime();
        random = new Random();
        random.setSeed(seed);

        tracker = new UnsettledMap<>(DeliveryType::getDeliveryId);

        for (int i = 1; i <= objArray.length; i++) {
            UnsignedInteger x = uintArray[i - 1] = UnsignedInteger.valueOf(i);
            DeliveryType y = objArray[i - 1] = new DeliveryType(UnsignedInteger.valueOf(i).intValue());
            tracker.put(x, y);
        }
    }

    protected UnsettledMap<DeliveryType> createMap() {
        return new UnsettledMap<>(DeliveryType::getDeliveryId);
    }

    protected UnsettledMap<DeliveryType> createMap(int numBuckets, int bucketSize) {
        return new UnsettledMap<>(DeliveryType::getDeliveryId, numBuckets, bucketSize);
    }

    /**
     * Asserts that each delivery the tracker still counts as unsettled can be found by its ID. A
     * delivery that survives in the tracker but cannot be located by a random access search could
     * never be settled by a disposition again.
     * <p>
     * The check visits every tracked delivery and searches for it by ID, which costs more than the
     * operations it guards, so the random tests only run it at intervals and once at the end.
     */
    private void assertAllTrackedDeliveriesAreReachable(UnsettledMap<DeliveryType> tracker) {
        int visited = 0;

        for (DeliveryType delivery : tracker.values()) {
            final int deliveryId = delivery.getDeliveryId();
            assertNotNull(tracker.get(deliveryId),
                "Delivery " + UnsignedInteger.valueOf(deliveryId) + " should be reachable by its ID");
            visited++;
        }

        assertEquals(tracker.size(), visited, "The values view should visit each tracked delivery once");
    }

    /**
     * Simple delivery type used for this test
     */
    private static class DeliveryType {

        private final int deliveryId;

        public DeliveryType(int deliveryid) {
            this.deliveryId = deliveryid;
        }

        public int getDeliveryId() {
            return deliveryId;
        }

        @Override
        public int hashCode() {
            return deliveryId;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof DeliveryType) {
                DeliveryType otherType = (DeliveryType) other;
                return otherType.deliveryId == deliveryId;
            }

            return false;
        }

        @Override
        public String toString() {
            return "DeliveryType: { " + deliveryId + " }";
        }
    }

    @Test
    public void testCreateFailsWhenInitialBucketsNegativeSingleArgument() {
        assertThrows(IllegalArgumentException.class, () -> new UnsettledMap<>(DeliveryType::getDeliveryId, -1));
    }

    @Test
    public void testCreateFailsWhenInitialBucketsNegative() {
        assertThrows(IllegalArgumentException.class, () -> new UnsettledMap<>(DeliveryType::getDeliveryId, -1, 10));
    }

    @Test
    public void testCreateFailsWhenBucketsSizeNegative() {
        assertThrows(IllegalArgumentException.class, () -> new UnsettledMap<>(DeliveryType::getDeliveryId, 10, -1));
    }

    @Test
    public void testEqualsReturnsString() {
        assertNotNull(tracker.toString());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testMapRejectedOversizedLongObjects() {
        UnsettledMap<DeliveryType> tracker = createMap();

        assertNull(tracker.get(Long.valueOf(Long.MAX_VALUE)));
        assertNull(tracker.remove(Long.valueOf(Long.MAX_VALUE)));
        assertFalse(tracker.containsKey(Long.valueOf(Long.MAX_VALUE)));
    }

    @Test
    public void testCreateUnsettledTracker() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());
        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testContainsKeyOnEmptyMap() {
        UnsettledMap<DeliveryType> tracker = createMap();

        assertFalse(tracker.containsKey(0));
        assertFalse(tracker.containsKey(UnsignedInteger.ZERO));
    }

    @Test
    public void testGetWhenEmpty() {
        UnsettledMap<DeliveryType> tracker = createMap();

        assertNull(tracker.get(0));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testGetWithGenericObject() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));

        assertNull(tracker.get("foo"));
    }

    @Test
    public void testValuesRemoveNullReturnsFalsePerCollectionsContract() {
        final UnsettledMap<DeliveryType> map = createMap();
        map.put(UnsignedInteger.valueOf(1), new DeliveryType(1));

        assertFalse(map.values().remove(null));
    }

    @Test
    public void testGet() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(65535, new DeliveryType(65535));
        tracker.put(-1, new DeliveryType(-1));

        assertEquals(new DeliveryType(0), tracker.get(0));
        assertEquals(new DeliveryType(1), tracker.get(1));
        assertEquals(new DeliveryType(2), tracker.get(2));
        assertEquals(new DeliveryType(65535), tracker.get(65535));
        assertEquals(new DeliveryType(-1), tracker.get(-1));

        assertNull(tracker.get(3));

        assertEquals(5, tracker.size());
    }

    @Test
    public void testGetUnsignedInteger() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(-3, new DeliveryType(-3));

        assertEquals(new DeliveryType(0), tracker.get(UnsignedInteger.valueOf(0)));
        assertEquals(new DeliveryType(1), tracker.get(UnsignedInteger.valueOf(1)));
        assertEquals(new DeliveryType(-3), tracker.get(UnsignedInteger.valueOf(-3)));

        assertNull(tracker.get(3));

        assertEquals(3, tracker.size());
    }

    @Test
    public void testContainsKey() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(-3, new DeliveryType(-3));

        assertTrue(tracker.containsKey(0));
        assertFalse(tracker.containsKey(3));

        assertEquals(3, tracker.size());
    }

    @Test
    public void testContainsKeyUnsignedInteger() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(UnsignedInteger.MAX_VALUE.intValue(), new DeliveryType(UnsignedInteger.MAX_VALUE.intValue()));
        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));

        assertTrue(tracker.containsKey(0));
        assertFalse(tracker.containsKey(3));

        assertEquals(3, tracker.size());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testContainsValue() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(-3, new DeliveryType(-3));

        assertTrue(tracker.containsValue(new DeliveryType(0)));
        assertFalse(tracker.containsValue(new DeliveryType(4)));

        assertEquals(3, tracker.size());

        assertFalse(tracker.containsValue("foo"));
    }

    @Test
    public void testContainsValueOnEmptyMap() {
        UnsettledMap<DeliveryType> tracker = createMap();

        assertFalse(tracker.containsValue(new DeliveryType(0)));
    }

    @Test
    public void testRemoveIsIdempotent() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));

        assertEquals(3, tracker.size());

        assertEquals(new DeliveryType(0), tracker.remove(0));
        assertEquals(null, tracker.remove(0));

        assertEquals(2, tracker.size());

        assertEquals(new DeliveryType(1), tracker.remove(1));
        assertEquals(null, tracker.remove(1));

        assertEquals(1, tracker.size());

        assertEquals(new DeliveryType(2), tracker.remove(2));
        assertEquals(null, tracker.remove(2));

        assertEquals(0, tracker.size());
    }

    @Test
    public void testRemoveValueNotInMap() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(9, new DeliveryType(9));
        tracker.put(7, new DeliveryType(7));
        tracker.put(-1, new DeliveryType(-1));

        assertNull(tracker.remove(5));
    }

    @Test
    public void testRemoveFirstEntryTwice() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(16, new DeliveryType(16));

        assertNotNull(tracker.remove(0));
        assertNull(tracker.remove(0));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testRemoveWithInvalidType() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));

        assertNull(tracker.remove("foo"));
    }

    @Test
    public void testRemoveUnsignedInteger() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(UnsignedInteger.valueOf(9), new DeliveryType(9));
        tracker.put(7, new DeliveryType(7));
        tracker.put(UnsignedInteger.valueOf(-1), new DeliveryType(-1));

        assertEquals(5, tracker.size());
        assertNull(tracker.remove(UnsignedInteger.valueOf(5)));
        assertEquals(5, tracker.size());
        assertEquals(new DeliveryType(9), tracker.remove(UnsignedInteger.valueOf(9)));
        assertEquals(4, tracker.size());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testRemoveInteger() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(UnsignedInteger.valueOf(9), new DeliveryType(9));
        tracker.put(7, new DeliveryType(7));
        tracker.put(UnsignedInteger.valueOf(-1), new DeliveryType(-1));

        assertEquals(5, tracker.size());
        assertNull(tracker.remove(Integer.valueOf(5)));
        assertEquals(5, tracker.size());
        assertEquals(new DeliveryType(9), tracker.remove(Integer.valueOf(9)));
        assertEquals(4, tracker.size());
    }

    @Test
    public void testRemoveEntriesFromMiddleBucket() {
        // Start with three buckets of size two
        UnsettledMap<DeliveryType> tracker = createMap(3, 2);

        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));
        tracker.put(4, new DeliveryType(4));
        tracker.put(5, new DeliveryType(5));
        tracker.put(6, new DeliveryType(6));

        assertEquals(6, tracker.size());

        tracker.remove(3);
        tracker.remove(4);

        assertEquals(4, tracker.size());

        assertTrue(tracker.containsKey(1));
        assertTrue(tracker.containsKey(2));
        assertTrue(tracker.containsKey(5));
        assertTrue(tracker.containsKey(6));

        assertFalse(tracker.containsKey(3));
        assertFalse(tracker.containsKey(4));

        tracker.put(7, new DeliveryType(7));
        tracker.put(8, new DeliveryType(8));

        assertEquals(6, tracker.size());
    }

    @Test
    public void testRemoveOneFromTail() {
        UnsettledMap<DeliveryType> tracker = createMap(2, 10);

        for (int i = 0; i < 10; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertNotNull(tracker.remove(1)); // Right after tail value of zero

        assertEquals(0, tracker.get(0).getDeliveryId());
        assertEquals(9, tracker.get(9).getDeliveryId());
    }

    @Test
    public void testRemoveOneFromHead() {
        UnsettledMap<DeliveryType> tracker = createMap(2, 10);

        for (int i = 0; i < 10; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertNotNull(tracker.remove(8)); // Right before head of the list value nine

        assertEquals(0, tracker.get(0).getDeliveryId());
        assertEquals(9, tracker.get(9).getDeliveryId());
    }

    @Test
    public void testInsert() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));
        tracker.put(5, new DeliveryType(5));
        tracker.put(9, new DeliveryType(9));
        tracker.put(7, new DeliveryType(7));
        tracker.put(-1, new DeliveryType(-1));

        assertEquals(8, tracker.size());
    }

    @Test
    public void testInsertUnsignedInteger() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(UnsignedInteger.valueOf(0), new DeliveryType(0));
        tracker.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        tracker.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        tracker.put(UnsignedInteger.valueOf(3), new DeliveryType(3));
        tracker.put(UnsignedInteger.valueOf(5), new DeliveryType(5));
        tracker.put(UnsignedInteger.valueOf(9), new DeliveryType(9));
        tracker.put(UnsignedInteger.valueOf(7), new DeliveryType(7));
        tracker.put(UnsignedInteger.valueOf(-1), new DeliveryType(-1));

        assertEquals(8, tracker.size());
    }

    @Test
    public void testPutAll() {
        UnsettledMap<DeliveryType> tracker = createMap();

        Map<UnsignedInteger, DeliveryType> hashmap = new TreeMap<>();

        hashmap.put(UnsignedInteger.valueOf(0), new DeliveryType(0));
        hashmap.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        hashmap.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        hashmap.put(UnsignedInteger.valueOf(3), new DeliveryType(3));
        hashmap.put(UnsignedInteger.valueOf(5), new DeliveryType(5));
        hashmap.put(UnsignedInteger.valueOf(9), new DeliveryType(9));
        hashmap.put(UnsignedInteger.valueOf(7), new DeliveryType(7));
        hashmap.put(UnsignedInteger.valueOf(-1), new DeliveryType(-1));

        tracker.putAll(hashmap);

        assertEquals(8, tracker.size());

        assertEquals(new DeliveryType(0), tracker.get(0));
        assertEquals(new DeliveryType(1), tracker.get(1));
        assertEquals(new DeliveryType(2), tracker.get(2));
        assertEquals(new DeliveryType(3), tracker.get(3));
        assertEquals(new DeliveryType(5), tracker.get(5));
        assertEquals(new DeliveryType(9), tracker.get(9));
        assertEquals(new DeliveryType(7), tracker.get(7));
        assertEquals(new DeliveryType(-1), tracker.get(-1));
    }

    @Test
    public void testPutIfAbsent() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(UnsignedInteger.valueOf(0), new DeliveryType(0));
        tracker.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        tracker.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        tracker.put(UnsignedInteger.valueOf(3), new DeliveryType(3));
        tracker.put(UnsignedInteger.valueOf(5), new DeliveryType(5));
        tracker.put(UnsignedInteger.valueOf(7), new DeliveryType(7));
        tracker.put(UnsignedInteger.valueOf(9), new DeliveryType(9));
        tracker.put(UnsignedInteger.valueOf(-1), new DeliveryType(-1));

        assertEquals(8, tracker.size());

        assertEquals(new DeliveryType(0), tracker.get(0));
        assertEquals(new DeliveryType(1), tracker.get(1));
        assertEquals(new DeliveryType(2), tracker.get(2));
        assertEquals(new DeliveryType(3), tracker.get(3));
        assertEquals(new DeliveryType(5), tracker.get(5));
        assertEquals(new DeliveryType(7), tracker.get(7));
        assertEquals(new DeliveryType(9), tracker.get(9));
        assertEquals(new DeliveryType(-1), tracker.get(-1));

        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(0), new DeliveryType(0)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(1), new DeliveryType(1)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(2), new DeliveryType(2)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(3), new DeliveryType(3)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(5), new DeliveryType(5)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(7), new DeliveryType(7)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(9), new DeliveryType(9)));
        assertNotNull(tracker.putIfAbsent(UnsignedInteger.valueOf(-1), new DeliveryType(-1)));

        assertEquals(8, tracker.size());

        assertEquals(new DeliveryType(0), tracker.get(0));
        assertEquals(new DeliveryType(1), tracker.get(1));
        assertEquals(new DeliveryType(2), tracker.get(2));
        assertEquals(new DeliveryType(3), tracker.get(3));
        assertEquals(new DeliveryType(5), tracker.get(5));
        assertEquals(new DeliveryType(7), tracker.get(7));
        assertEquals(new DeliveryType(9), tracker.get(9));
        assertEquals(new DeliveryType(-1), tracker.get(-1));
    }

    @Test
    public void testAddedDeliveriesUpdatesSizeValue() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        DeliveryType delivery1 = new DeliveryType(0);
        DeliveryType delivery2 = new DeliveryType(1);

        tracker.put(delivery1.getDeliveryId(), delivery1);
        assertEquals(1, tracker.size());

        tracker.put(delivery2.getDeliveryId(), delivery2);
        assertEquals(2, tracker.size());
    }

    @Test
    public void testAddThenRemoveDelivery() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        DeliveryType delivery1 = new DeliveryType(127);
        DeliveryType delivery2 = new DeliveryType(32);

        tracker.put(delivery1.getDeliveryId(), delivery1);
        assertEquals(1, tracker.size());
        tracker.remove(delivery1.getDeliveryId());
        assertEquals(0, tracker.size());

        tracker.put(delivery2.getDeliveryId(), delivery2);
        assertEquals(1, tracker.size());
        tracker.remove(delivery2.getDeliveryId());
        assertEquals(0, tracker.size());
    }

    @Test
    public void testAddThenRemoveMultipleDeliveriesInSequence() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        DeliveryType delivery1 = new DeliveryType(Integer.MAX_VALUE);
        DeliveryType delivery2 = new DeliveryType(-1);

        tracker.put(delivery1.getDeliveryId(), delivery1);
        tracker.put(delivery2.getDeliveryId(), delivery2);

        assertEquals(2, tracker.size());
        assertNotNull(tracker.remove(delivery1.getDeliveryId()));
        assertEquals(1, tracker.size());
        assertNotNull(tracker.remove(delivery2.getDeliveryId()));
        assertEquals(0, tracker.size());
    }

    @Test
    public void testAddThenClearMultipleDeliveriesAddedInSequence() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        DeliveryType delivery1 = new DeliveryType(0);
        DeliveryType delivery2 = new DeliveryType(1);
        DeliveryType delivery3 = new DeliveryType(2);
        DeliveryType delivery4 = new DeliveryType(3);

        tracker.put(delivery1.getDeliveryId(), delivery1);
        tracker.put(delivery2.getDeliveryId(), delivery2);
        tracker.put(delivery3.getDeliveryId(), delivery3);
        tracker.put(delivery4.getDeliveryId(), delivery4);

        assertEquals(4, tracker.size());
        tracker.clear();
        assertEquals(0, tracker.size());
    }

    @Test
    public void testGetOneDeliveryInBetweenOthersThatWereAdded() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        DeliveryType delivery1 = new DeliveryType(0);
        DeliveryType delivery2 = new DeliveryType(1);
        DeliveryType delivery3 = new DeliveryType(2);
        DeliveryType delivery4 = new DeliveryType(3);

        tracker.put(delivery1.getDeliveryId(), delivery1);
        tracker.put(delivery2.getDeliveryId(), delivery2);
        tracker.put(delivery3.getDeliveryId(), delivery3);
        tracker.put(delivery4.getDeliveryId(), delivery4);

        assertEquals(4, tracker.size());
        assertEquals(delivery3, tracker.get(delivery3.getDeliveryId()));
        assertEquals(4, tracker.size());
    }

    @Test
    public void testAddLargeSeriesOfDeliveriesAndThenEnumerateOverThemWithGet() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 4080;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        for (int i = 0; i < COUNT; ++i) {
            assertEquals(i, tracker.get(i).getDeliveryId());
        }
    }

    @Test
    public void testAddLargeSeriesOfDeliveriesAndThenIterateOverThemWithValues() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 4080;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        Collection<DeliveryType> values = tracker.values();

        int index = 0;

        for (DeliveryType delivery : values) {
            assertEquals(index++, delivery.getDeliveryId());
        }

        assertEquals(index, COUNT);
    }

    @Test
    public void testRemoveAllViaIteration() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 16;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        Collection<DeliveryType> values = tracker.values();
        assertEquals(COUNT, values.size());
        Iterator<DeliveryType> iter = values.iterator();

        int index = 0;

        while (iter.hasNext()) {
            assertEquals(index++, iter.next().getDeliveryId());
            iter.remove();
        }

        assertEquals(index, COUNT);
        assertEquals(0, tracker.size());
    }

    @Test
    public void testAddLargeSeriesOfDeliveriesAndThenRemoveAllViaIteration() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 4080;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        Collection<DeliveryType> values = tracker.values();
        assertEquals(COUNT, values.size());
        Iterator<DeliveryType> iter = values.iterator();

        int index = 0;

        while (iter.hasNext()) {
            assertEquals(index++, iter.next().getDeliveryId());
            iter.remove();
        }

        assertEquals(index, COUNT);
        assertEquals(0, tracker.size());
    }

    @Test
    public void testIteratorRemoveInChunks() {
        UnsettledMap<DeliveryType> tracker = createMap(3, 6);
        assertEquals(0, tracker.size());

        final int COUNT = 18;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        Collection<DeliveryType> values = tracker.values();
        assertEquals(COUNT, values.size());
        Iterator<DeliveryType> iter = values.iterator();

        int index = 0;
        int count = 0;

        while (iter.hasNext()) {
            assertEquals(index++, iter.next().getDeliveryId());

            if (count++ < COUNT / 6) {
                iter.remove();
            }

            if (count == 6) {
                count = 0;
            }
        }

        assertEquals(index, COUNT);
        assertEquals(COUNT / 2, tracker.size());
    }

    @Test
    public void testRemoveUsingIteratorFromFullMiddleBucket() {
        UnsettledMap<DeliveryType> tracker = createMap(3, 6);
        assertEquals(0, tracker.size());

        final int COUNT = 18;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        // Remove enough from front and back buckets that
        // a drain of the middle should compress the chain

        // Front
        tracker.remove(0);
        tracker.remove(1);
        tracker.remove(2);
        // Back
        tracker.remove(15);
        tracker.remove(16);
        tracker.remove(17);

        Collection<DeliveryType> values = tracker.values();
        Iterator<DeliveryType> iter = values.iterator();

        // Skip elements from first section
        iter.next();
        iter.next();
        iter.next();

        for (int i = 6; i < 12; ++i) {
            assertEquals(i, iter.next().getDeliveryId());
            iter.remove();
        }

        assertEquals(6, tracker.size());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testValuesCollection() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));

        Collection<DeliveryType> values = tracker.values();
        assertNotNull(values);
        assertEquals(4, values.size());
        assertFalse(values.isEmpty());
        assertSame(values, tracker.values());
        assertFalse(values.contains("foo"));
        assertTrue(values.contains(new DeliveryType(3)));
        assertTrue(values.remove(new DeliveryType(3)));
        assertFalse(values.remove(new DeliveryType(3)));
        assertFalse(values.contains(new DeliveryType(3)));

        values.clear();
        assertEquals(0, values.size());
        assertTrue(values.isEmpty());
    }

    @Test
    public void testValuesIteration() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<DeliveryType> values = tracker.values();
        Iterator<DeliveryType> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(new DeliveryType(intValues[counter++]), iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
    }

    @Test
    public void testValuesIterationRemove() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<DeliveryType> values = tracker.values();
        Iterator<DeliveryType> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(new DeliveryType(intValues[counter++]), iterator.next());
            iterator.remove();
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
        assertTrue(tracker.isEmpty());
        assertEquals(0, tracker.size());
    }

    @Test
    public void testValuesIterationFollowUnsignedOrderingExpectations() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<DeliveryType> values = tracker.values();
        Iterator<DeliveryType> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(new DeliveryType(inputValues[counter++]), iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(inputValues.length, counter);
    }

    @Test
    public void testValuesIterationFailsWhenConcurrentlyModified() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {1, 2, 3, 5, 7, 9, 11};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<DeliveryType> values = tracker.values();
        Iterator<DeliveryType> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        tracker.remove(3);

        try {
            iterator.next();
            fail("Should not iterate when modified outside of iterator");
        } catch (ConcurrentModificationException cme) {}
    }

    @Test
    public void testValuesIterationOnEmptyTree() {
        UnsettledMap<DeliveryType> tracker = createMap();
        Collection<DeliveryType> values = tracker.values();
        Iterator<DeliveryType> iterator = values.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testKeySetReturned() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));

        Set<UnsignedInteger> keys = tracker.keySet();
        assertNotNull(keys);
        assertEquals(4, keys.size());
        assertFalse(keys.isEmpty());
        assertSame(keys, tracker.keySet());

        final UnsignedInteger VALUE = UnsignedInteger.valueOf(3);

        assertFalse(keys.contains("foo"));
        assertTrue(keys.contains(VALUE));
        assertTrue(keys.remove(VALUE));
        assertFalse(keys.remove(VALUE));
        assertFalse(keys.contains(VALUE));

        keys.clear();
        assertEquals(0, keys.size());
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testKeysIterationRemove() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<UnsignedInteger> keys = tracker.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(UnsignedInteger.valueOf(intValues[counter++]), iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
    }

    @Test
    public void testKeysIterationRemoveContract() {
        Set<UnsignedInteger> set = tracker.keySet();
        Iterator<UnsignedInteger> iter = set.iterator();
        iter.next();
        iter.remove();

        // No remove allowed again until next is called
        assertThrows(IllegalStateException.class, () -> iter.remove());

        iter.next();
        iter.remove();

        assertEquals(998, tracker.size());

        iter.next();
        assertNotNull(tracker.remove(999));

        assertThrows(ConcurrentModificationException.class, () -> iter.remove());
    }

    @Test
    public void testKeysIteration() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<UnsignedInteger> keys = tracker.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(UnsignedInteger.valueOf(intValues[counter++]), iterator.next());
            iterator.remove();
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
        assertTrue(tracker.isEmpty());
        assertEquals(0, tracker.size());
    }

    @Test
    public void testKeysIterationFollowsUnsignedOrderingExpectations() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<UnsignedInteger> keys = tracker.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(UnsignedInteger.valueOf(inputValues[counter++]), iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(inputValues.length, counter);
    }

    @Test
    public void testKeysIterationFailsWhenConcurrentlyModified() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {1, 3, 5, 7, 9, 11, 13};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Collection<UnsignedInteger> keys = tracker.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        tracker.remove(3);

        try {
            iterator.next();
            fail("Should not iterate when modified outside of iterator");
        } catch (ConcurrentModificationException cme) {}
    }

    @Test
    public void testKeysIterationOnEmptyTree() {
        UnsettledMap<DeliveryType> tracker = createMap();
        Collection<UnsignedInteger> keys = tracker.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @Test
    public void testKeySetRemoveAllFromCollection() {
        final Collection<UnsignedInteger> collection = Arrays.asList(uintArray);

        assertTrue(tracker.keySet().removeAll(collection));
        assertEquals(0, tracker.size());
        assertFalse(tracker.keySet().iterator().hasNext());

        // Second attempt should do nothing.
        assertFalse(tracker.keySet().removeAll(collection));
    }

    @Test
    public void testKeySetRetainAllFromCollectionAtZero() {
        doTestKeySetRetainAllFromCollection(0);
    }

    @Test
    public void testKeySetRetainAllFromCollectionAtOne() {
        doTestKeySetRetainAllFromCollection(1);
    }

    @Test
    public void testKeySetRetainAllFromCollectionAtTwoHundered() {
        doTestKeySetRetainAllFromCollection(200);
    }

    @Test
    public void testKeySetRetainAllFromCollectionAtFiveHundred() {
        doTestKeySetRetainAllFromCollection(500);
    }

    private void doTestKeySetRetainAllFromCollection(int index) {
        final Collection<UnsignedInteger> collection = new ArrayList<>();
        collection.add(uintArray[index]);

        assertEquals(1000, tracker.size());

        final Set<UnsignedInteger> keys = tracker.keySet();

        keys.retainAll(collection);
        assertEquals(1, tracker.size());
        keys.removeAll(collection);
        assertEquals(0, tracker.size());
        tracker.put(1, new DeliveryType(1));
        assertEquals(1, tracker.size());
        keys.clear();
        assertEquals(0, tracker.size());
    }

    @Test
    public void testKeySetRetainAllFromCollectionWhenMapHasCustomBucketsAndRetainedIsInFirstBucket() {
        // Start with three buckets of size three
        UnsettledMap<DeliveryType> tracker = createMap(3, 3);

        tracker.put(1, new DeliveryType(1)); // First
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));
        tracker.put(4, new DeliveryType(4)); // Second
        tracker.put(5, new DeliveryType(5));
        tracker.put(6, new DeliveryType(6));
        tracker.put(7, new DeliveryType(7)); // Third
        tracker.put(8, new DeliveryType(8));
        tracker.put(9, new DeliveryType(9));

        assertEquals(9, tracker.size());

        final Collection<UnsignedInteger> collection = new ArrayList<>();
        collection.add(UnsignedInteger.valueOf(1));  // Retain element from bucket one

        final Set<UnsignedInteger> keys = tracker.keySet();

        keys.retainAll(collection);
        assertEquals(1, tracker.size());
        keys.removeAll(collection);
        assertEquals(0, tracker.size());
        tracker.put(1, new DeliveryType(1));
        assertEquals(1, tracker.size());
        keys.clear();
        assertEquals(0, tracker.size());
    }

    @Test
    public void testKeySetRetainAllFromCollectionWhenMapHasCustomBucketsAndRetainedIsInLastBucket() {
        // Start with three buckets of size three
        UnsettledMap<DeliveryType> tracker = createMap(3, 3);

        tracker.put(1, new DeliveryType(1)); // First
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));
        tracker.put(4, new DeliveryType(4)); // Second
        tracker.put(5, new DeliveryType(5));
        tracker.put(6, new DeliveryType(6));
        tracker.put(7, new DeliveryType(7)); // Third
        tracker.put(8, new DeliveryType(8));
        tracker.put(9, new DeliveryType(9));

        assertEquals(9, tracker.size());

        final Collection<UnsignedInteger> collection = new ArrayList<>();
        collection.add(UnsignedInteger.valueOf(7));  // Retain element from bucket three

        final Set<UnsignedInteger> keys = tracker.keySet();

        keys.retainAll(collection);
        assertEquals(1, tracker.size());
        keys.removeAll(collection);
        assertEquals(0, tracker.size());
        tracker.put(1, new DeliveryType(1));
        assertEquals(1, tracker.size());
        keys.clear();
        assertEquals(0, tracker.size());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEntrySetReturned() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));

        Set<Entry<UnsignedInteger, DeliveryType>> entries= tracker.entrySet();
        assertNotNull(entries);
        assertEquals(4, entries.size());
        assertFalse(entries.isEmpty());
        assertSame(entries, tracker.entrySet());

        final UnsettledMap.ImmutableUnsettledTrackingMapEntry<DeliveryType> entry =
            new UnsettledMap.ImmutableUnsettledTrackingMapEntry<DeliveryType>(3, new DeliveryType(3));

        assertEquals(3, entry.getPrimitiveKey());
        assertEquals(3, entry.getValue().getDeliveryId());
        assertThrows(UnsupportedOperationException.class, () -> entry.setValue(new DeliveryType(4)));

        assertFalse(entries.contains("foo"));
        assertFalse(entries.remove("foo"));
        assertTrue(entries.contains(entry));
        assertTrue(entries.remove(entry));
        assertFalse(entries.remove(entry));
        assertFalse(entries.contains(entry));

        entries.clear();
        assertEquals(0, entries.size());
        assertTrue(entries.isEmpty());
    }

    @Test
    public void testEntrySetContainsSimple() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(2, new DeliveryType(2));
        tracker.put(3, new DeliveryType(3));

        Set<Entry<UnsignedInteger, DeliveryType>> entries = tracker.entrySet();
        assertNotNull(entries);
        assertEquals(4, entries.size());
        assertFalse(entries.isEmpty());
        assertSame(entries, tracker.entrySet());

        OutsideEntry<UnsignedInteger, DeliveryType> entry1 = new OutsideEntry<>(UnsignedInteger.valueOf(0), new DeliveryType(0));
        OutsideEntry<UnsignedInteger, DeliveryType> entry2 = new OutsideEntry<>(UnsignedInteger.valueOf(7), new DeliveryType(7));

        assertTrue(entries.contains(entry1));
        assertFalse(entries.contains(entry2));
    }

    @Test
    public void testEntryIteration() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Set<Entry<UnsignedInteger, DeliveryType>> entries= tracker.entrySet();
        Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            Entry<UnsignedInteger, DeliveryType> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(UnsignedInteger.valueOf(intValues[counter]), entry.getKey());
            assertEquals(new DeliveryType(intValues[counter++]), entry.getValue());
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
    }

    @Test
    public void testEntryIterationRemove() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Set<Entry<UnsignedInteger, DeliveryType>> entries= tracker.entrySet();
        Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            Entry<UnsignedInteger, DeliveryType> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(UnsignedInteger.valueOf(intValues[counter]), entry.getKey());
            assertEquals(new DeliveryType(intValues[counter++]), entry.getValue());
            iterator.remove();
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
        assertTrue(tracker.isEmpty());
        assertEquals(0, tracker.size());
    }

    @Test
    public void testEntryIterationFollowsIntersectionOrderingExpectations() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Set<Entry<UnsignedInteger, DeliveryType>> entries= tracker.entrySet();
        Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            Entry<UnsignedInteger, DeliveryType> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(UnsignedInteger.valueOf(inputValues[counter]), entry.getKey());
            assertEquals(new DeliveryType(inputValues[counter++]), entry.getValue());
        }

        // Check that we really did iterate.
        assertEquals(inputValues.length, counter);
    }

    @Test
    public void testEntryIterationFailsWhenConcurrentlyModified() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {2, 3, 5, 9, 12, 42};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        Set<Entry<UnsignedInteger, DeliveryType>> entries= tracker.entrySet();
        Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        tracker.remove(3);

        try {
            iterator.next();
            fail("Should not iterate when modified outside of iterator");
        } catch (ConcurrentModificationException cme) {}
    }

    @Test
    public void testEntrySetIterationOnEmptyTree() {
        UnsettledMap<DeliveryType> tracker = createMap();
        Set<Entry<UnsignedInteger, DeliveryType>> entries= tracker.entrySet();
        Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = entries.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @Test
    public void testRandomProduceAndConsumeWithBacklog() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int ITERATIONS = 8192;

        try {

            for (int i = 0; i < ITERATIONS; ++i) {
                tracker.put(UnsignedInteger.valueOf(i), new DeliveryType(i));
            }

            assertAllTrackedDeliveriesAreReachable(tracker);

            for (int i = 0; i < ITERATIONS; ++i) {
                int p = random.nextInt(ITERATIONS);
                int c = random.nextInt(ITERATIONS);

                tracker.put(UnsignedInteger.valueOf(p), new DeliveryType(p));
                tracker.remove(UnsignedInteger.valueOf(c));

                if (i % 2048 == 0) {
                    assertAllTrackedDeliveriesAreReachable(tracker);
                }
            }

            assertAllTrackedDeliveriesAreReachable(tracker);
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testRandomPutAndGetIntoEmptyMap() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int ITERATIONS = 8192;

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                int p = random.nextInt(ITERATIONS);
                int c = random.nextInt(ITERATIONS);

                tracker.put(UnsignedInteger.valueOf(p), new DeliveryType(p));
                tracker.remove(UnsignedInteger.valueOf(c));

                if (i % 2048 == 0) {
                    assertAllTrackedDeliveriesAreReachable(tracker);
                }
            }

            assertAllTrackedDeliveriesAreReachable(tracker);
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testRandomPutAndGetIntoEmptyMapWithCustomBucketSize() {
        UnsettledMap<DeliveryType> tracker = createMap(2, 4);

        final int ITERATIONS = 8192;

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                int p = random.nextInt(ITERATIONS);
                int c = random.nextInt(ITERATIONS);

                tracker.put(UnsignedInteger.valueOf(p), new DeliveryType(p));
                tracker.remove(UnsignedInteger.valueOf(c));

                if (i % 2048 == 0) {
                    assertAllTrackedDeliveriesAreReachable(tracker);
                }
            }

            assertAllTrackedDeliveriesAreReachable(tracker);
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testPutEntriesWithDuplicateIdsIntoMapThenRemoveInSameOrder() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] ids = new int[] { 4, 7, 5, 0, 7, 7, 0 };

        for(int id : ids) {
            tracker.put(id, new DeliveryType(id));
        }

        for (int id : ids) {
            assertEquals(new DeliveryType(id), tracker.get(id));
        }

        for (int id : ids) {
            assertEquals(new DeliveryType(id), tracker.remove(id));
        }

        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testPutThatBreaksOrderLeavesMapUsable() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] ids = new int[] { 1, 2, 0 };

        for(int id : ids) {
            tracker.put(id, new DeliveryType(id));
        }

        for (int id : ids) {
            assertEquals(new DeliveryType(id), tracker.get(id));
        }

        for (int id : ids) {
            assertEquals(new DeliveryType(id), tracker.remove(id));
        }

        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testPutInSeriesAndRemoveAllValuesRandomly() {
        UnsettledMap<DeliveryType> tracker = createMap();

        List<UnsignedInteger> values = new ArrayList<>();
        List<UnsignedInteger> removes = new ArrayList<>();

        final int ITERATIONS = 8192;

        for (int i = 0; i < ITERATIONS; ++i) {
            values.add(UnsignedInteger.valueOf(i));
        }

        removes.addAll(values);
        Collections.shuffle(removes, random);

        try {
            for (UnsignedInteger id : values) {
                tracker.put(id, new DeliveryType(id.intValue()));
            }

            assertEquals(ITERATIONS, tracker.size());

            for (UnsignedInteger id : values) {
                assertEquals(new DeliveryType(id.intValue()), tracker.get(id));
            }

            for (UnsignedInteger id : removes) {
                assertEquals(new DeliveryType(id.intValue()), tracker.remove(id));
            }

            assertTrue(tracker.isEmpty());
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testPutInRandomOrderAndRemoveAllValuesInSeries() {
        UnsettledMap<DeliveryType> tracker = createMap();

        List<UnsignedInteger> values = new ArrayList<>();
        List<UnsignedInteger> removes = new ArrayList<>();

        final int ITERATIONS = 8192;

        for (int i = 0; i < ITERATIONS; ++i) {
            values.add(UnsignedInteger.valueOf(i));
        }

        removes.addAll(values);
        Collections.shuffle(values, random);

        try {
            for (UnsignedInteger id : values) {
                tracker.put(id, new DeliveryType(id.intValue()));
            }

            assertEquals(ITERATIONS, tracker.size());

            for (UnsignedInteger id : values) {
                assertEquals(new DeliveryType(id.intValue()), tracker.get(id));
            }

            for (UnsignedInteger id : removes) {
                assertEquals(new DeliveryType(id.intValue()), tracker.remove(id));
            }

            assertTrue(tracker.isEmpty());
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testPutInRandomOrderAndRemoveAllValuesInRandomOrder() {
        UnsettledMap<DeliveryType> tracker = createMap();

        List<UnsignedInteger> values = new ArrayList<>();
        List<UnsignedInteger> removes = new ArrayList<>();

        final int ITERATIONS = 8192;

        for (int i = 0; i < ITERATIONS; ++i) {
            values.add(UnsignedInteger.valueOf(i));
        }

        removes.addAll(values);
        Collections.shuffle(values, random);
        Collections.shuffle(removes, random);

        try {
            for (UnsignedInteger id : values) {
                tracker.put(id, new DeliveryType(id.intValue()));
            }

            assertEquals(ITERATIONS, tracker.size());

            for (UnsignedInteger id : values) {
                assertEquals(new DeliveryType(id.intValue()), tracker.get(id));
            }

            for (UnsignedInteger id : removes) {
                assertEquals(new DeliveryType(id.intValue()), tracker.remove(id));
            }

            assertTrue(tracker.isEmpty());
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testPutRandomValueIntoMapThenRemoveInSameOrder() {
        final UnsettledMap<DeliveryType> tracker = createMap();

        final int ITERATIONS = 8192;

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                final int index = random.nextInt(ITERATIONS);
                tracker.put(index, new DeliveryType(index));
            }

            // Reset to verify insertions
            random.setSeed(seed);

            for (int i = 0; i < ITERATIONS; ++i) {
                final int index = random.nextInt(ITERATIONS);
                assertEquals(new DeliveryType(index), tracker.get(index));
            }

            // Reset to remove
            random.setSeed(seed);

            for (int i = 0; i < ITERATIONS; ++i) {
                final int index = random.nextInt(ITERATIONS);
                assertEquals(new DeliveryType(index), tracker.remove(index));
            }

            assertTrue(tracker.isEmpty());
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, seed, true);
            throw error;
        }
    }

    @Test
    public void testPutInSeriesAndClear() {
        final UnsettledMap<DeliveryType> tracker = createMap();

        final int LOOPS = 16;
        final int ITERATIONS = 8192;

        int putDeliveryId = 0;
        int getDeliveryId = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            try {
                for (int i = 0; i < ITERATIONS; ++i, putDeliveryId++) {
                    tracker.put(putDeliveryId, new DeliveryType(putDeliveryId));
                }

                for (int i = 0; i < ITERATIONS; ++i, getDeliveryId++) {
                    assertEquals(new DeliveryType(getDeliveryId), tracker.get(getDeliveryId));
                }

                tracker.clear();

                assertTrue(tracker.isEmpty());
            } catch (Throwable error) {
                dumpRandomDataSet(ITERATIONS, seed, true);
                throw error;
            }
        }
    }

    @Test
    public void testPutInSeriesAndRemoveInSeries() {
        final UnsettledMap<DeliveryType> tracker = createMap();

        final int LOOPS = 16;
        final int ITERATIONS = 8192;

        int putDeliveryId = 0;
        int getDeliveryId = 0;
        int removeDeliveryId = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            try {
                for (int i = 0; i < ITERATIONS; ++i, putDeliveryId++) {
                    tracker.put(putDeliveryId, new DeliveryType(putDeliveryId));
                }

                for (int i = 0; i < ITERATIONS; ++i, getDeliveryId++) {
                    assertEquals(new DeliveryType(getDeliveryId), tracker.get(getDeliveryId));
                }

                for (int i = 0; i < ITERATIONS; ++i, removeDeliveryId++) {
                    assertEquals(new DeliveryType(removeDeliveryId), tracker.remove(removeDeliveryId));
                }

                assertTrue(tracker.isEmpty());
            } catch (Throwable error) {
                dumpRandomDataSet(ITERATIONS, seed, true);
                throw error;
            }
        }
    }

    @Test
    public void testEqualsJDKMapTypes() {
        Map<UnsignedInteger, DeliveryType> m1 = createMap();
        Map<UnsignedInteger, DeliveryType> m2 = createMap();

        m1.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        m1.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        m2.put(UnsignedInteger.valueOf(3), new DeliveryType(3));
        m2.put(UnsignedInteger.valueOf(4), new DeliveryType(4));

        assertNotEquals(m1, m2, "Maps should not be equal 1");
        assertNotEquals(m2, m1, "Maps should not be equal 2");

        // comparing UnsettledMap3 with HashMap with equal values
        m1 = createMap();
        m2 = new HashMap<>();
        m1.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        m2.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        assertNotEquals(m1, m2, "Maps should not be equal 3");
        assertNotEquals(m2, m1, "Maps should not be equal 4");

        // comparing UnsettledMap3 with differing objects inside values
        m1 = createMap();
        m2 = createMap();
        m1.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        m2.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        assertNotEquals(m1, m2, "Maps should not be equal 5");
        assertNotEquals(m2, m1, "Maps should not be equal 6");

        // comparing UnsettledMap3 with same objects inside values
        m1 = createMap();
        m2 = createMap();
        m1.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        m2.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        assertTrue(m1.equals(m2), "Maps should be equal 7");
        assertTrue(m2.equals(m1), "Maps should be equal 8");
        assertTrue(m1.equals(m1), "Maps should be equal 9");

        assertNotEquals(m2, "foo", "Maps should not be equal 10");

        // comparing UnsettledMap3 with different sizes
        m1 = createMap();
        m2 = createMap();

        m1.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        assertNotEquals(m1, m2, "Maps should not be equal 11");
    }

    @Test
    public void testEntrySetContains() {
        UnsettledMap<DeliveryType> first = createMap();
        UnsettledMap<DeliveryType> second = createMap();

        first.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        Object[] entry = first.entrySet().toArray();
        assertFalse(second.entrySet().contains(entry[0]),
            "Empty map should not contain anything from first map");

        second.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        assertTrue(second.entrySet().containsAll(first.entrySet()),
            "entrySet().containsAll(...) should work with values");

        first.clear();
        first.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        entry = first.entrySet().toArray();
        assertTrue(second.entrySet().contains(entry[0]),
            "new valued entry with same delivery ID should equal old valued entry");
        first.put(UnsignedInteger.valueOf(2), new DeliveryType(2));
        entry = first.entrySet().toArray();
        assertFalse(second.entrySet().contains(entry[1]),
            "additional value in first should not match any in second");
    }

    @Test
    public void testValues() {
        Collection<DeliveryType> vals = tracker.values();
        vals.iterator();
        assertEquals(vals.size(), objArray.length, "Returned collection of incorrect size");
        for (DeliveryType element : objArray) {
            assertTrue(vals.contains(element), "Collection contains incorrect elements");
        }

        assertEquals(1000, vals.size());
        int j = 0;
        for (Iterator<DeliveryType> iter = vals.iterator(); iter.hasNext(); j++) {
            DeliveryType element = iter.next();
            assertNotNull(element);
        }
        assertEquals(1000, j);

        UnsettledMap<DeliveryType> myMap = new UnsettledMap<DeliveryType>(DeliveryType::getDeliveryId);
        for (int i = 0; i < 100; i++) {
            myMap.put(uintArray[i], objArray[i]);
        }
        Collection<DeliveryType> values = myMap.values();
        assertEquals(100, values.size());
        assertTrue(values.remove(new DeliveryType(1)));
        assertTrue(!myMap.containsKey(UnsignedInteger.ONE), "Removing from the values collection should remove from the original map");
        assertTrue(!myMap.containsValue(new DeliveryType(1)), "Removing from the values collection should remove from the original map");
        assertEquals(99, values.size());
        j = 0;
        for (Iterator<DeliveryType> iter = values.iterator(); iter.hasNext(); j++) {
            iter.next();
        }
        assertEquals(99, j);
    }

    @Test
    public void testRemoveValueUsingValuesIteratorAndCheckAllOtherValuesRemain() {
        Iterator<DeliveryType> iterator = tracker.values().iterator();

        DeliveryType removed = null;

        for (int i = 0; i < 10; ++i) {
            removed = iterator.next();
        }

        iterator.remove();

        for (UnsignedInteger id : uintArray) {
            if (id.intValue() != removed.getDeliveryId()) {
                assertTrue(tracker.containsKey(id.intValue()));
            } else {
                assertFalse(tracker.containsKey(id.intValue()));
            }
        }
    }

    @Test
    public void testRemoveFromIteratorFromMiddleBucket() {
        final int numBuckets = 5;
        final int bucketSize = 10;
        final int numEntries = numBuckets * bucketSize;

        final UnsettledMap<DeliveryType> map = createMap(numBuckets, bucketSize);

        for (int i = 0; i < numEntries; ++i) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(numEntries, map.size());

        Iterator<UnsignedInteger> entries = map.keySet().iterator();

        // Move to center of bucket two
        for (int i = 0; i < bucketSize + (bucketSize / 2); ++i) {
            entries.next();
        }

        UnsignedInteger lastValue = null;

        // Remove from center of bucket two into bucket three until a compaction event should occur.
        for (int i = 0; i < bucketSize; ++i) {
            lastValue = entries.next();
            entries.remove();
        }

        assertEquals(lastValue.intValue() + 1, entries.next().intValue());
    }

    @Test
    public void testRemoveFromMiddleBucket() {
        final int numBuckets = 5;
        final int bucketSize = 10;
        final int numEntries = numBuckets * bucketSize;

        final UnsettledMap<DeliveryType> map = createMap(numBuckets, bucketSize);

        for (int i = 0; i < numEntries; ++i) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(numEntries, map.size());

        int position = bucketSize + (bucketSize / 2);
        int lastValue = 0;

        // Remove from center of bucket two into bucket three until a compaction event should occur.
        for (int i = 0; i < bucketSize + 3; ++i) {
            lastValue = map.get(position).getDeliveryId();
            map.remove(position++);
        }

        assertEquals(lastValue + 1, map.get(position).getDeliveryId());
    }

    @Test
    public void testRepeatedRemoveOldestHotPath() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int COUNT = 10000;

        for (int i = 0; i < COUNT; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        for (int i = 0; i < COUNT; i++) {
            DeliveryType removed = tracker.remove(i);
            assertNotNull(removed);
            assertEquals(i, removed.getDeliveryId());
        }

        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testRemoveOldestAcrossBucketBoundaries() {
        UnsettledMap<DeliveryType> tracker = createMap(3, 4);

        for (int i = 0; i < 12; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        for (int i = 0; i < 12; i++) {
            assertEquals(i, tracker.remove(i).getDeliveryId());
        }

        assertEquals(0, tracker.size());
    }

    @Test
    public void testSlidingWindowPutRemoveOldest() {
        UnsettledMap<DeliveryType> tracker = createMap();

        int window = 1024;

        for (int i = 0; i < window; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        for (int i = 0; i < 5000; i++) {
            tracker.put(window + i, new DeliveryType(window + i));
            tracker.remove(i);
        }

        assertEquals(window, tracker.size());
    }

    @Test
    public void testMixedTailAndMiddleRemovals() {
        UnsettledMap<DeliveryType> tracker = createMap();

        for (int i = 0; i < 1000; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        tracker.remove(0);     // tail
        tracker.remove(500);   // middle
        tracker.remove(1);     // tail again

        assertFalse(tracker.containsKey(0));
        assertFalse(tracker.containsKey(1));
        assertFalse(tracker.containsKey(500));
        assertNotNull(tracker.get(499));
    }

    @Test
    public void testDuplicateIdsAfterWrap() {
        UnsettledMap<DeliveryType> tracker = createMap();

        int nearMax = Integer.MAX_VALUE - 50;

        for (int i = 0; i < 100; i++) {
            tracker.put(nearMax + i, new DeliveryType(nearMax + i));
        }

        // Wrap
        for (int i = 0; i < 100; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        // Validate both ranges exist
        assertNotNull(tracker.get(nearMax + 10));
        assertNotNull(tracker.get(10));
    }

    @Test
    public void testRemoveDuplicateIdRemovesCorrectInstance() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(1, new DeliveryType(1));   // old
        // simulate wrap
        tracker.put(1, new DeliveryType(1));   // new

        tracker.remove(1);

        // One should still remain
        assertTrue(tracker.containsKey(1));
    }

    @Test
    public void testHighestIdLookupFindsEarliestInstanceOfADuplicateId() {
        UnsettledMap<DeliveryType> tracker = createMap();

        DeliveryType original = new DeliveryType(1);
        DeliveryType duplicate = new DeliveryType(1);

        tracker.put(1, original);
        tracker.put(1, duplicate); // duplicate ID forces a new generation at the head

        // The ID being sought is the highest of the series and so the head bucket case, however the
        // search from the tail must still win while an overflow is active as that is where the
        // matching series starts.
        assertSame(original, tracker.get(1), "Should have found the original instance of the ID");
        assertSame(original, tracker.remove(1), "Should have removed the original instance of the ID");

        // The remaining instance is now the only match for the ID and also the highest of the
        // series, which is the case answered directly from the head bucket.
        assertSame(duplicate, tracker.get(1), "Should have found the remaining instance of the ID");
        assertSame(duplicate, tracker.remove(1), "Should have removed the remaining instance of the ID");

        assertTrue(tracker.isEmpty(), "All tracked deliveries should have been settled");
    }

    @Test
    public void testIteratorRemoveAllSequentially() {
        UnsettledMap<DeliveryType> tracker = createMap();

        for (int i = 0; i < 1000; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        Iterator<?> it = tracker.values().iterator();

        while (it.hasNext()) {
            it.next();
            it.remove();
        }

        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testCompactionAtLowWaterMarkBoundary() {
        UnsettledMap<DeliveryType> tracker = createMap(2, 16);

        for (int i = 0; i < 32; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        for (int i = 0; i < 12; i++) {
            tracker.remove(i);
        }

        // Now near low water mark
        tracker.remove(12);

        // Validate still consistent
        assertNotNull(tracker.get(20));
    }

    @Test
    public void testIteratorRemoveTriggeringCompactionFromHeadBucket() {
        UnsettledMap<DeliveryType> tracker = createMap(3, 10); // 30 Entries of capacity

        // Fill the map to capacity but no further should remain at three buckets
        for (int i = 0; i < 30; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        Iterator<DeliveryType> it = tracker.values().iterator();

        // Move to the middle before we start to remove entries
        for (int i = 0; i < 15; i++) {
            it.next();
        }

        DeliveryType lastReturned = null;

        // Remove ten elements which should trigger a compaction and a roll
        // forward of data in the center bucket.
        for (int i = 0; i < 10; i++) {
            it.remove();
            lastReturned = it.next();
        }

        for (int i = 24; i < 30; i++) {
            assertEquals(lastReturned.getDeliveryId(), i);
            if (i < 29) {
                lastReturned = it.next(); // Don't pass the end.
            }
        }
    }

    @Test
    public void testIteratorRemoveTriggeringCompactionToHeadBucket() {
        UnsettledMap<DeliveryType> tracker = createMap(3, 10); // 30 Entries of capacity

        // Fill the map to capacity but no further should remain at three buckets
        for (int i = 0; i < 30; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        Iterator<UnsignedInteger> it = tracker.keySet().iterator();

        // Move to the middle before we start to remove entries
        for (int i = 0; i < 25; i++) {
            it.next();
        }

        // Should now be positioned in the head bucket and we remove the last five entries
        for (int i = 0; i < 5; i++) {
            assertEquals(25 + i,  it.next().intValue());
            it.remove();
        }

        Iterator<DeliveryType> values = tracker.values().iterator();

        for (int i = 0; i < 10; i++) {
            values.next();
        }

        for (int i = 0; i < 5; i++) {
            values.next();
            values.remove();
        }
    }

    @Test
    public void testIteratorRemoveWhenHeadBucketShouldHaveWrappedAround() {
        UnsettledMap<DeliveryType> tracker = createMap(3, 10); // 30 Entries of capacity

        // Fill the map to capacity but no further should remain at three buckets
        for (int i = 0; i < 30; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        // Remove the first 20 so head and tail are now the same.
        for (int i = 0; i < 20; i++) {
            tracker.remove(i);
        }

        // Put another 20 in so that head wraps to the slot behind tail
        for (int i = 30; i < 50; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(30, tracker.size());

        // Remove five from head so that when we remove five from the middle bucket it rolls into head
        tracker.remove(40);
        tracker.remove(41);
        tracker.remove(42);
        tracker.remove(43);
        tracker.remove(44);

        Iterator<UnsignedInteger> it = tracker.keySet().iterator();

        for (int i = 0; i < 15; ++i) {
            assertEquals(20 + i, it.next().intValue());
        }

        for (int i = 0; i < 5; ++i) {
            assertEquals(35 + i, it.next().intValue());
            it.remove();
        }

        for (int i = 0; i < 5; ++i) {
            assertEquals(45 + i, it.next().intValue());
        }
    }

    @Test
    public void testIteratorRemoveBeyondDefaultBucketSizeDoesNotThrow() {
        final int bucketSize = 512;
        final UnsettledMap<DeliveryType> tracker = createMap(2, bucketSize);  // uses test helper

        for (int i = 0; i < bucketSize; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        final Iterator<DeliveryType> it = tracker.values().iterator();

        DeliveryType removed = null;
        for (int i = 0; i <= 300; ++i) {
            removed = it.next();
        }

        assertDoesNotThrow(() -> it.remove());
        assertNotNull(removed);
        assertNull(tracker.get(removed.getDeliveryId()));
        assertEquals(bucketSize - 1, tracker.size());
    }

    @Test
    public void testRemoveFromMiddleDoesNotLoseTailEntriesWhenCompactionTriggered() {
        // Use small bucketSize so bucketLowWaterMark is small and compaction is more likely.
        final UnsettledMap<DeliveryType> tracker = createMap(6, 10);  // low-water ≈ 3

        // Fill enough to create multiple buckets.
        for (int i = 0; i < 60; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        // Make tail bucket sparse (but not empty) so tryCompact(tail) has a chance to do something.
        for (int i = 0; i < 8; ++i) {
            assertNotNull(tracker.remove(i));
        }

        // Now remove from a middle region (not tail) and ensure the early tail-adjacent IDs remain.
        // If removeValue incorrectly compacts tail and corrupts the ring, these can disappear.
        assertNotNull(tracker.remove(25));  // removal from a non-tail bucket

        // Sanity: nearby entries should still exist
        assertNotNull(tracker.get(24));
        assertNull(tracker.get(25));
        assertNotNull(tracker.get(26));

        // Tail-adjacent entries (8..15) should still exist
        for (int i = 8; i < 16; ++i) {
            assertNotNull(tracker.get(i), "Entry " + i + " missing after middle removal/compaction");
        }
    }

    @Test
    public void testRemoveFromNonTailTriggersWrongCompactionAndStillPreservesCorrectness() {
        final UnsettledMap<DeliveryType> tracker = createMap(6, 10);

        // Fill 3 buckets: 0..29
        for (int i = 0; i < 30; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        // Make tail bucket small (remove 0..6 leaves 7..9 in first bucket => 3 entries == low-water)
        for (int i = 0; i < 7; ++i) {
            assertNotNull(tracker.remove(i));
        }

        // Now remove entries from a *non-tail* bucket to make that bucket sparse too.
        // Removing these should make the target bucket hit <= low-water and trigger the compaction path.
        assertNotNull(tracker.remove(15));
        assertNotNull(tracker.remove(16));
        assertNotNull(tracker.remove(17));

        // If tryCompact(tail) corrupts the map, these will be missing or inconsistent.
        for (int i = 7; i < 10; ++i) {
            assertNotNull(tracker.get(i), "Tail-adjacent entry missing after non-tail removal triggered compaction");
        }
        assertNull(tracker.get(15));
        assertNull(tracker.get(16));
        assertNull(tracker.get(17));
        assertNotNull(tracker.get(18));
    }

    @Test
    public void testIterateOverBucketsThatHaveWrapped() {
        final UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(UnsignedInteger.MAX_VALUE.intValue(), new DeliveryType(UnsignedInteger.MAX_VALUE.intValue()));
        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));

        assertEquals(3, tracker.size());

        Iterator<UnsignedInteger> iter = tracker.keySet().iterator();

        final int[] expected = { UnsignedInteger.MAX_VALUE.intValue(), 0, 1 };

        int count = 0;

        while (iter.hasNext()) {
            assertEquals(expected[count++], iter.next().intValue());
        }

        assertEquals(3, count);
    }

    protected void dumpRandomDataSet(int iterations, long seed, boolean bounded) {
        final int[] dataSet = new int[iterations];

        random.setSeed(seed);

        for (int i = 0; i < iterations; ++i) {
            if (bounded) {
                dataSet[i] = random.nextInt(iterations);
            } else {
                dataSet[i] = random.nextInt();
            }
        }

        LOG.info("Iterations was {}, Random seed was: {}", iterations , seed);
        LOG.info("Entries in data set: {}", dataSet);
    }

    protected static class OutsideEntry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private V value;

        public OutsideEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public V setValue(V value) {
            V oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public K getKey() {
            return key;
        }
    }

    //----- forEach methods test variations

    @Test
    public void testForEachEntry() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            tracker.put(entry, new DeliveryType(entry));
        }

        final SequenceNumber index = new SequenceNumber(0);
        tracker.forEach((value) -> {
            int i = index.getAndIncrement().intValue();
            assertEquals(new DeliveryType(inputValues[i]), value);
        });

        assertEquals(index.intValue(), inputValues.length);
    }

    @Test
    public void testForEachDeliveryIteratesOverLargeSeriesOfDeliveries() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 4080;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        final AtomicInteger index = new AtomicInteger();

        tracker.forEach((delivery) -> index.incrementAndGet());

        assertEquals(index.get(), COUNT);
    }

    @Test
    public void testForEachOnEmptyMap() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final AtomicInteger index = new AtomicInteger();

        tracker.forEach(0, UnsignedInteger.MAX_VALUE.intValue(), (delivery) -> index.incrementAndGet());

        assertEquals(index.get(), 0);
    }

    @Test
    public void testForEachBiConsumerDeliveryIteratesOverLargeSeriesOfDeliveries() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 4080;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        final AtomicInteger index = new AtomicInteger();

        tracker.forEach((deliveryId, delivery) -> index.incrementAndGet());

        assertEquals(index.get(), COUNT);
    }

    @Test
    public void testRangedForEachDeliveryIteratesOverSmallSeriesOfDeliveries() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        final int COUNT = 512;

        for (int i = 0; i < COUNT; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(COUNT, tracker.size());

        final AtomicInteger index = new AtomicInteger();

        tracker.forEach(260, 262, (delivery) -> index.incrementAndGet());

        assertEquals(index.get(), 3);
    }

    @Test
    public void testRangedForEachDeliveryIteratesSeriesWhenValuesOverflowIntRange() {
        UnsettledMap<DeliveryType> tracker = createMap();
        assertEquals(0, tracker.size());

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(Integer.MAX_VALUE, new DeliveryType(Integer.MAX_VALUE));
        tracker.put(Integer.MAX_VALUE + 1, new DeliveryType(Integer.MAX_VALUE + 1));
        tracker.put(Integer.MAX_VALUE + 2, new DeliveryType(Integer.MAX_VALUE + 2));
        tracker.put(Integer.MAX_VALUE + 3, new DeliveryType(Integer.MAX_VALUE + 3));
        tracker.put(Integer.MAX_VALUE + 4, new DeliveryType(Integer.MAX_VALUE + 4));

        final AtomicInteger index = new AtomicInteger();

        tracker.forEach(Integer.MAX_VALUE, Integer.MAX_VALUE + 2, (delivery) -> index.incrementAndGet());

        assertEquals(3, index.get());
    }

    @Test
    public void testForEachWithRangeMuchLargerThanContainedEntries() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = 100;

        map.put(0, new DeliveryType(0));

        for (int i = 0, j = 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        map.put(Integer.MAX_VALUE, new DeliveryType(Integer.MAX_VALUE));

        assertEquals(NUM_ENTRIES + 2, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, Integer.MAX_VALUE, (delivery) -> traversed.incrementAndGet());

        assertEquals(NUM_ENTRIES + 2, traversed.get());
        assertEquals(NUM_ENTRIES + 2, map.size());
    }

    @Test
    public void testForEachCoversElementsInBetweenGivenRangeInOtherBuckets() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);
        final int NUM_ENTRIES = 100;

        for (int i = 0, j = 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        assertEquals(NUM_ENTRIES, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, Integer.MAX_VALUE, (delivery) -> traversed.incrementAndGet());

        assertEquals(NUM_ENTRIES, traversed.get());
        assertEquals(NUM_ENTRIES, map.size());
    }

    @Test
    public void testForEachWhereLastValueNotPresentButGreaterValuesAre() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = 100;

        map.put(0, new DeliveryType(0));

        for (int i = 0, j = 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        map.put(65534, new DeliveryType(65534));
        map.put(65536, new DeliveryType(65536));
        map.put(65537, new DeliveryType(65537));

        map.put(UnsignedInteger.MAX_VALUE.intValue(), new DeliveryType(UnsignedInteger.MAX_VALUE.intValue()));

        assertEquals(NUM_ENTRIES + 5, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, 65535, (delivery) -> traversed.incrementAndGet());

        assertEquals(NUM_ENTRIES + 2, traversed.get());
    }

    @Test
    public void testForEachWhereLastNotPresentAndNextValuesAreOverflow() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = 100;
        final int EXPECTED_ENTRIES = 102;

        map.put(0, new DeliveryType(0));

        for (int i = 0, j = 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        map.put(65534, new DeliveryType(65534));

        map.put(0, new DeliveryType(0));
        map.put(1, new DeliveryType(1));
        map.put(2, new DeliveryType(2));

        assertEquals(NUM_ENTRIES + 5, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, 65535, (delivery) -> traversed.incrementAndGet());

        assertEquals(EXPECTED_ENTRIES, traversed.get());
    }

    @Test
    public void testForEachWithRangeThatWrapped() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, 0, 1, 2, 3, Integer.MAX_VALUE };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, Integer.MAX_VALUE, (delivery) -> traversed.incrementAndGet());

        assertEquals(2, traversed.get());
        assertEquals(entries.length, map.size());

        map.remove(512);
        map.remove(513);

        map.forEach(0, Integer.MAX_VALUE, (delivery) -> traversed.incrementAndGet());

        assertEquals(7, traversed.get());
    }

    @Test
    public void testForEachWithRangeThatWrappedAndStartBeyondMaxInt() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, Integer.MAX_VALUE + 1, UnsignedInteger.MAX_VALUE.intValue() };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(Integer.MAX_VALUE + 1, UnsignedInteger.MAX_VALUE.intValue(), (delivery) -> traversed.incrementAndGet());

        assertEquals(2, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForEachFindsNoValues() {
        final int afterLast = uintArray.length + 1; // Entries are one based

        final AtomicBoolean traversed = new AtomicBoolean();

        tracker.forEach(afterLast, afterLast + 10, (delivery) -> traversed.set(true));

        assertFalse(traversed.get());
    }

    @Test
    public void testForEachRangeFindsOnlyLastValue() {
        final int lastEntry = uintArray.length; // Entries are one based

        final AtomicInteger traversed = new AtomicInteger();

        tracker.forEach(lastEntry, lastEntry, (delivery) -> traversed.incrementAndGet());

        assertEquals(1, traversed.get());
    }

    @Test
    public void testForEachRangedLastValueAndRangeOutsideOfActualEntries() {
        final int lastEntry = uintArray.length; // Entries are one based

        final AtomicInteger traversed = new AtomicInteger();

        tracker.forEach(lastEntry, lastEntry + 10, (delivery) -> traversed.incrementAndGet());

        assertEquals(1, traversed.get());
    }

    @Test
    public void testForEachWithRangeThatMatchesMany() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, 512, 513, 512, 513 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(512, 513, (delivery) -> traversed.incrementAndGet());

        assertEquals(2, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForEachWithRangeThatFallBetweenManyGenerations() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, 512, 513, 512, 513 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(1, 10, (delivery) -> traversed.incrementAndGet());
        map.forEach(600, 65535, (delivery) -> traversed.incrementAndGet());
        map.forEach(510, 511, (delivery) -> traversed.incrementAndGet());

        assertEquals(0, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForEachWithRangeThatFallAtEndOfLastGeneration() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, 512, 513, 512, 513, 65530, 65531, 65535 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> returned = new ArrayList<>();

        map.forEach(65531, 65580, (delivery) -> {
            traversed.incrementAndGet();
            returned.add(delivery.getDeliveryId());
        });

        assertEquals(2, traversed.get());
        assertEquals(entries.length, map.size());

        assertTrue(returned.contains(65531));
        assertTrue(returned.contains(65535));
    }

    @Test
    public void testForEachBoundedToFirstGeneration() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, UnsignedInteger.MAX_VALUE.intValue(), UnsignedInteger.MAX_VALUE.intValue() };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, UnsignedInteger.MAX_VALUE.intValue(), (delivery) -> {
            assertEquals(entries[traversed.getAndIncrement()], delivery.getDeliveryId());
        });

        assertEquals(3, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForEachWhenSpanHasValuePresentInSuccessiveGeneration() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 0, 1, 65535, Integer.MAX_VALUE, UnsignedInteger.MAX_VALUE.intValue(), UnsignedInteger.MAX_VALUE.intValue() };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(0, UnsignedInteger.MAX_VALUE.intValue(), (delivery) -> {
            assertEquals(entries[traversed.getAndIncrement()], delivery.getDeliveryId());
        });

        assertEquals(5, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForEachWhereFirstAndLastAreEqualOnlyReturnsOnce() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { UnsignedInteger.MAX_VALUE.intValue(), 0, 1, UnsignedInteger.MAX_VALUE.intValue() };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(UnsignedInteger.MAX_VALUE.intValue(), UnsignedInteger.MAX_VALUE.intValue(), (delivery) -> traversed.incrementAndGet());

        assertEquals(1, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForeachWhenRangeCarriesOverflow() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 65534, 65535, 10, 11, 15, 90 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> returned = new ArrayList<>();

        map.forEach(65534, 15, (delivery) -> {
            traversed.incrementAndGet();
            returned.add(delivery.getDeliveryId());
        });

        assertEquals(5, traversed.get());
        assertEquals(entries.length, map.size());

        assertTrue(returned.contains(65534));
        assertTrue(returned.contains(65535));
        assertTrue(returned.contains(10));
        assertTrue(returned.contains(11));
        assertTrue(returned.contains(15));
    }

    @Test
    public void testForeachWhenRangeCarriesOverflowButStopsBeforeFirstValueInNextGeneration() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 65534, 65535, 10, 11, 15, 90 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> returned = new ArrayList<>();

        map.forEach(65534, 9, (delivery) -> {
            traversed.incrementAndGet();
            returned.add(delivery.getDeliveryId());
        });

        assertEquals(2, traversed.get());
        assertEquals(entries.length, map.size());

        assertTrue(returned.contains(65534));
        assertTrue(returned.contains(65535));
    }

    @Test
    public void testForeachWhenRangeCarriesOverflowButStopsBeforeFirstValueInGenerationBeyondNextValue() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 65534, 65535, 10, 10, 11, 15, 90 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> returned = new ArrayList<>();

        assertNotNull(map.remove(10));

        map.forEach(65534, 11, (delivery) -> {
            traversed.incrementAndGet();
            returned.add(delivery.getDeliveryId());
        });

        assertEquals(4, traversed.get());
        assertEquals(entries.length - 1, map.size());

        assertTrue(returned.contains(65534));
        assertTrue(returned.contains(65535));
        assertTrue(returned.contains(10));
        assertTrue(returned.contains(11));
    }

    @Test
    public void testForeachWhenRangeCarriesOverflowNoOverflowValuesInMap() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 65534, 65535 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> returned = new ArrayList<>();

        map.forEach(65534, 255, (delivery) -> {
            traversed.incrementAndGet();
            returned.add(delivery.getDeliveryId());
        });

        assertEquals(2, traversed.get());
        assertEquals(entries.length, map.size());

        assertTrue(returned.contains(65534));
        assertTrue(returned.contains(65535));
    }

    @Test
    public void testForEachInBucketThatHasNoMatchingSequence() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 65534, 65535, 65550, 65551 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversed = new AtomicInteger();

        map.forEach(65538, 65549, (delivery) -> {
            traversed.incrementAndGet();
        });

        assertEquals(0, traversed.get());
        assertEquals(entries.length, map.size());
    }

    @Test
    public void testForEachWithRangeThatWrappedAndNonZeroReadOffset() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        // Populate items in high range and lower wrapped range
        final int[] entries = new int[] { -4, -3, -2, -1, 0, 1, 2, 3, 4, 5 };

        for (int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger traversedCount = new AtomicInteger();
        final List<Integer> traversed = new ArrayList<>();

        // Starts mid-bucket in the upper segment and ends mid-bucket in the lower segment.
        map.forEach(-2, 2, (delivery) -> {
            traversedCount.incrementAndGet();
            traversed.add(delivery.getDeliveryId());
        });

        assertEquals(5, traversedCount.get());
        assertEquals(entries.length, map.size());

        assertTrue(traversed.contains(-2));
        assertTrue(traversed.contains(-1));
        assertTrue(traversed.contains(0));
        assertTrue(traversed.contains(1));
        assertTrue(traversed.contains(2));
    }

    @Test
    public void testForEachAcrossMultipleGenerations() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        // Add sequence that spans two full generations of unsigned integer wrap
        map.put(-2, new DeliveryType(-2)); // Gen 0
        map.put(-1, new DeliveryType(-1)); // Gen 0
        map.put(0, new DeliveryType(0));   // Gen 1 (overflow)
        map.put(1, new DeliveryType(1));   // Gen 1
        map.put(2, new DeliveryType(2));   // Gen 1

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> visited = new ArrayList<>();

        // Iterate over wrapped range
        map.forEach(-2, 1, (delivery) -> {
            traversed.incrementAndGet();
            visited.add(delivery.getDeliveryId());
        });

        assertEquals(4, traversed.get());
        assertEquals(Arrays.asList(-2, -1, 0, 1), visited);
    }

    @Test
    public void testForEachRangedContinuationShortCircuitsWhenBucketLowExceedsLast() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        // Gen 0: High IDs
        map.put(-2, new DeliveryType(-2)); // 0xFFFFFFFE
        map.put(-1, new DeliveryType(-1)); // 0xFFFFFFFF

        // Gen 1: Low IDs that start well past the second bound 'last' (1)
        map.put(10, new DeliveryType(10));
        map.put(11, new DeliveryType(11));

        final AtomicInteger traversed = new AtomicInteger();

        // Search range -2 to 1 (wrapped). Gen 0 has -2, -1. Gen 1 starts at 10 (> 1).
        map.forEach(-2, 1, d -> traversed.incrementAndGet());

        assertEquals(2, traversed.get());
    }

    @Test
    public void testForEachRangedContinuationBucketsExceedRangeOfLast() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        // Gen 0
        map.put(-2, new DeliveryType(-2));
        map.put(-1, new DeliveryType(-1));

        // Gen 1
        map.put(10, new DeliveryType(10));
        map.put(11, new DeliveryType(11));

        final AtomicInteger traversed = new AtomicInteger();
        final List<Integer> visited = new ArrayList<>();

        map.forEach(-2, 1, (delivery) -> {
            traversed.incrementAndGet();
            visited.add(delivery.getDeliveryId());
        });

        assertEquals(2, traversed.get());
        assertEquals(4, map.size());

        assertTrue(visited.contains(-2));
        assertTrue(visited.contains(-1));
    }

    @Test
    public void testForEachAndRemoveEachCoverOneSeriesOfDeliveriesAtATime() {
        final UnsettledMap<DeliveryType> map = createMap(4, 8);

        // Deliveries of a first series of IDs, the next two IDs roll the series backwards which
        // begins a second series of deliveries that is tracked alongside the first one.
        map.put(100, new DeliveryType(100));
        map.put(101, new DeliveryType(101));
        map.put(102, new DeliveryType(102));
        map.put(50, new DeliveryType(50));
        map.put(51, new DeliveryType(51));

        assertEquals(5, map.size(), "Every delivery added should be tracked");
        assertAllTrackedDeliveriesAreReachable(map);

        final List<Integer> visited = new ArrayList<>();
        map.forEach(50, 200, (delivery) -> visited.add(delivery.getDeliveryId()));

        // A ranged visit covers a single series of deliveries at a time so only the oldest series
        // is visited even though the range also holds the deliveries of the newer series.
        assertEquals(Arrays.asList(100, 101, 102), visited,
            "The ranged visit should cover the deliveries of the first series: " + visited);
        assertEquals(5, map.size(), "forEach should not alter the tracker contents");
        assertAllTrackedDeliveriesAreReachable(map);

        // A range that only holds the deliveries of the newer series covers them regardless of the
        // series that precedes them in the tracker.
        final List<Integer> newerSeries = new ArrayList<>();
        map.forEach(50, 60, (delivery) -> newerSeries.add(delivery.getDeliveryId()));

        assertEquals(Arrays.asList(50, 51), newerSeries,
            "The range of the newer series should cover its deliveries: " + newerSeries);
        assertEquals(5, map.size(), "forEach should not alter the tracker contents");

        final List<Integer> removed = new ArrayList<>();
        map.removeEach(50, 200, (delivery) -> removed.add(delivery.getDeliveryId()));

        assertEquals(Arrays.asList(100, 101, 102), removed,
            "The ranged removal should cover the same deliveries as the ranged visit: " + removed);
        assertEquals(2, map.size(), "Only the deliveries of the covered series should have been removed");
        assertNull(map.get(100), "Delivery within the range should have been removed");
        assertNotNull(map.get(50), "Delivery of the other series should still be tracked");
        assertAllTrackedDeliveriesAreReachable(map);

        // The newer series remains tracked and is covered by the next pass over the same range.
        final List<Integer> continued = new ArrayList<>();
        map.forEach(50, 200, (delivery) -> continued.add(delivery.getDeliveryId()));

        assertEquals(Arrays.asList(50, 51), continued,
            "The next pass over the range should cover the remaining series: " + continued);
        assertEquals(2, map.size(), "forEach should not alter the tracker contents");
        assertAllTrackedDeliveriesAreReachable(map);
    }

    //----- removeEach method test variations

    @Test
    public void testRemoveRangeRemovesNoValues() {
        final int afterLast = uintArray.length + 1; // Entries are one based

        final AtomicBoolean removed = new AtomicBoolean();

        tracker.removeEach(afterLast, afterLast + 10, (delivery) -> removed.set(true));

        assertFalse(removed.get());
    }

    @Test
    public void testRemoveRangeRemovesLastValue() {
        final int lastEntry = uintArray.length; // Entries are one based

        final AtomicInteger removed = new AtomicInteger();

        tracker.removeEach(lastEntry, lastEntry, (delivery) -> removed.incrementAndGet());

        assertEquals(1, removed.get());
    }

    @Test
    public void testRemoveRangeRemovesLastValueAndRangeOutsideOfActualEntries() {
        final int lastEntry = uintArray.length; // Entries are one based

        final AtomicInteger removed = new AtomicInteger();

        tracker.removeEach(lastEntry, lastEntry + 10, (delivery) -> removed.incrementAndGet());

        assertEquals(1, removed.get());
    }

    @Test
    public void testRemoveEachWithRangeThatMatchesMany() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int[] entries = new int[] { 512, 513, 512, 513, 512, 513 };

        for(int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(512, 513, (delivery) -> removed.incrementAndGet());

        assertEquals(2, removed.get());
        assertEquals(entries.length - 2, map.size());
    }

    @Test
    public void testRemoveAllEntriesFromFirstBucket() {
        doTestRemoveEach(0, 15);
    }

    @Test
    public void testRemoveAllEntriesFromMiddleBucket() {
        doTestRemoveEach(16, 31);
    }

    @Test
    public void testRemoveAllEntriesFromEndBucket() {
        doTestRemoveEach(32, 47);
    }

    @Test
    public void testRemoveEntriesSpanningThreeBuckets() {
        doTestRemoveEach(8, 39);
    }

    @Test
    public void testRemoveAllEntriesWithClosedRange() {
        doTestRemoveEach(0, 47);
    }

    @Test
    public void testRemoveAllEntriesWithOpenRange() {
        doTestRemoveEach(0, 64);
    }

    public void doTestRemoveEach(int start, int end) {
        final int numBuckets = 3;
        final int bucketSize = 16;
        final int numEntries = numBuckets * bucketSize;
        final int numRemoved = Math.min(end - start + 1, numEntries);

        UnsettledMap<DeliveryType> map = createMap(numBuckets, bucketSize);

        for (int i = 0; i < numEntries; ++i) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(numEntries, map.size());

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(start, end, (delivery) -> removed.incrementAndGet());

        assertEquals(numRemoved, removed.get());
        assertEquals(numEntries - numRemoved, map.size());
    }

    @Test
    public void testRemoveEachWithRangeMuchLargerThanContainedEntries() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = 100;

        map.put(0, new DeliveryType(0));

        for (int i = 0, j = 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        map.put(Integer.MAX_VALUE, new DeliveryType(Integer.MAX_VALUE));

        assertEquals(NUM_ENTRIES + 2, map.size());

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(0, Integer.MAX_VALUE, (delivery) -> removed.incrementAndGet());

        assertEquals(NUM_ENTRIES + 2, removed.get());
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveEachWithRangeMuchLargerThanContainedEntriesInRangeAboveMaxInt() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = 100;

        map.put(Integer.MAX_VALUE, new DeliveryType(Integer.MAX_VALUE));

        for (int i = 0, j = Integer.MAX_VALUE + 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        map.put(UnsignedInteger.MAX_VALUE.intValue(), new DeliveryType(UnsignedInteger.MAX_VALUE.intValue()));

        assertEquals(NUM_ENTRIES + 2, map.size());

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(Integer.MAX_VALUE, UnsignedInteger.MAX_VALUE.intValue() - 1, (delivery) -> removed.incrementAndGet());

        assertEquals(NUM_ENTRIES + 1, removed.get());
        assertEquals(1, map.size());
    }

    @Test
    public void testRemoveEachWhereLastNotPresentAndNextValuesAreOverflow() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = 100;

        map.put(0, new DeliveryType(0));

        for (int i = 0, j = 512; i < NUM_ENTRIES; ++i, j += 25) {
            map.put(j, new DeliveryType(j));
        }

        map.put(65534, new DeliveryType(65534));

        map.put(0, new DeliveryType(0));
        map.put(1, new DeliveryType(1));
        map.put(2, new DeliveryType(2));

        assertEquals(NUM_ENTRIES + 5, map.size());

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(0, 65535, (delivery) -> removed.incrementAndGet());

        assertEquals(NUM_ENTRIES + 2, removed.get());
        assertEquals(3, map.size());
    }

    @Test
    public void testRemoveEachEntireDeliveryIdRangeTwoBuckets() {
        final UnsettledMap<DeliveryType> map = createMap(3, 128);
        final int NUM_ENTRIES = UnsignedByte.MAX_VALUE.intValue();

        for (int i = 0; i < NUM_ENTRIES; ++i) {
            map.put(i, new DeliveryType(i));
        }

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(0, NUM_ENTRIES, (delivery) -> removed.incrementAndGet());

        assertEquals(NUM_ENTRIES, removed.get());
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveEachEntireDeliveryIdRange() {
        final UnsettledMap<DeliveryType> map = createMap();
        final int NUM_ENTRIES = UnsignedShort.MAX_VALUE.intValue();

        for (int i = 0; i < NUM_ENTRIES; ++i) {
            map.put(i, new DeliveryType(i));
        }

        final AtomicInteger removed = new AtomicInteger();

        map.removeEach(0, NUM_ENTRIES, (delivery) -> removed.incrementAndGet());

        assertEquals(NUM_ENTRIES, removed.get());
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveAllEntriesInSmallChunks() {
        final AtomicInteger removed = new AtomicInteger();

        for (int i = 0; i < uintArray.length; i += 2) {
            tracker.removeEach(uintArray[i].intValue(), uintArray[i+1].intValue(), (delivery) -> removed.incrementAndGet());
        }

        assertEquals(uintArray.length, removed.get());
    }

    @Test
    public void testRemoveEachWithNonZeroReadOffset() {
        UnsettledMap<DeliveryType> tracker = createMap(2, 16);

        for (int i = 0; i < 32; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        for (int i = 0; i < 5; i++) {
            tracker.remove(i);
        }

        tracker.removeEach(5, 20, d -> {});

        for (int i = 5; i <= 20; i++) {
            assertFalse(tracker.containsKey(i));
        }
    }

    @Test
    public void testRemoveEachClearsThenReuseMap() {
        UnsettledMap<DeliveryType> tracker = createMap();

        for (int i = 0; i < 100; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        tracker.removeEach(0, 200, d -> {});

        assertTrue(tracker.isEmpty());

        tracker.put(999, new DeliveryType(999));

        assertEquals(1, tracker.size());
        assertNotNull(tracker.get(999));
    }

    @Test
    public void testRecycleBucketWhenTailHasWrapped() {
        final UnsettledMap<DeliveryType> tracker = createMap(5, 2);

        // Fill all 5 buckets: bucket0=[0,1], bucket1=[2,3], bucket2=[4,5], bucket3=[6,7], bucket4=[8,9]
        for (int i = 0; i < 10; ++i) {
            tracker.put(i, new DeliveryType(i));
        }
        assertEquals(10, tracker.size());

        // Remove 0..5 => drains bucket0, bucket1, bucket2 fully (advances tail to bucket3)
        for (int i = 0; i < 6; ++i) {
            assertNotNull(tracker.remove(i), "Expected to remove existing id: " + i);
        }
        assertEquals(4, tracker.size()); // remaining: 6,7,8,9

        // Add 10..13:
        // bucket4 is full so putting 10 advances head 4->0 (wrap) and uses bucket0 again
        // then 12 advances head 0->1 and uses bucket1 again
        for (int i = 10; i < 14; ++i) {
            tracker.put(i, new DeliveryType(i));
        }
        assertEquals(8, tracker.size()); // now: 6,7,8,9,10,11,12,13

        // Now remove entries that occupy bucket index 0 (10,11) fully.
        // This should recycle bucket index 0 while tail is at 3 and head at 1:
        // tail > head and index(0) < tail(3) => final else branch in recycleBucket.
        tracker.removeEach(10, 11, d -> {});

        assertEquals(6, tracker.size());
        assertFalse(tracker.containsKey(10));
        assertFalse(tracker.containsKey(11));
        assertTrue(tracker.containsKey(12));
        assertTrue(tracker.containsKey(13));

        // Validate map remains consistent and all remaining values are removable.
        final int[] remaining = { 6, 7, 8, 9, 12, 13 };

        for (int id : remaining) {
            DeliveryType removed = tracker.remove(id);
            assertNotNull(removed, "Expected to remove existing id: " + id);
            assertEquals(id, removed.getDeliveryId());
        }

        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testRecycleHeadBucketDoesNotLoseEarlierEntries() {
        final UnsettledMap<DeliveryType> tracker = createMap(8, 4);  // 8 buckets of 4 entries each

        // Fill exactly 3 buckets: [0..11]. Head should be at the 3rd bucket.
        for (int i = 0; i < 12; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(12, tracker.size());

        // Remove the last bucket's range [8..11] which should fully drain the head bucket,
        // forcing recycleBucket(head).
        tracker.removeEach(8, 11, d -> { });

        // Now [0..7] must still exist
        assertEquals(8, tracker.size());

        for (int i = 0; i < 8; ++i) {
            assertNotNull(tracker.get(i), "Missing entry " + i + " after recycling head bucket");
        }
        for (int i = 8; i < 12; ++i) {
            assertNull(tracker.get(i), "Entry " + i + " should have been removed");
        }

        // Ensure map still accepts new writes after head recycling.
        tracker.put(12, new DeliveryType(12));
        assertNotNull(tracker.get(12));
        assertEquals(9, tracker.size());
    }

    @Test
    public void testRecycleBucketInWrappedSpanDoesNotCorruptMap() {
        // 5 buckets of size 2 => easy to force wrap.
        final UnsettledMap<DeliveryType> tracker = createMap(5, 2);

        // Fill all 5 buckets: IDs 0..9
        for (int i = 0; i < 10; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        // Drain 0..5 => recycles buckets at the front, tail advances forward
        for (int i = 0; i < 6; ++i) {
            assertNotNull(tracker.remove(i));
        }

        // Add 10..13 => forces head wrap-around into earlier indices
        for (int i = 10; i < 14; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        // Now remove a bucket that is *not* tail and not head, but sits in the wrapped portion.
        // This should recycle an internal bucket and still keep all remaining IDs accessible.
        tracker.removeEach(10, 11, d -> { });

        assertNull(tracker.get(10));
        assertNull(tracker.get(11));
        assertNotNull(tracker.get(12));
        assertNotNull(tracker.get(13));

        // Verify remaining removals are consistent and no entries were lost
        final int[] remaining = { 6, 7, 8, 9, 12, 13 };

        for (int id : remaining) {
            assertNotNull(tracker.remove(id), "Expected to remove remaining id: " + id);
        }

        assertTrue(tracker.isEmpty());
    }

    @Test
    public void testRecycleHeadWhenTailNotZeroDoesNotCorruptSpan() {
        final UnsettledMap<DeliveryType> tracker = createMap(8, 4);

        // Fill 3 buckets worth: 0..11
        for (int i = 0; i < 12; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        // Drain first bucket completely (0..3) so tail advances away from 0.
        for (int i = 0; i < 4; ++i) {
            assertNotNull(tracker.remove(i));
        }

        // Now map contains 4..11; tail should no longer be 0 internally.

        // Remove the last bucket worth (8..11) to fully empty the head bucket and force
        // recycleBucket(head).
        tracker.removeEach(8, 11, d -> {});

        // Remaining should be 4..7 exactly
        assertEquals(4, tracker.size());

        for (int i = 4; i < 8; ++i) {
            assertNotNull(tracker.get(i), "Missing " + i + " after head recycle with tail != 0");
        }
        for (int i = 8; i < 12; ++i) {
            assertNull(tracker.get(i), "Expected removed " + i);
        }

        // Continue using the map to ensure head/tail pointers are still consistent.
        tracker.put(12, new DeliveryType(12));
        tracker.put(13, new DeliveryType(13));
        assertNotNull(tracker.get(12));
        assertNotNull(tracker.get(13));
    }

    @Test
    public void testRemoveEachSpanningMultipleBucketRecyclesDoesNotSkipBuckets() {
        final UnsettledMap<DeliveryType> tracker = createMap(10, 4);

        // Fill 7 buckets worth => 28 entries: 0..27
        for (int i = 0; i < 28; ++i) {
            tracker.put(i, new DeliveryType(i));
        }

        // Remove a span that starts mid-bucket and ends exactly at a bucket boundary.
        // Buckets: [0..3],[4..7],[8..11],[12..15],[16..19],[20..23],[24..27]
        // Remove 2..19 covers partial first bucket + full next four buckets.
        tracker.removeEach(2, 19, d -> {});

        // Keys 2..19 must be gone
        for (int i = 2; i <= 19; ++i) {
            assertNull(tracker.get(i), "Expected removed " + i);
        }

        // Keys outside range must remain
        for (int i = 0; i < 2; ++i) {
            assertNotNull(tracker.get(i), "Unexpectedly missing " + i);
        }
        for (int i = 20; i < 28; ++i) {
            assertNotNull(tracker.get(i), "Unexpectedly missing " + i);
        }
    }

    @Test
    public void testRemoveEachWithRangeThatWrappedAndNonZeroReadOffset() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        // Populate items in high range and lower wrapped range
        final int[] entries = new int[] { -4, -3, -2, -1, 0, 1, 2, 3, 4, 5 };

        for (int i : entries) {
            map.put(i, new DeliveryType(i));
        }

        assertEquals(entries.length, map.size());

        final AtomicInteger removed = new AtomicInteger();
        final List<Integer> removedIds = new ArrayList<>();

        // Starts mid-bucket in the upper segment and ends mid-bucket in the lower segment.
        map.removeEach(-2, 2, (delivery) -> {
            removed.incrementAndGet();
            removedIds.add(delivery.getDeliveryId());
        });

        assertEquals(5, removed.get());
        assertEquals(entries.length - 5, map.size());

        assertTrue(removedIds.contains(-2));
        assertTrue(removedIds.contains(-1));
        assertTrue(removedIds.contains(0));
        assertTrue(removedIds.contains(1));
        assertTrue(removedIds.contains(2));

        // Ensure boundary items remain intact
        assertNotNull(map.get(-4));
        assertNotNull(map.get(-3));
        assertNotNull(map.get(3));
        assertNotNull(map.get(4));
        assertNotNull(map.get(5));
    }

    @Test
    public void testRemoveEachAcrossMultipleGenerations() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        map.put(-2, new DeliveryType(-2)); // Gen 0
        map.put(-1, new DeliveryType(-1)); // Gen 0
        map.put(0, new DeliveryType(0));   // Gen 1
        map.put(1, new DeliveryType(1));   // Gen 1
        map.put(2, new DeliveryType(2));   // Gen 1

        assertEquals(5, map.size());

        final AtomicInteger removed = new AtomicInteger();

        // Remove range wrapping across boundary
        map.removeEach(-2, 1, (delivery) -> removed.incrementAndGet());

        assertEquals(4, removed.get());
        assertEquals(1, map.size());

        assertNull(map.get(-2));
        assertNull(map.get(-1));
        assertNull(map.get(0));
        assertNull(map.get(1));
        assertNotNull(map.get(2));
    }

    @Test
    public void testRemoveEachRangedContinuationBucketsExceedRangeOfLast() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        // Gen 0
        map.put(-2, new DeliveryType(-2));
        map.put(-1, new DeliveryType(-1));

        // Gen 1
        map.put(10, new DeliveryType(10));
        map.put(11, new DeliveryType(11));

        final AtomicInteger removed = new AtomicInteger();

        // Remove range -2 to 1 (wrapped). Should remove -2 and -1, then short-circuit when seeing 10.
        map.removeEach(-2, 1, d -> removed.incrementAndGet());

        assertEquals(2, removed.get());
        assertEquals(2, map.size());
        assertNull(map.get(-2));
        assertNull(map.get(-1));
        assertNotNull(map.get(10));
        assertNotNull(map.get(11));
    }

    @Test
    public void testRangedOperationsTraverseOverEmptyIntermediateBuckets() {
        final UnsettledMap<DeliveryType> map = createMap(5, 4);

        // Fill 4 buckets
        for (int i = 0; i < 16; i++) {
            map.put(i, new DeliveryType(i));
        }

        // Drain the middle two buckets completely using single removals
        for (int i = 4; i < 12; i++) {
            assertNotNull(map.remove(i));
        }

        final AtomicInteger forEachCount = new AtomicInteger();
        map.forEach(0, 15, d -> forEachCount.incrementAndGet());
        assertEquals(8, forEachCount.get()); // Only 0..3 and 12..15 remain

        final AtomicInteger removeEachCount = new AtomicInteger();
        map.removeEach(0, 15, d -> removeEachCount.incrementAndGet());
        assertEquals(8, removeEachCount.get());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRangedForEachWithDuplicateKeysAcrossGenerations() {
        final UnsettledMap<DeliveryType> map = createMap(5, 4);

        // Gen 0: ID 5
        map.put(5, new DeliveryType(5));
        map.put(UnsignedInteger.MAX_VALUE.intValue(), new DeliveryType(UnsignedInteger.MAX_VALUE.intValue()));

        // Gen 1: Overflow and re-insert ID 5
        map.put(5, new DeliveryType(5));

        final AtomicInteger count = new AtomicInteger();

        // Ranged search bounded to Gen 0 range
        map.forEach(0, UnsignedInteger.MAX_VALUE.intValue(), d -> count.incrementAndGet());

        // Should stop at end of Gen 0 and not bleed into Gen 1
        assertEquals(2, count.get());
    }

    @Test
    public void testIteratorRemoveEveryOtherEntryFromExactlyFilledBuckets() {
        final UnsettledMap<DeliveryType> map = createMap(2, 2); // Two buckets of two entries

        for (int i = 0; i < 4; i++) {
            map.put(i, new DeliveryType(i));
        }

        // The head bucket is exactly full and the removed entry is the last entry of that
        // bucket, which forces the remaining entries to be compacted into the previous bucket.
        final Iterator<DeliveryType> iterator = map.values().iterator();
        int visited = 0;
        while (iterator.hasNext()) {
            iterator.next();
            visited++;
            if (visited % 2 == 0) {
                iterator.remove();
            }
        }

        assertEquals(4, visited, "Iteration should visit all four entries");
        assertEquals(2, map.size(), "Half of the deliveries should have been removed");
        assertNotNull(map.get(0), "Even numbered deliveries should still be tracked");
        assertNull(map.get(1), "Odd numbered deliveries should have been removed");
        assertNotNull(map.get(2), "Even numbered deliveries should still be tracked");
        assertNull(map.get(3), "Odd numbered deliveries should have been removed");
    }

    @Test
    public void testIteratorRemoveEveryOtherEntryFromDefaultSizedMap() {
        final UnsettledMap<DeliveryType> map = createMap(); // Default bucket size of 256

        // Two buckets that are both exactly full when the iteration begins.
        for (int i = 0; i < 512; i++) {
            map.put(i, new DeliveryType(i));
        }

        final Iterator<DeliveryType> iterator = map.values().iterator();
        int visited = 0;
        while (iterator.hasNext()) {
            iterator.next();
            visited++;
            if (visited % 2 == 0) {
                iterator.remove();
            }
        }

        assertEquals(512, visited, "Iteration should visit all of the added deliveries");
        assertEquals(256, map.size(), "Half of the deliveries should have been removed");

        for (int i = 0; i < 512; i++) {
            if (i % 2 == 0) {
                assertNotNull(map.get(i), "Even delivery " + i + " should still be tracked");
            } else {
                assertNull(map.get(i), "Odd delivery " + i + " should have been removed");
            }
        }
    }

    @Test
    public void testKeySetIteratorRemoveEveryOtherEntryFromDefaultSizedMap() {
        final UnsettledMap<DeliveryType> map = createMap(); // Default bucket size of 256

        for (int i = 0; i < 512; i++) {
            map.put(i, new DeliveryType(i));
        }

        final Iterator<UnsignedInteger> iterator = map.keySet().iterator();
        int visited = 0;
        while (iterator.hasNext()) {
            iterator.next();
            visited++;
            if (visited % 2 == 0) {
                iterator.remove();
            }
        }

        assertEquals(512, visited, "Iteration should visit all of the added keys");
        assertEquals(256, map.size(), "Half of the deliveries should have been removed");
    }

    @Test
    public void testEntrySetIteratorRemoveEveryOtherEntryFromDefaultSizedMap() {
        final UnsettledMap<DeliveryType> map = createMap(); // Default bucket size of 256

        for (int i = 0; i < 512; i++) {
            map.put(i, new DeliveryType(i));
        }

        final Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = map.entrySet().iterator();
        int visited = 0;
        while (iterator.hasNext()) {
            iterator.next();
            visited++;
            if (visited % 2 == 0) {
                iterator.remove();
            }
        }

        assertEquals(512, visited, "Iteration should visit all of the added entries");
        assertEquals(256, map.size(), "Half of the deliveries should have been removed");
    }

    @Test
    public void testRangedForEachWhenWrappedRangeStartsAtTheHighestPreWrapDeliveryId() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int PRE_WRAP_LAST_DELIVERY_ID = UnsignedInteger.MAX_VALUE.intValue() - 15;
        final int PRE_WRAP_FIRST_DELIVERY_ID = PRE_WRAP_LAST_DELIVERY_ID - 4;

        // Same contents as the tests above, built here so the two generation layout is visible.
        for (int i = PRE_WRAP_FIRST_DELIVERY_ID; i <= PRE_WRAP_LAST_DELIVERY_ID; i++) {
            map.put(i, new DeliveryType(i));
        }
        for (int i = 0; i < 11; i++) {
            map.put(i, new DeliveryType(i));
        }

        assertNotNull(map.get(PRE_WRAP_LAST_DELIVERY_ID), "Delivery before the overflow should be tracked");
        assertNotNull(map.get(10), "Delivery following the overflow should be tracked");

        // Counterpart of the first test above: when the start of the range is covered by the
        // deliveries that preceded the overflow, the visit does continue into the deliveries that
        // follow the overflow.
        final List<Integer> visited = new ArrayList<>();
        map.forEach(PRE_WRAP_LAST_DELIVERY_ID, 5, (delivery) -> visited.add(delivery.getDeliveryId()));

        assertEquals(7, visited.size(), "Ranged visit should cover both sides of the overflow: " + visited);
        assertTrue(visited.contains(PRE_WRAP_LAST_DELIVERY_ID),
            "The delivery that the range starts at should be visited: " + visited);
        for (int i = 0; i <= 5; i++) {
            assertTrue(visited.contains(i), "Delivery " + i + " is within the range and should be visited: " + visited);
        }
        assertEquals(16, map.size(), "forEach should not alter the tracker contents");
    }

    @Test
    public void testEqualsWithHashMapKeyedOnUnsignedInteger() {
        final UnsettledMap<DeliveryType> unsettled = createMap();
        final Map<UnsignedInteger, DeliveryType> hashMap = new HashMap<>();

        for (int i = 1; i <= 5; i++) {
            unsettled.put(i, new DeliveryType(i));
            hashMap.put(UnsignedInteger.valueOf(i), new DeliveryType(i));
        }

        assertEquals(hashMap.size(), unsettled.size(), "Maps should hold the same number of entries");
        assertTrue(unsettled.equals(hashMap), "UnsettledMap should equal a HashMap holding the same mappings");
        assertTrue(hashMap.equals(unsettled), "Map equality should be symmetric for a HashMap");
    }

    @Test
    public void testEqualsWithTreeMapKeyedOnUnsignedInteger() {
        final UnsettledMap<DeliveryType> unsettled = createMap();
        final Map<UnsignedInteger, DeliveryType> treeMap = new TreeMap<>();

        for (int i = 1; i <= 5; i++) {
            unsettled.put(i, new DeliveryType(i));
            treeMap.put(UnsignedInteger.valueOf(i), new DeliveryType(i));
        }

        assertEquals(treeMap.size(), unsettled.size(), "Maps should hold the same number of entries");
        assertTrue(unsettled.equals(treeMap), "UnsettledMap should equal a TreeMap holding the same mappings");
        assertTrue(treeMap.equals(unsettled), "Map equality should be symmetric for a TreeMap");
    }

    @Test
    public void testImmutableMapEntryEquals() {
        final UnsettledMap<DeliveryType> first = createMap();
        final UnsettledMap<DeliveryType> second = createMap();

        first.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        second.put(UnsignedInteger.valueOf(1), new DeliveryType(1));

        final Entry<UnsignedInteger, DeliveryType> firstEntry = first.entrySet().iterator().next();
        final Entry<UnsignedInteger, DeliveryType> secondEntry = second.entrySet().iterator().next();

        assertEquals(firstEntry.getKey(), secondEntry.getKey(), "Entries should hold the same key");
        assertEquals(firstEntry.getValue(), secondEntry.getValue(), "Entries should hold the same value");

        assertTrue(firstEntry.equals(secondEntry),
            "Map.Entry requires value based equality, the same mapping must compare equal");
        assertTrue(secondEntry.equals(firstEntry),
            "Map.Entry equality must be symmetric for two equal mappings");
        assertFalse(firstEntry.equals(Map.entry(UnsignedInteger.valueOf(1), new DeliveryType(2))),
            "Map.Entry instances with different values must not compare equal");
    }

    @Test
    public void testImmutableMapEntryHashCode() {
        final UnsettledMap<DeliveryType> first = createMap();
        final UnsettledMap<DeliveryType> second = createMap();

        first.put(UnsignedInteger.valueOf(1), new DeliveryType(1));
        second.put(UnsignedInteger.valueOf(1), new DeliveryType(1));

        final Entry<UnsignedInteger, DeliveryType> firstEntry = first.entrySet().iterator().next();
        final Entry<UnsignedInteger, DeliveryType> secondEntry = second.entrySet().iterator().next();

        assertEquals(firstEntry.hashCode(), secondEntry.hashCode(),
            "Equal Map.Entry instances must have the same hash code");
        assertEquals(firstEntry.hashCode(),
            Map.entry(UnsignedInteger.valueOf(1), new DeliveryType(1)).hashCode(),
            "Map.Entry hash codes must be based on the key and value, not the instance identity");
    }

    @Test
    public void testEntrySetContainsMatchesKeyAndValue() {
        final UnsettledMap<DeliveryType> map = createMap();
        map.put(UnsignedInteger.valueOf(1), new DeliveryType(1));

        assertTrue(map.entrySet().contains(Map.entry(UnsignedInteger.valueOf(1), new DeliveryType(1))),
            "entrySet should contain an entry with the same key and value");
        assertFalse(map.entrySet().contains(Map.entry(UnsignedInteger.valueOf(1), new DeliveryType(2))),
            "entrySet should not contain an entry that has the same key but a different value");
    }

    @Test
    public void testForEachDeliveryFailsWhenEntryRemovedByAction() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        for (int i = 0; i < 25; i++) {
            map.put(i, new DeliveryType(i));
        }

        // The visited delivery is a stand in for any action that settles another delivery while
        // the tracker is being traversed, which restructures the buckets underneath the traversal.
        assertThrows(ConcurrentModificationException.class, () -> map.forEach(delivery -> {
            if (delivery != null && delivery.getDeliveryId() == 5) {
                map.remove(6);
            }
        }), "Removing from the tracker while forEach visits it should fail fast as the iterators do");
    }

    @Test
    public void testForEachEntryFailsWhenEntryRemovedByAction() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        for (int i = 0; i < 25; i++) {
            map.put(i, new DeliveryType(i));
        }

        assertThrows(ConcurrentModificationException.class, () -> map.forEach((deliveryId, delivery) -> {
            if (deliveryId.intValue() == 5) {
                map.remove(6);
            }
        }), "Removing from the tracker while forEach visits it should fail fast as the iterators do");
    }

    @Test
    public void testRangedForEachFailsWhenEntryRemovedByAction() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        for (int i = 0; i < 25; i++) {
            map.put(i, new DeliveryType(i));
        }

        assertThrows(ConcurrentModificationException.class, () -> map.forEach(0, 24, delivery -> {
            if (delivery != null && delivery.getDeliveryId() == 5) {
                map.remove(6);
            }
        }), "Removing from the tracker while a ranged forEach visits it should fail fast as the iterators do");
    }

    @Test
    public void testRemoveEachFailsWhenEntryRemovedByAction() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        for (int i = 0; i < 25; i++) {
            map.put(i, new DeliveryType(i));
        }

        // The ranged removal compacts the buckets once the action has run for the entries it
        // covers, so a removal by the action itself leaves that compaction working on stale state.
        assertThrows(ConcurrentModificationException.class, () -> map.removeEach(0, 24, delivery -> {
            if (delivery != null && delivery.getDeliveryId() == 5) {
                map.remove(6);
            }
        }), "Removing from the tracker while removeEach visits it should fail fast as the iterators do");
    }

    @Test
    public void testForEachDeliveryFailsWhenEntryAddedByAction() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        for (int i = 0; i < 25; i++) {
            map.put(i, new DeliveryType(i));
        }

        final AtomicBoolean added = new AtomicBoolean();

        // Only one delivery is added so that the traversal cannot run away when no error is raised.
        assertThrows(ConcurrentModificationException.class, () -> map.forEach(delivery -> {
            if (added.compareAndSet(false, true)) {
                map.put(1000, new DeliveryType(1000));
            }
        }), "Adding to the tracker while forEach visits it should fail fast as the iterators do");
    }

    @Test
    public void testForEachActionsThatOnlyReadTheMapAreNotRejected() {
        final UnsettledMap<DeliveryType> map = createMap(5, 10);

        for (int i = 0; i < 25; i++) {
            map.put(i, new DeliveryType(i));
        }

        final AtomicInteger visited = new AtomicInteger();
        map.forEach(delivery -> {
            assertNotNull(delivery);
            assertNotNull(map.get(delivery.getDeliveryId()), "Delivery should still be tracked while visited");
            assertTrue(map.containsKey(delivery.getDeliveryId()), "Delivery should still be tracked while visited");
            visited.incrementAndGet();
        });

        assertEquals(25, visited.get(), "An action that only reads the tracker should visit every delivery");

        final AtomicInteger rangedVisited = new AtomicInteger();
        map.forEach(0, 24, delivery -> rangedVisited.incrementAndGet());

        assertEquals(25, rangedVisited.get(), "An action that only reads the tracker should visit every delivery");
        assertEquals(25, map.size(), "An action that only reads the tracker must not alter its contents");
    }

    @Test
    public void testRangedAccessAfterRemovalsCompactEntriesIntoAdjacentBuckets() {
        final UnsettledMap<DeliveryType> map = createMap(3, 10);

        for (int i = 0; i < 30; i++) {
            map.put(i, new DeliveryType(i));
        }

        // Removing a run that spans two buckets leaves those buckets partially empty which lets the
        // remaining entries be merged into the neighboring buckets. The lowest and highest delivery
        // IDs held by each bucket are re-derived as part of that work, and every search either skips
        // an entire bucket or stops searching the buckets entirely based on those two values, so the
        // merged buckets must still describe the entries they now hold.
        final List<Integer> removed = new ArrayList<>();
        map.removeEach(4, 15, (delivery) -> removed.add(delivery.getDeliveryId()));

        assertEquals(12, removed.size(), "Every delivery of the range should have been removed: " + removed);
        for (int i = 4; i <= 15; i++) {
            assertTrue(removed.contains(i), "Delivery " + i + " is within the range and should have been removed");
        }
        assertEquals(18, map.size(), "Only the deliveries covered by the range should have been removed");

        for (int i = 0; i < 30; i++) {
            if (i >= 4 && i <= 15) {
                assertNull(map.get(i), "Delivery " + i + " was within the range and should not be tracked");
                assertFalse(map.containsKey(i), "Delivery " + i + " was within the range and should not be tracked");
            } else {
                assertNotNull(map.get(i), "Delivery " + i + " should still be tracked after the entries were merged");
                assertTrue(map.containsKey(i), "Delivery " + i + " should still be tracked after the entries were merged");
            }
        }

        // A range that covers both the entries that stayed put and the entries that were moved must
        // visit each one exactly once, and a range of only settled deliveries must match nothing.
        final List<Integer> visited = new ArrayList<>();
        map.forEach(0, 29, (delivery) -> visited.add(delivery.getDeliveryId()));

        assertEquals(18, visited.size(), "Ranged visit should cover every tracked delivery: " + visited);
        for (int i = 0; i < 30; i++) {
            if (i >= 4 && i <= 15) {
                assertFalse(visited.contains(i), "Delivery " + i + " is no longer tracked: " + visited);
            } else {
                assertTrue(visited.contains(i), "Delivery " + i + " is within the range and should be visited: " + visited);
            }
        }

        final List<Integer> settledRange = new ArrayList<>();
        map.forEach(5, 15, (delivery) -> settledRange.add(delivery.getDeliveryId()));

        assertTrue(settledRange.isEmpty(), "No delivery of the settled range should be visited: " + settledRange);

        // Removing the entries that were moved into the first bucket pulls the highest delivery ID
        // tracked by that bucket back again, which must not hide the entries that remain ahead of it.
        final List<Integer> secondRemoval = new ArrayList<>();
        map.removeEach(16, 19, (delivery) -> secondRemoval.add(delivery.getDeliveryId()));

        assertEquals(4, secondRemoval.size(), "Every delivery of the range should have been removed: " + secondRemoval);
        assertEquals(14, map.size(), "Only the deliveries covered by the range should have been removed");

        for (int i = 0; i < 4; i++) {
            assertNotNull(map.get(i), "Delivery " + i + " is outside the range and should still be tracked");
        }
        for (int i = 16; i < 20; i++) {
            assertNull(map.get(i), "Delivery " + i + " is within the range and should not be tracked");
        }
        for (int i = 20; i < 30; i++) {
            assertNotNull(map.get(i), "Delivery " + i + " is outside the range and should still be tracked");
        }

        final List<Integer> remaining = new ArrayList<>();
        map.forEach(0, UnsignedInteger.MAX_VALUE.intValue(), (delivery) -> remaining.add(delivery.getDeliveryId()));

        assertEquals(14, remaining.size(), "Ranged visit should cover every tracked delivery: " + remaining);

        final List<Integer> lowSide = new ArrayList<>();
        map.forEach(0, 19, (delivery) -> lowSide.add(delivery.getDeliveryId()));

        assertEquals(4, lowSide.size(), "Only the deliveries below the settled range should be visited: " + lowSide);
        for (int i = 0; i < 4; i++) {
            assertTrue(lowSide.contains(i), "Delivery " + i + " is within the range and should be visited: " + lowSide);
        }

        final List<Integer> emptyLowSide = new ArrayList<>();
        map.forEach(4, 19, (delivery) -> emptyLowSide.add(delivery.getDeliveryId()));

        assertTrue(emptyLowSide.isEmpty(), "No delivery of the settled range should be visited: " + emptyLowSide);
    }

    @Test
    public void testRangedForEachWhenWrappedRangeStraddlesTheOverflowOfAFullySettledEarlierGeneration() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int PRE_WRAP_LAST_DELIVERY_ID = UnsignedInteger.MAX_VALUE.intValue() - 15;
        final int PRE_WRAP_FIRST_DELIVERY_ID = PRE_WRAP_LAST_DELIVERY_ID - 4;

        // Deliveries of the generation that precedes the ID overflow, followed by the deliveries
        // that follow the overflow, and then every delivery of the earlier generation is settled,
        // which leaves the tracker holding only deliveries that follow the overflow.
        for (int i = PRE_WRAP_FIRST_DELIVERY_ID; i <= PRE_WRAP_LAST_DELIVERY_ID; i++) {
            map.put(i, new DeliveryType(i));
        }
        for (int i = 0; i < 11; i++) {
            map.put(i, new DeliveryType(i));
        }
        for (int i = PRE_WRAP_FIRST_DELIVERY_ID; i <= PRE_WRAP_LAST_DELIVERY_ID; i++) {
            assertNotNull(map.remove(i), "Delivery before the overflow should have been tracked: " + i);
        }

        assertEquals(11, map.size(), "Only the deliveries that follow the overflow should remain tracked");

        // The range straddles the overflow point so it covers [first .. MAX_VALUE] and [0 .. 5].
        // The earlier generation is gone and can match nothing in the first half, but the tracked
        // deliveries all fall inside the second half of the range.
        for (int i = 0; i <= 5; i++) {
            assertTrue(map.containsKey(i), "Delivery " + i + " should be tracked before the ranged visit");
        }

        final List<Integer> visited = new ArrayList<>();
        map.forEach(PRE_WRAP_LAST_DELIVERY_ID + 1, 5, (delivery) -> visited.add(delivery.getDeliveryId()));

        assertEquals(6, visited.size(), "Ranged visit should cover the tracked deliveries that fall within"
            + " the second half of the straddling range: " + visited);
        for (int i = 0; i <= 5; i++) {
            assertTrue(visited.contains(i), "Delivery " + i + " is within the range and should be visited: " + visited);
        }
        assertEquals(11, map.size(), "forEach should not alter the tracker contents");
    }

    @Test
    public void testRemoveEachWhenWrappedRangeStraddlesTheOverflowOfAFullySettledEarlierGeneration() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        final int PRE_WRAP_LAST_DELIVERY_ID = UnsignedInteger.MAX_VALUE.intValue() - 15;
        final int PRE_WRAP_FIRST_DELIVERY_ID = PRE_WRAP_LAST_DELIVERY_ID - 4;

        // Same layout as the forEach test above: deliveries that follow the overflow are the only
        // ones left unsettled once the generation that preceded the overflow was settled.
        for (int i = PRE_WRAP_FIRST_DELIVERY_ID; i <= PRE_WRAP_LAST_DELIVERY_ID; i++) {
            map.put(i, new DeliveryType(i));
        }
        for (int i = 0; i < 11; i++) {
            map.put(i, new DeliveryType(i));
        }
        for (int i = PRE_WRAP_FIRST_DELIVERY_ID; i <= PRE_WRAP_LAST_DELIVERY_ID; i++) {
            assertNotNull(map.remove(i), "Delivery before the overflow should have been tracked: " + i);
        }

        assertEquals(11, map.size(), "Only the deliveries that follow the overflow should remain tracked");

        for (int i = 0; i <= 5; i++) {
            assertTrue(map.containsKey(i), "Delivery " + i + " should be tracked before the ranged removal");
        }

        final List<Integer> removed = new ArrayList<>();
        map.removeEach(PRE_WRAP_LAST_DELIVERY_ID + 1, 5, (delivery) -> removed.add(delivery.getDeliveryId()));

        assertEquals(6, removed.size(), "Ranged removal should cover the tracked deliveries that fall within"
            + " the second half of the straddling range: " + removed);
        for (int i = 0; i <= 5; i++) {
            assertNull(map.get(i), "Delivery " + i + " is within the range and should have been removed");
        }
        assertNotNull(map.get(6), "Deliveries past the end of the range should still be tracked");
        assertEquals(5, map.size(), "Only the deliveries covered by the range should have been removed");
    }

    @Test
    public void testForEachFirstRangeIsAllThatIsInMap() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        map.put(UnsignedInteger.valueOf(10), new DeliveryType(10));
        map.put(UnsignedInteger.valueOf(12), new DeliveryType(12));
        map.put(UnsignedInteger.valueOf(14), new DeliveryType(14));

        final List<Integer> removed = new ArrayList<>();

        map.forEach(10, 5, d -> removed.add(d.getDeliveryId()));

        assertEquals(3, removed.size());
        assertTrue(removed.contains(10));
        assertTrue(removed.contains(12));
        assertTrue(removed.contains(14));
    }

    @Test
    public void testRemoveEachClearFirstRange() {
        final UnsettledMap<DeliveryType> map = createMap(5, 50);

        map.put(UnsignedInteger.valueOf(10), new DeliveryType(10));
        map.put(UnsignedInteger.valueOf(12), new DeliveryType(12));
        map.put(UnsignedInteger.valueOf(14), new DeliveryType(14));

        final List<Integer> removed = new ArrayList<>();

        map.removeEach(10, 5, d -> removed.add(d.getDeliveryId()));

        assertEquals(3, removed.size());
        assertTrue(removed.contains(10));
        assertTrue(removed.contains(12));
        assertTrue(removed.contains(14));
    }

    //----- Iterator removal when the delivery ID series rolls over the unsigned integer range

    @Test
    public void testValuesIteratorRemoveVisitsAllDeliveriesAfterIDOverflow() {
        final UnsettledMap<DeliveryType> tracker = createMap(3, 4);

        // One bucket of IDs that precede the ID series rolling over, then one bucket of IDs that
        // follow the overflow, which leaves the two buckets with overlapping ID ranges.
        for (int i = 10; i < 14; i++) {
            tracker.put(i, new DeliveryType(i));
        }
        for (int i = 5; i < 9; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(8, tracker.size());

        final List<Integer> visited = new ArrayList<>();
        final Iterator<DeliveryType> iterator = tracker.values().iterator();

        while (iterator.hasNext()) {
            final DeliveryType delivery = iterator.next();

            visited.add(delivery.getDeliveryId());

            // ID 13 is the last entry of the first bucket, ID 5 the first entry of the second.
            if (delivery.getDeliveryId() == 13 || delivery.getDeliveryId() == 5) {
                iterator.remove();
            }
        }

        assertEquals(Arrays.asList(10, 11, 12, 13, 5, 6, 7, 8), visited,
            "Iteration should visit every delivery of both buckets: " + visited);
        assertEquals(6, tracker.size(), "Both selected deliveries should have been removed");
        assertNull(tracker.get(13), "Delivery removed from the bucket that precedes the overflow should be gone");
        assertNull(tracker.get(5), "Delivery removed from the bucket that follows the overflow should be gone");

        for (int i : new int[] { 10, 11, 12, 6, 7, 8 }) {
            assertNotNull(tracker.get(i), "Delivery " + i + " should still be tracked");
        }
    }

    @Test
    public void testKeysIteratorRemoveVisitsAllDeliveriesAfterIDOverflow() {
        final UnsettledMap<DeliveryType> tracker = createMap(3, 4);

        // IDs 10..13 precede the ID series rolling over, IDs 5..8 follow it.
        for (int i = 10; i < 14; i++) {
            tracker.put(i, new DeliveryType(i));
        }
        for (int i = 5; i < 9; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(8, tracker.size());

        final List<Integer> visited = new ArrayList<>();
        final Iterator<UnsignedInteger> iterator = tracker.keySet().iterator();

        while (iterator.hasNext()) {
            final UnsignedInteger key = iterator.next();
            visited.add(key.intValue());

            if (key.intValue() == 13 || key.intValue() == 5) {
                iterator.remove();
            }
        }

        assertEquals(Arrays.asList(10, 11, 12, 13, 5, 6, 7, 8), visited,
            "Iteration should visit every key of both buckets: " + visited);
        assertEquals(6, tracker.size(), "Both selected keys should have been removed");
        assertNull(tracker.get(13), "Key removed from the bucket that precedes the overflow should be gone");
        assertNull(tracker.get(5), "Key removed from the bucket that follows the overflow should be gone");
    }

    @Test
    public void testEntryIteratorRemoveVisitsAllDeliveriesAfterIDOverflow() {
        final UnsettledMap<DeliveryType> tracker = createMap(3, 4);

        // IDs 10..13 precede the ID series rolling over, IDs 5..8 follow it.
        for (int i = 10; i < 14; i++) {
            tracker.put(i, new DeliveryType(i));
        }
        for (int i = 5; i < 9; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(8, tracker.size());

        final List<Integer> visited = new ArrayList<>();
        final Iterator<Entry<UnsignedInteger, DeliveryType>> iterator = tracker.entrySet().iterator();
        while (iterator.hasNext()) {
            final Entry<UnsignedInteger, DeliveryType> entry = iterator.next();
            visited.add(entry.getKey().intValue());

            if (entry.getKey().intValue() == 13 || entry.getKey().intValue() == 5) {
                iterator.remove();
            }
        }

        assertEquals(Arrays.asList(10, 11, 12, 13, 5, 6, 7, 8), visited,
            "Iteration should visit every entry of both buckets: " + visited);
        assertEquals(6, tracker.size(), "Both selected entries should have been removed");
        assertNull(tracker.get(13), "Entry removed from the bucket that precedes the overflow should be gone");
        assertNull(tracker.get(5), "Entry removed from the bucket that follows the overflow should be gone");
    }

    @Test
    public void testValuesRemoveIfRemovesDeliveriesOnBothSidesOfIDOverflow() {
        final UnsettledMap<DeliveryType> tracker = createMap(3, 4);

        // IDs 10..13 precede the ID series rolling over, IDs 5..8 follow it.
        for (int i = 10; i < 14; i++) {
            tracker.put(i, new DeliveryType(i));
        }
        for (int i = 5; i < 9; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(8, tracker.size());

        final boolean modified = tracker.values().removeIf(delivery -> {
            return delivery.getDeliveryId() < 6 || delivery.getDeliveryId() == 13;
        });

        assertTrue(modified, "Deliveries on both sides of the overflow should have been removed");

        assertEquals(6, tracker.size(), "Two deliveries should have been removed");
        assertNull(tracker.get(5), "Delivery removed before the overflow point should be gone");
        assertNull(tracker.get(13), "Delivery removed after the overflow point should be gone");

        for (int i : new int[] { 10, 11, 12, 6, 7, 8 }) {
            assertNotNull(tracker.get(i), "Delivery " + i + " should still be tracked");
        }
    }

    @Test
    public void testValuesIteratorRemoveVisitsAllDeliveriesAfterIDOverflowWithDefaultBuckets() {
        final UnsettledMap<DeliveryType> tracker = createMap(); // Default bucket size of 256

        // Two exactly filled buckets, the second one starting below the ID the first one ended on.
        final int firstIDBeforeOverflow = UnsignedInteger.MAX_VALUE.intValue() - 255;
        for (int i = 0; i < 256; i++) {
            tracker.put(firstIDBeforeOverflow + i, new DeliveryType(firstIDBeforeOverflow + i));
        }
        for (int i = 0; i < 256; i++) {
            tracker.put(i, new DeliveryType(i));
        }

        assertEquals(512, tracker.size());

        final int lastIDBeforeOverflow = UnsignedInteger.MAX_VALUE.intValue();
        final Iterator<DeliveryType> iterator = tracker.values().iterator();

        int visited = 0;

        while (iterator.hasNext()) {
            final DeliveryType delivery = iterator.next();
            visited++;

            if (delivery.getDeliveryId() == lastIDBeforeOverflow || delivery.getDeliveryId() == 0) {
                iterator.remove();
            }
        }

        assertEquals(512, visited, "Iteration should visit every delivery of both buckets");
        assertEquals(510, tracker.size(), "Both selected deliveries should have been removed");
        assertNull(tracker.get(lastIDBeforeOverflow), "Delivery removed before the overflow point should be gone");
        assertNull(tracker.get(0), "Delivery removed after the overflow point should be gone");
        assertNotNull(tracker.get(255), "Last delivery of the tracker should still be tracked");
    }

    @Test
    public void testEveryTrackedDeliveryRemainsReachableAfterTheIDSeriesRollsBackwardsTwice() {
        // Two deliveries per bucket so that a drained bucket is recycled between the additions.
        final UnsettledMap<DeliveryType> tracker = createMap(3, 2);
        final int nearMaxId = Integer.MAX_VALUE;

        // The second and third delivery make the ID series roll backwards, which begins a new
        // series of deliveries in its own bucket, the fourth joins the series that precedes it.
        tracker.put(nearMaxId + 3, new DeliveryType(nearMaxId + 3));
        tracker.put(nearMaxId + 1, new DeliveryType(nearMaxId + 1));
        tracker.put(nearMaxId - 4, new DeliveryType(nearMaxId - 4));
        tracker.put(nearMaxId - 1, new DeliveryType(nearMaxId - 1));

        // Draining the bucket of the first rolled back series recycles it, and the next addition
        // of that ID recycles the bucket it lands in when it is settled again.
        assertNotNull(tracker.remove(nearMaxId + 1), "Delivery should have been tracked");
        tracker.put(nearMaxId + 1, new DeliveryType(nearMaxId + 1));
        assertNotNull(tracker.remove(nearMaxId + 1), "Delivery should have been tracked");

        final List<DeliveryType> tracked = new ArrayList<>(tracker.values());
        assertEquals(3, tracked.size(), "Every delivery that was not settled should still be tracked");

        // The tracker still knows the deliveries are unsettled so each of them must remain
        // reachable, otherwise no disposition can ever settle them.
        for (DeliveryType delivery : tracked) {
            final int deliveryId = delivery.getDeliveryId();
            final String name = "delivery " + UnsignedInteger.valueOf(deliveryId);
            assertNotNull(tracker.get(deliveryId), "The " + name + " should be reachable by its ID");
            assertTrue(tracker.containsKey(deliveryId), "The " + name + " should be tracked by its ID");
            assertEquals(delivery, tracker.remove(deliveryId), "The " + name + " should be settleable");
        }

        assertTrue(tracker.isEmpty(), "Settling every tracked delivery should empty the tracker");
    }
}
