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
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the unsettled delivery tacker
 */
public class UnsettledMapTest {

    protected static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SplayMapTest.class);

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
     * Simple delivery type used for this test
     */
    private class DeliveryType {

        private final int deliveryId;

        public DeliveryType(int deliveryid) {
            this.deliveryId = deliveryid;
        }

        public int getDeliveryId() {
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

    @Test
    public void testContainsValue() {
        UnsettledMap<DeliveryType> tracker = createMap();

        tracker.put(0, new DeliveryType(0));
        tracker.put(1, new DeliveryType(1));
        tracker.put(-3, new DeliveryType(-3));

        assertTrue(tracker.containsValue(new DeliveryType(0)));
        assertFalse(tracker.containsValue(new DeliveryType(4)));

        assertEquals(3, tracker.size());
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

        try {
            tracker.remove("foo");
            fail("Should not accept incompatible types");
        } catch (ClassCastException ccex) {}
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

        assertEquals(index.get(), 3);
    }

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
    public void TestKeySetRetainAllFromCollectionWhenMapHasCustomBucketsAndRetainedIsInFirstBucket() {
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
    public void TestKeySetRetainAllFromCollectionWhenMapHasCustomBucketsAndRetainedIsInLastBucket() {
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

    @Test
    public void tesEntrySetReturned() {
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
    }

    @Test
    public void tesEntrySetContains() {
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
    public void testEntryIterationFollowsInterstionOrderingExpectations() {
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
    public void testRandomProduceAndConsumeWithBacklog() {
        UnsettledMap<DeliveryType> tracker = createMap();

        final int ITERATIONS = 8192;

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                tracker.put(UnsignedInteger.valueOf(i), new DeliveryType(i));
            }

            for (int i = 0; i < ITERATIONS; ++i) {
                int p = random.nextInt(ITERATIONS);
                int c = random.nextInt(ITERATIONS);

                tracker.put(UnsignedInteger.valueOf(p), new DeliveryType(p));
                tracker.remove(UnsignedInteger.valueOf(c));
            }
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, true);
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
            }
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, true);
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
            }
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, true);
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
            dumpRandomDataSet(ITERATIONS, true);
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
            dumpRandomDataSet(ITERATIONS, true);
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
            dumpRandomDataSet(ITERATIONS, true);
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
            dumpRandomDataSet(ITERATIONS, true);
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
                dumpRandomDataSet(ITERATIONS, true);
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
                dumpRandomDataSet(ITERATIONS, true);
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
        assertTrue(m2.equals(m1), "Maps should be equal 7");
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
    public void testRemoveAllEntriesInSmallChunks() {
        final AtomicInteger removed = new AtomicInteger();

        for (int i = 0; i < uintArray.length; i += 2) {
            tracker.removeEach(uintArray[i].intValue(), uintArray[i+1].intValue(), (delivery) -> removed.incrementAndGet());
        }

        assertEquals(uintArray.length, removed.get());
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

    protected void dumpRandomDataSet(int iterations, boolean bounded) {
        final int[] dataSet = new int[iterations];

        random.setSeed(seed);

        for (int i = 0; i < iterations; ++i) {
            if (bounded) {
                dataSet[i] = random.nextInt(iterations);
            } else {
                dataSet[i] = random.nextInt();
            }
        }

        LOG.info("Random seed was: {}" , seed);
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
}
