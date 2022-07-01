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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test {@link SplayMap} type
 */
public class SplayMapTest {

    protected static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SplayMapTest.class);

    protected long seed;
    protected Random random;

    @BeforeEach
    public void setUp() {
        seed = System.nanoTime();
        random = new Random();
        random.setSeed(seed);
    }

    protected <E> SplayMap<E> createMap() {
        return new SplayMap<>();
    }

    @Test
    public void testComparator() {
        SplayMap<String> map = createMap();

        assertNotNull(map.comparator());
        assertSame(map.comparator(), map.comparator());
    }

    @Test
    public void testClear() {
        SplayMap<String> map = createMap();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());

        map.put(2, "two");
        map.put(0, "zero");
        map.put(1, "one");

        assertEquals(3, map.size());
        assertFalse(map.isEmpty());

        map.clear();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());

        map.put(5, "five");
        map.put(9, "nine");
        map.put(3, "three");
        map.put(7, "seven");
        map.put(-1, "minus one");

        assertEquals(5, map.size());
        assertFalse(map.isEmpty());

        map.clear();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());

        map.clear();
    }

    @Test
    public void testSize() {
        SplayMap<String> map = createMap();

        assertEquals(0, map.size());
        map.put(0, "zero");
        assertEquals(1, map.size());
        map.put(1, "one");
        assertEquals(2, map.size());
        map.put(0, "update");
        assertEquals(2, map.size());
        map.remove(0);
        assertEquals(1, map.size());
        map.remove(1);
        assertEquals(0, map.size());
    }

    @Test
    public void testInsert() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");
        map.put(5, "five");
        map.put(9, "nine");
        map.put(7, "seven");
        map.put(-1, "minus one");

        assertEquals(8, map.size());
    }

    @Test
    public void testInsertUnsignedInteger() {
        SplayMap<String> map = createMap();

        map.put(UnsignedInteger.valueOf(0), "zero");
        map.put(UnsignedInteger.valueOf(1), "one");
        map.put(UnsignedInteger.valueOf(2), "two");
        map.put(UnsignedInteger.valueOf(3), "three");
        map.put(UnsignedInteger.valueOf(5), "five");
        map.put(UnsignedInteger.valueOf(9), "nine");
        map.put(UnsignedInteger.valueOf(7), "seven");
        map.put(UnsignedInteger.valueOf(-1), "minus one");

        assertEquals(8, map.size());
    }

    @Test
    public void testInsertAndReplace() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "foo");
        assertEquals("foo", map.put(2, "two"));

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("two", map.get(2));

        assertEquals(3, map.size());
    }

    @Test
    public void testInsertAndRemove() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");

        assertEquals(3, map.size());

        assertEquals("zero", map.remove(0));
        assertEquals("one", map.remove(1));
        assertEquals("two", map.remove(2));

        assertEquals(0, map.size());
    }

    @Test
    public void testPutAll() {
        SplayMap<String> map = createMap();

        Map<UnsignedInteger, String> hashmap = new HashMap<>();

        hashmap.put(UnsignedInteger.valueOf(0), "zero");
        hashmap.put(UnsignedInteger.valueOf(1), "one");
        hashmap.put(UnsignedInteger.valueOf(2), "two");
        hashmap.put(UnsignedInteger.valueOf(3), "three");
        hashmap.put(UnsignedInteger.valueOf(5), "five");
        hashmap.put(UnsignedInteger.valueOf(9), "nine");
        hashmap.put(UnsignedInteger.valueOf(7), "seven");
        hashmap.put(UnsignedInteger.valueOf(-1), "minus one");

        map.putAll(hashmap);

        assertEquals(8, map.size());

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("two", map.get(2));
        assertEquals("three", map.get(3));
        assertEquals("five", map.get(5));
        assertEquals("nine", map.get(9));
        assertEquals("seven", map.get(7));
        assertEquals("minus one", map.get(-1));
    }

    @Test
    public void testPutIfAbsent() {
        SplayMap<String> map = createMap();

        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(0), "zero"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(1), "one"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(2), "two"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(3), "three"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(5), "five"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(9), "nine"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(7), "seven"));
        assertNull(map.putIfAbsent(UnsignedInteger.valueOf(-1), "minus one"));

        assertEquals(8, map.size());

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("two", map.get(2));
        assertEquals("three", map.get(3));
        assertEquals("five", map.get(5));
        assertEquals("nine", map.get(9));
        assertEquals("seven", map.get(7));
        assertEquals("minus one", map.get(-1));

        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(0), "zero-zero"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(1), "one-one"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(2), "two-two"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(3), "three-three"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(5), "five-five"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(9), "nine-nine"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(7), "seven-seven"));
        assertNotNull(map.putIfAbsent(UnsignedInteger.valueOf(-1), "minus one minus one"));

        assertEquals(8, map.size());

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("two", map.get(2));
        assertEquals("three", map.get(3));
        assertEquals("five", map.get(5));
        assertEquals("nine", map.get(9));
        assertEquals("seven", map.get(7));
        assertEquals("minus one", map.get(-1));
    }

    @Test
    public void testGetWhenEmpty() {
        SplayMap<String> map = createMap();

        assertNull(map.get(0));
    }

    @Test
    public void testGet() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("-three", map.get(-3));

        assertNull(map.get(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testGetUnsignedInteger() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertEquals("zero", map.get(UnsignedInteger.valueOf(0)));
        assertEquals("one", map.get(UnsignedInteger.valueOf(1)));
        assertEquals("-three", map.get(UnsignedInteger.valueOf(-3)));

        assertNull(map.get(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsKeyOnEmptyMap() {
        SplayMap<String> map = createMap();

        assertFalse(map.containsKey(0));
        assertFalse(map.containsKey(UnsignedInteger.ZERO));
    }

    @Test
    public void testContainsKey() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertTrue(map.containsKey(0));
        assertFalse(map.containsKey(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsKeyUnsignedInteger() {
        SplayMap<String> map = createMap();

        map.put(UnsignedInteger.valueOf(0), "zero");
        map.put(UnsignedInteger.valueOf(1), "one");
        map.put(UnsignedInteger.valueOf(-3), "-three");

        assertTrue(map.containsKey(0));
        assertFalse(map.containsKey(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsValue() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertTrue(map.containsValue("zero"));
        assertFalse(map.containsValue("four"));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsValueOnEmptyMap() {
        SplayMap<String> map = createMap();

        assertFalse(map.containsValue("0"));
    }

    @Test
    public void testRemove() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(9, "nine");
        map.put(7, "seven");
        map.put(-1, "minus one");

        assertEquals(5, map.size());
        assertNull(map.remove(5));
        assertEquals(5, map.size());
        assertEquals("nine", map.remove(9));
        assertEquals(4, map.size());
    }

    @Test
    public void testRemoveIsIdempotent() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");

        assertEquals(3, map.size());

        assertEquals("zero", map.remove(0));
        assertEquals(null, map.remove(0));

        assertEquals(2, map.size());

        assertEquals("one", map.remove(1));
        assertEquals(null, map.remove(1));

        assertEquals(1, map.size());

        assertEquals("two", map.remove(2));
        assertEquals(null, map.remove(2));

        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveValueNotInMap() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(9, "nine");
        map.put(7, "seven");
        map.put(-1, "minus one");

        assertNull(map.remove(5));
    }

    @Test
    public void testRemoveFirstEntryTwice() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(16, "sixteen");

        assertNotNull(map.remove(0));
        assertNull(map.remove(0));
    }

    @Test
    @SuppressWarnings({"SuspiciousMethodCalls", "CollectionIncompatibleType"})
    public void testRemoveWithInvalidType() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");

        try {
            map.remove("foo");
            fail("Should not accept incompatible types");
        } catch (ClassCastException ccex) {}
    }

    @Test
    public void testRemoveUnsignedInteger() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(UnsignedInteger.valueOf(9), "nine");
        map.put(7, "seven");
        map.put(UnsignedInteger.valueOf(-1), "minus one");

        assertEquals(5, map.size());
        assertNull(map.remove(UnsignedInteger.valueOf(5)));
        assertEquals(5, map.size());
        assertEquals("nine", map.remove(UnsignedInteger.valueOf(9)));
        assertEquals(4, map.size());
    }

    @Test
    @SuppressWarnings({"SuspiciousMethodCalls", "CollectionIncompatibleType"})
    public void testRemoveInteger() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(UnsignedInteger.valueOf(9), "nine");
        map.put(7, "seven");
        map.put(UnsignedInteger.valueOf(-1), "minus one");

        assertEquals(5, map.size());
        assertNull(map.remove(Integer.valueOf(5)));
        assertEquals(5, map.size());
        assertEquals("nine", map.remove(Integer.valueOf(9)));
        assertEquals(4, map.size());
    }

    @Test
    public void testRemoveEntryWithValue() {
        SplayMap<String> map = createMap();

        assertFalse(map.remove(1, "zero"));

        map.put(0, "zero");
        map.put(1, "one");
        map.put(UnsignedInteger.valueOf(9), "nine");
        map.put(7, "seven");
        map.put(UnsignedInteger.valueOf(-1), "minus one");

        assertEquals(5, map.size());
        assertFalse(map.remove(1, "zero"));
        assertEquals(5, map.size());
        assertTrue(map.remove(1, "one"));
        assertEquals(4, map.size());
        assertFalse(map.remove(42, "forty-two"));
        assertEquals(4, map.size());

        assertEquals("zero", map.get(0));
        assertEquals("nine", map.get(UnsignedInteger.valueOf(9)));
        assertEquals("seven", map.get(7));
        assertEquals("minus one", map.get(-1));
    }

    @Test
    public void testReplaceOldValueWithNew() {
        SplayMap<String> map = createMap();

        assertFalse(map.replace(1, "two", "zero-zero"));

        map.put(0, "zero");
        map.put(1, "one");
        map.put(UnsignedInteger.valueOf(9), "nine");
        map.put(7, "seven");
        map.put(UnsignedInteger.valueOf(-1), "minus one");

        assertEquals(5, map.size());
        assertFalse(map.replace(1, "two", "zero-zero"));
        assertEquals(5, map.size());
        assertTrue(map.replace(1, "one", "one-one"));
        assertEquals(5, map.size());
        assertFalse(map.replace(42, null, "forty-two"));
        assertEquals(5, map.size());
        assertEquals("one-one", map.get(1));

        assertTrue(map.replace(UnsignedInteger.valueOf(1), "one-one", "one"));
        assertEquals(5, map.size());
        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("nine", map.get(UnsignedInteger.valueOf(9)));
        assertEquals("seven", map.get(7));
        assertEquals("minus one", map.get(-1));    }

    @Test
    public void testReplaceValue() {
        SplayMap<String> map = createMap();

        assertNull(map.replace(1, "zero-zero"));

        map.put(0, "zero");
        map.put(1, "one");
        map.put(UnsignedInteger.valueOf(9), "nine");
        map.put(7, "seven");
        map.put(UnsignedInteger.valueOf(-1), "minus one");

        assertEquals(5, map.size());
        assertEquals("one", map.replace(1, "one-one"));
        assertEquals(5, map.size());
        assertNull(map.replace(42, "forty-two"));
        assertEquals(5, map.size());
        assertEquals("one-one", map.get(1));

        assertEquals("one-one", map.replace(UnsignedInteger.valueOf(1), "one"));
        assertEquals(5, map.size());
        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("nine", map.get(UnsignedInteger.valueOf(9)));
        assertEquals("seven", map.get(7));
        assertEquals("minus one", map.get(-1));
    }

    @Test
    public void testValuesCollection() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "one");
        map.put(3, "one");

        Collection<String> values = map.values();
        assertNotNull(values);
        assertEquals(4, values.size());
        assertFalse(values.isEmpty());
        assertSame(values, map.values());
    }

    @Test
    public void testValuesIteration() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Collection<String> values = map.values();
        Iterator<String> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals("" + intValues[counter++], iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
    }

    @Test
    public void testValuesIterationRemove() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Collection<String> values = map.values();
        Iterator<String> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals("" + intValues[counter++], iterator.next());
            iterator.remove();
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void testValuesIterationFollowUnsignedOrderingExpectations() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Collection<String> values = map.values();
        Iterator<String> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals("" + expectedOrder[counter++], iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(inputValues.length, counter);
    }

    @Test
    public void testValuesIterationFailsWhenConcurrentlyModified() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Collection<String> values = map.values();
        Iterator<String> iterator = values.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        map.remove(3);

        try {
            iterator.next();
            fail("Should not iterate when modified outside of iterator");
        } catch (ConcurrentModificationException cme) {}
    }

    @Test
    public void testValuesIterationOnEmptyTree() {
        SplayMap<String> map = createMap();
        Collection<String> values = map.values();
        Iterator<String> iterator = values.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @Test
    public void testKeySetReturned() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        Set<UnsignedInteger> keys = map.keySet();
        assertNotNull(keys);
        assertEquals(4, keys.size());
        assertFalse(keys.isEmpty());
        assertSame(keys, map.keySet());
    }

    @Test
    public void testKeysIterationRemove() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.keySet();
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
    public void testKeysIteration() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.keySet();
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
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void testKeysIterationFollowsUnsignedOrderingExpectations() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(UnsignedInteger.valueOf(expectedOrder[counter++]), iterator.next());
        }

        // Check that we really did iterate.
        assertEquals(inputValues.length, counter);
    }

    @Test
    public void testKeysIterationFailsWhenConcurrentlyModified() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        map.remove(3);

        try {
            iterator.next();
            fail("Should not iterate when modified outside of iterator");
        } catch (ConcurrentModificationException cme) {}
    }

    @Test
    public void testKeysIterationOnEmptyTree() {
        SplayMap<String> map = createMap();
        Collection<UnsignedInteger> keys = map.keySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @Test
    public void tesEntrySetReturned() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        assertNotNull(entries);
        assertEquals(4, entries.size());
        assertFalse(entries.isEmpty());
        assertSame(entries, map.entrySet());
    }

    @Test
    public void tesEntrySetContains() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        Set<Entry<UnsignedInteger, String>> entries = map.entrySet();
        assertNotNull(entries);
        assertEquals(4, entries.size());
        assertFalse(entries.isEmpty());
        assertSame(entries, map.entrySet());

        OutsideEntry<UnsignedInteger, String> entry1 = new OutsideEntry<>(UnsignedInteger.valueOf(0), "zero");
        OutsideEntry<UnsignedInteger, String> entry2 = new OutsideEntry<>(UnsignedInteger.valueOf(0), "hero");

        assertTrue(entries.contains(entry1));
        assertFalse(entries.contains(entry2));
    }

    @Test
    public void testEntryIteration() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        Iterator<Entry<UnsignedInteger, String>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            Entry<UnsignedInteger, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(UnsignedInteger.valueOf(intValues[counter]), entry.getKey());
            assertEquals("" + intValues[counter++], entry.getValue());
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
    }

    @Test
    public void testEntryIterationRemove() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        Iterator<Entry<UnsignedInteger, String>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            Entry<UnsignedInteger, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(UnsignedInteger.valueOf(intValues[counter]), entry.getKey());
            assertEquals("" + intValues[counter++], entry.getValue());
            iterator.remove();
        }

        // Check that we really did iterate.
        assertEquals(intValues.length, counter);
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void testEntryIterationFollowsUnsignedOrderingExpectations() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        Iterator<Entry<UnsignedInteger, String>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int counter = 0;
        while (iterator.hasNext()) {
            Entry<UnsignedInteger, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(UnsignedInteger.valueOf(expectedOrder[counter]), entry.getKey());
            assertEquals("" + expectedOrder[counter++], entry.getValue());
        }

        // Check that we really did iterate.
        assertEquals(inputValues.length, counter);
    }

    @Test
    public void testEntryIterationFailsWhenConcurrentlyModified() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        Iterator<Entry<UnsignedInteger, String>> iterator = entries.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        map.remove(3);

        try {
            iterator.next();
            fail("Should not iterate when modified outside of iterator");
        } catch (ConcurrentModificationException cme) {}
    }

    @Test
    public void testEntrySetIterationOnEmptyTree() {
        SplayMap<String> map = createMap();
        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        Iterator<Entry<UnsignedInteger, String>> iterator = entries.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @Test
    public void testFirstKeyOnEmptyMap() {
        SplayMap<String> map = new SplayMap<>();
        assertNull(map.firstKey());
    }

    @Test
    public void testFirstKey() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.firstKey().intValue());
            map.remove(expected);
        }

        assertNull(map.firstKey());
    }

    @Test
    public void testFirstEntryOnEmptyMap() {
        SplayMap<String> map = createMap();
        assertNull(map.firstEntry());
    }

    @Test
    public void testFirstEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.firstEntry().getPrimitiveKey());
            map.remove(expected);
        }

        assertNull(map.firstKey());
    }

    @Test
    public void testPollFirstEntryEmptyMap() {
        SplayMap<String> map = createMap();
        assertNull(map.pollFirstEntry());
    }

    @Test
    public void testPollFirstEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.pollFirstEntry().getPrimitiveKey());
        }

        assertNull(map.firstKey());
    }

    @Test
    public void testLastKeyOnEmptyMap() {
        SplayMap<String> map = createMap();
        assertNull(map.lastKey());
    }

    @Test
    public void testLastKey() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {-1, -2, 3, 2, 1, 0};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.lastKey().intValue());
            map.remove(expected);
        }

        assertNull(map.lastKey());
    }

    @Test
    public void testLastEntryOnEmptyMap() {
        SplayMap<String> map = createMap();
        assertNull(map.lastEntry());
    }

    @Test
    public void testLastEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {-1, -2, 3, 2, 1, 0};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.lastEntry().getPrimitiveKey());
            map.remove(expected);
        }

        assertNull(map.lastEntry());
    }

    @Test
    public void testPollLastEntryEmptyMap() {
        SplayMap<String> map = createMap();
        assertNull(map.pollLastEntry());
    }

    @Test
    public void testPollLastEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {-1, -2, 3, 2, 1, 0};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.pollLastEntry().getPrimitiveKey());
        }

        assertNull(map.lastEntry());
    }

    @Test
    public void testForEach() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        final SequenceNumber index = new SequenceNumber(0);
        map.forEach((k, v) -> {
            int value = index.getAndIncrement().intValue();
            assertEquals(expectedOrder[value], k.intValue());
        });

        assertEquals(index.intValue(), inputValues.length);
    }

    @Test
    public void testForEachEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        final SequenceNumber index = new SequenceNumber(0);
        map.forEach((value) -> {
            int i = index.getAndIncrement().intValue();
            assertEquals(expectedOrder[i] + "", value);
        });

        assertEquals(index.intValue(), inputValues.length);
    }

    @Test
    public void testRandomProduceAndConsumeWithBacklog() {
        SplayMap<String> map = createMap();

        final int ITERATIONS = 8192;
        final String DUMMY_STRING = "test";

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                map.put(UnsignedInteger.valueOf(i), DUMMY_STRING);
            }

            for (int i = 0; i < ITERATIONS; ++i) {
                int p = random.nextInt(ITERATIONS);
                int c = random.nextInt(ITERATIONS);

                map.put(UnsignedInteger.valueOf(p), DUMMY_STRING);
                map.remove(UnsignedInteger.valueOf(c));
            }
        } catch (Throwable error) {
            dumpRandomDataSet(ITERATIONS, true);
            throw error;
        }
    }

    @Test
    public void testRandomPutAndGetIntoEmptyMap() {
        SplayMap<String> map = createMap();

        final int ITERATIONS = 8192;
        final String DUMMY_STRING = "test";

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                int p = random.nextInt(ITERATIONS);
                int c = random.nextInt(ITERATIONS);

                map.put(UnsignedInteger.valueOf(p), DUMMY_STRING);
                map.remove(UnsignedInteger.valueOf(c));
            }
        } catch (AssertionError error) {
            dumpRandomDataSet(ITERATIONS, true);
            throw error;
        }
    }

    @Test
    public void testPutRandomValueIntoMapThenRemoveInSameOrder() {
        SplayMap<String> map = createMap();

        final int ITERATIONS = 8192;

        try {
            for (int i = 0; i < ITERATIONS; ++i) {
                final int index = random.nextInt(ITERATIONS);
                map.put(index, String.valueOf(index));
            }

            // Reset to verify insertions
            random.setSeed(seed);

            for (int i = 0; i < ITERATIONS; ++i) {
                final int index = random.nextInt(ITERATIONS);
                assertEquals(String.valueOf(index), map.get(index));
            }

            // Reset to remove
            random.setSeed(seed);

            for (int i = 0; i < ITERATIONS; ++i) {
                final int index = random.nextInt(ITERATIONS);
                map.remove(index);
            }

            assertTrue(map.isEmpty());
        } catch (AssertionError error) {
            dumpRandomDataSet(ITERATIONS, true);
            throw error;
        }
    }

    @Test
    public void testLowerEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(-2), map.lowerEntry(UnsignedInteger.valueOf(-1)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.lowerEntry(UnsignedInteger.valueOf(-2)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.lowerEntry(UnsignedInteger.valueOf(4)).getKey());
        assertEquals(UnsignedInteger.valueOf(2), map.lowerEntry(UnsignedInteger.valueOf(3)).getKey());
        assertEquals(UnsignedInteger.valueOf(1), map.lowerEntry(UnsignedInteger.valueOf(2)).getKey());
        assertEquals(UnsignedInteger.valueOf(0), map.lowerEntry(UnsignedInteger.valueOf(1)).getKey());
        assertNull(map.lowerEntry(UnsignedInteger.valueOf(0)));
    }

    @Test
    public void testLowerKey() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(-2), map.lowerKey(UnsignedInteger.valueOf(-1)));
        assertEquals(UnsignedInteger.valueOf(3), map.lowerKey(UnsignedInteger.valueOf(-2)));
        assertEquals(UnsignedInteger.valueOf(3), map.lowerKey(UnsignedInteger.valueOf(4)));
        assertEquals(UnsignedInteger.valueOf(2), map.lowerKey(UnsignedInteger.valueOf(3)));
        assertEquals(UnsignedInteger.valueOf(1), map.lowerKey(UnsignedInteger.valueOf(2)));
        assertEquals(UnsignedInteger.valueOf(0), map.lowerKey(UnsignedInteger.valueOf(1)));
        assertNull(map.lowerEntry(UnsignedInteger.valueOf(0)));
    }

    @Test
    public void testHigherEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(1), map.higherEntry(UnsignedInteger.valueOf(0)).getKey());
        assertEquals(UnsignedInteger.valueOf(2), map.higherEntry(UnsignedInteger.valueOf(1)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.higherEntry(UnsignedInteger.valueOf(2)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.higherEntry(UnsignedInteger.valueOf(3)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.higherEntry(UnsignedInteger.valueOf(4)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.higherEntry(UnsignedInteger.valueOf(-3)).getKey());
        assertEquals(UnsignedInteger.valueOf(-1), map.higherEntry(UnsignedInteger.valueOf(-2)).getKey());
        assertNull(map.higherEntry(UnsignedInteger.valueOf(-1)));
    }

    @Test
    public void testHigherKey() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(1), map.higherKey(UnsignedInteger.valueOf(0)));
        assertEquals(UnsignedInteger.valueOf(2), map.higherKey(UnsignedInteger.valueOf(1)));
        assertEquals(UnsignedInteger.valueOf(3), map.higherKey(UnsignedInteger.valueOf(2)));
        assertEquals(UnsignedInteger.valueOf(-2), map.higherKey(UnsignedInteger.valueOf(3)));
        assertEquals(UnsignedInteger.valueOf(-2), map.higherKey(UnsignedInteger.valueOf(4)));
        assertEquals(UnsignedInteger.valueOf(-2), map.higherKey(UnsignedInteger.valueOf(-3)));
        assertEquals(UnsignedInteger.valueOf(-1), map.higherKey(UnsignedInteger.valueOf(-2)));
        assertNull(map.higherKey(UnsignedInteger.valueOf(-1)));
    }

    @Test
    public void testFloorEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(-1), map.floorEntry(UnsignedInteger.valueOf(-1)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.floorEntry(UnsignedInteger.valueOf(-2)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.floorEntry(UnsignedInteger.valueOf(4)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.floorEntry(UnsignedInteger.valueOf(-3)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.floorEntry(UnsignedInteger.valueOf(Integer.MAX_VALUE)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.floorEntry(UnsignedInteger.valueOf(3)).getKey());
        assertEquals(UnsignedInteger.valueOf(2), map.floorEntry(UnsignedInteger.valueOf(2)).getKey());
        assertEquals(UnsignedInteger.valueOf(1), map.floorEntry(UnsignedInteger.valueOf(1)).getKey());
        assertEquals(UnsignedInteger.valueOf(0), map.floorEntry(UnsignedInteger.valueOf(0)).getKey());
    }

    @Test
    public void testFloorKey() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(-1), map.floorKey(UnsignedInteger.valueOf(-1)));
        assertEquals(UnsignedInteger.valueOf(-2), map.floorKey(UnsignedInteger.valueOf(-2)));
        assertEquals(UnsignedInteger.valueOf(3), map.floorKey(UnsignedInteger.valueOf(4)));
        assertEquals(UnsignedInteger.valueOf(3), map.floorKey(UnsignedInteger.valueOf(-3)));
        assertEquals(UnsignedInteger.valueOf(3), map.floorKey(UnsignedInteger.valueOf(Integer.MAX_VALUE)));
        assertEquals(UnsignedInteger.valueOf(3), map.floorKey(UnsignedInteger.valueOf(3)));
        assertEquals(UnsignedInteger.valueOf(2), map.floorKey(UnsignedInteger.valueOf(2)));
        assertEquals(UnsignedInteger.valueOf(1), map.floorKey(UnsignedInteger.valueOf(1)));
        assertEquals(UnsignedInteger.valueOf(0), map.floorKey(UnsignedInteger.valueOf(0)));
    }

    @Test
    public void testCeilingEntry() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(0), map.ceilingEntry(UnsignedInteger.valueOf(0)).getKey());
        assertEquals(UnsignedInteger.valueOf(1), map.ceilingEntry(UnsignedInteger.valueOf(1)).getKey());
        assertEquals(UnsignedInteger.valueOf(2), map.ceilingEntry(UnsignedInteger.valueOf(2)).getKey());
        assertEquals(UnsignedInteger.valueOf(3), map.ceilingEntry(UnsignedInteger.valueOf(3)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingEntry(UnsignedInteger.valueOf(4)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingEntry(UnsignedInteger.valueOf(Integer.MAX_VALUE)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingEntry(UnsignedInteger.valueOf(-3)).getKey());
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingEntry(UnsignedInteger.valueOf(-2)).getKey());
        assertEquals(UnsignedInteger.valueOf(-1), map.ceilingEntry(UnsignedInteger.valueOf(-1)).getKey());
    }

    @Test
    public void testCeilingKey() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(UnsignedInteger.valueOf(entry), "" + entry);
        }

        assertEquals(UnsignedInteger.valueOf(0), map.ceilingKey(UnsignedInteger.valueOf(0)));
        assertEquals(UnsignedInteger.valueOf(1), map.ceilingKey(UnsignedInteger.valueOf(1)));
        assertEquals(UnsignedInteger.valueOf(2), map.ceilingKey(UnsignedInteger.valueOf(2)));
        assertEquals(UnsignedInteger.valueOf(3), map.ceilingKey(UnsignedInteger.valueOf(3)));
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingKey(UnsignedInteger.valueOf(4)));
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingKey(UnsignedInteger.valueOf(Integer.MAX_VALUE)));
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingKey(UnsignedInteger.valueOf(-3)));
        assertEquals(UnsignedInteger.valueOf(-2), map.ceilingKey(UnsignedInteger.valueOf(-2)));
        assertEquals(UnsignedInteger.valueOf(-1), map.ceilingKey(UnsignedInteger.valueOf(-1)));
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
