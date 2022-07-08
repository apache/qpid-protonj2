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
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

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
    protected UnsignedInteger uintArray[] = new UnsignedInteger[1000];
    protected String objArray[] = new String[1000];
    protected SplayMap<String> testMap = new SplayMap<>();

    @BeforeEach
    public void setUp() {
        seed = System.nanoTime();
        random = new Random();
        random.setSeed(seed);

        testMap = new SplayMap<>();
        for (int i = 0; i < objArray.length; i++) {
            UnsignedInteger x = uintArray[i] = UnsignedInteger.valueOf(i);
            String y = objArray[i] = UnsignedInteger.valueOf(i).toString();
            testMap.put(x, y);
        }
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
    public void testSizeWithSubMaps() {
        assertEquals(1000, testMap.size(), "Returned incorrect size");
        assertEquals(500, testMap.headMap(UnsignedInteger.valueOf(500)).size(), "Returned incorrect size");
        assertEquals(0, testMap.headMap(UnsignedInteger.valueOf(0)).size(), "Returned incorrect size");
        assertEquals(1, testMap.headMap(UnsignedInteger.valueOf(1)).size(), "Returned incorrect size");
        assertEquals(501, testMap.headMap(UnsignedInteger.valueOf(501)).size(), "Returned incorrect size");
        assertEquals(500, testMap.tailMap(UnsignedInteger.valueOf(500)).size(), "Returned incorrect size");
        assertEquals(1000, testMap.tailMap(UnsignedInteger.valueOf(0)).size(), "Returned incorrect size");
        assertEquals(500, testMap.tailMap(UnsignedInteger.valueOf(500)).size(), "Returned incorrect size");
        assertEquals(100, testMap.subMap(UnsignedInteger.valueOf(500), UnsignedInteger.valueOf(600)).size(), "Returned incorrect size");

        assertThrows(NullPointerException.class, () -> testMap.subMap(null, UnsignedInteger.valueOf(600)));
        assertThrows(NullPointerException.class, () -> testMap.subMap(UnsignedInteger.valueOf(600), null));
        assertThrows(NullPointerException.class, () -> testMap.subMap(null, null));
        assertThrows(NullPointerException.class, () -> testMap.subMap(null, true, null, true));
        assertThrows(NullPointerException.class, () -> testMap.headMap(null));
        assertThrows(NullPointerException.class, () -> testMap.tailMap(null));
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
    public void testKeysIterationRemoveContract() {
        Set<UnsignedInteger> set = testMap.keySet();
        Iterator<UnsignedInteger> iter = set.iterator();
        iter.next();
        iter.remove();

        // No remove allowed again until next is called
        assertThrows(IllegalStateException.class, () -> iter.remove());

        iter.next();
        iter.remove();

        assertEquals(998, testMap.size());

        iter.next();
        assertNotNull(testMap.remove(999));

        assertThrows(ConcurrentModificationException.class, () -> iter.remove());
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
    public void testKeySetRetainAllFromCollection() throws Exception {
        final Collection<UnsignedInteger> collection = new ArrayList<>();
        collection.add(UnsignedInteger.valueOf(200));

        assertEquals(1000, testMap.size());

        final Set<UnsignedInteger> keys = testMap.keySet();

        keys.retainAll(collection);
        assertEquals(1, testMap.size());
        keys.removeAll(collection);
        assertEquals(0, testMap.size());
        testMap.put(1, "one");
        assertEquals(1, testMap.size());
        keys.clear();
        assertEquals(0, testMap.size());
    }

    @Test
    public void testNavigableKeySetReturned() {
        SplayMap<String> map = createMap();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        Set<UnsignedInteger> keys = map.navigableKeySet();
        assertNotNull(keys);
        assertEquals(4, keys.size());
        assertFalse(keys.isEmpty());
        assertSame(keys, map.keySet());
    }

    @Test
    public void testNavigableKeysIterationRemove() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.navigableKeySet();
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
    public void testNavigableKeysIterationRemoveContract() {
        Set<UnsignedInteger> set = testMap.navigableKeySet();
        Iterator<UnsignedInteger> iter = set.iterator();
        iter.next();
        iter.remove();

        // No remove allowed again until next is called
        assertThrows(IllegalStateException.class, () -> iter.remove());

        iter.next();
        iter.remove();

        assertEquals(998, testMap.size());

        iter.next();
        assertNotNull(testMap.remove(999));

        assertThrows(ConcurrentModificationException.class, () -> iter.remove());
    }

    @Test
    public void testNavigableKeysIteration() {
        SplayMap<String> map = createMap();

        final int[] intValues = {0, 1, 2, 3};

        for (int entry : intValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.navigableKeySet();
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
    public void testNavigableKeysIterationFollowsUnsignedOrderingExpectations() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {0, 1, 2, 3, -2, -1};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.navigableKeySet();
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
    public void testNavigableKeysIterationFailsWhenConcurrentlyModified() {
        SplayMap<String> map = createMap();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        Collection<UnsignedInteger> keys = map.navigableKeySet();
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
    public void testNavigableKeysIterationOnEmptyTree() {
        SplayMap<String> map = createMap();
        Collection<UnsignedInteger> keys = map.navigableKeySet();
        Iterator<UnsignedInteger> iterator = keys.iterator();

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown a NoSuchElementException");
        } catch (NoSuchElementException nse) {
        }
    }

    @Test
    public void testNavigableKeySetRetainAllFromCollection() throws Exception {
        final Collection<UnsignedInteger> collection = new ArrayList<>();
        collection.add(UnsignedInteger.valueOf(200));

        assertEquals(1000, testMap.size());

        final Set<UnsignedInteger> keys = testMap.navigableKeySet();

        keys.retainAll(collection);
        assertEquals(1, testMap.size());
        keys.removeAll(collection);
        assertEquals(0, testMap.size());
        testMap.put(1, "one");
        assertEquals(1, testMap.size());
        keys.clear();
        assertEquals(0, testMap.size());
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

        assertEquals(UnsignedInteger.valueOf(0), map.firstKey());
        SortedMap<UnsignedInteger, String> descending = map.descendingMap();
        assertEquals(UnsignedInteger.valueOf(-1), descending.firstKey());

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

        assertEquals(UnsignedInteger.valueOf(-1), map.lastKey());
        SortedMap<UnsignedInteger, String> descending = map.descendingMap();
        assertEquals(UnsignedInteger.valueOf(0), descending.lastKey());

        for (int expected : expectedOrder) {
            assertEquals(expected, map.lastKey().intValue());
            map.remove(expected);
        }

        assertNull(map.lastKey());
    }

    @Test
    public void testLastKeyAfterSubMap() {
        SplayMap<String> tm = new SplayMap<String>();
        tm.put(1, "VAL001");
        tm.put(3, "VAL003");
        tm.put(2, "VAL002");
        SortedMap<UnsignedInteger, String> sm = tm;
        UnsignedInteger firstKey = sm.firstKey();
        UnsignedInteger lastKey = null;

        for (int i = 1; i <= tm.size(); i++) {
            try {
                lastKey = sm.lastKey();
            } catch (NoSuchElementException excep) {
                assertTrue(sm.isEmpty());
                fail("NoSuchElementException thrown when there are elements in the map");
            }
            sm = sm.subMap(firstKey, lastKey);
        }
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

        NavigableMap<UnsignedInteger, String> descemding = map.descendingMap();

        assertEquals(map.size(), descemding.size());

        descemding.forEach((k, v) -> {
            int value = index.decrement().intValue();
            assertEquals(expectedOrder[value], k.intValue());
        });

        assertEquals(index.intValue(), 0);

        NavigableMap<UnsignedInteger, String> ascending = descemding.descendingMap();

        assertEquals(map.size(), ascending.size());

        ascending.forEach((k, v) -> {
            int value = index.getAndIncrement().intValue();
            assertEquals(expectedOrder[value], k.intValue());
        });

        assertEquals(index.intValue(), expectedOrder.length);
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

        map.remove(0);

        assertNull(map.floorEntry(UnsignedInteger.valueOf(0)));
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

        map.remove(0);

        assertNull(map.floorEntry(UnsignedInteger.valueOf(0)));
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

        map.remove(-1);

        assertNull(map.ceilingEntry(UnsignedInteger.valueOf(-1)));
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

        map.remove(-1);

        assertNull(map.ceilingKey(UnsignedInteger.valueOf(-1)));
    }

    @Test
    public void testHeadMapForBasicTestMap() {
        Map<UnsignedInteger, String> head = testMap.headMap(UnsignedInteger.valueOf(99));
        assertEquals(99, head.size(), "Returned map of incorrect size");
        assertTrue(head.containsKey(UnsignedInteger.valueOf(0)));
        assertTrue(head.containsValue("1"));
        assertTrue(head.containsKey(UnsignedInteger.valueOf(10)));

        SortedMap<UnsignedInteger, Integer> intMap;
        SortedMap<UnsignedInteger, Integer> sub;

        int size = 16;
        intMap = new SplayMap<Integer>();
        for (int i = 1; i <= size; i++) {
            intMap.put(UnsignedInteger.valueOf(i), i);
        }
        sub = intMap.headMap(UnsignedInteger.valueOf(0));
        assertEquals(sub.size(), 0, "size should be zero");
        assertTrue(sub.isEmpty(), "The SubMap should be empty");
        try {
            sub.firstKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch (java.util.NoSuchElementException e) {
        }

        SplayMap<Integer> t = new SplayMap<Integer>();
        try {
            @SuppressWarnings("unused")
            SortedMap<UnsignedInteger, Integer> th = t.headMap(null);
            fail("Should throw a NullPointerException");
        } catch (NullPointerException npe) {
            // expected
        }

        try {
            sub.lastKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch (java.util.NoSuchElementException e) {
        }

        size = 256;
        intMap = new SplayMap<Integer>();
        for (int i = 0; i < size; i++) {
            intMap.put(UnsignedInteger.valueOf(i), i);
        }
        sub = intMap.headMap(UnsignedInteger.valueOf(0));
        assertEquals(sub.size(), 0, "size should be zero");
        assertTrue(sub.isEmpty(), "SubMap should be empty");
        try {
            sub.firstKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch (java.util.NoSuchElementException e) {
        }

        try {
            sub.lastKey();
            fail("java.util.NoSuchElementException should be thrown");
        } catch (java.util.NoSuchElementException e) {
        }
    }

    @Test
    public void testSubMapTwoArgVariant() {
        SortedMap<UnsignedInteger, String> subMap = testMap.subMap(uintArray[100], uintArray[109]);

        assertEquals(9, subMap.size(), "SubMap returned is of incorrect size");
        for (int counter = 100; counter < 109; counter++) {
            assertTrue(subMap.get(uintArray[counter]).equals(objArray[counter]), "SubMap contains incorrect elements");
        }

        assertThrows(IllegalArgumentException.class, () -> testMap.subMap(uintArray[9], uintArray[1]));

        SortedMap<UnsignedInteger, String> map = new SplayMap<String>();
        map.put(UnsignedInteger.valueOf(1), "one");
        map.put(UnsignedInteger.valueOf(2), "two");
        map.put(UnsignedInteger.valueOf(3), "three");
        assertEquals(UnsignedInteger.valueOf(3), map.lastKey());
        SortedMap<UnsignedInteger, String> sub = map.subMap(UnsignedInteger.valueOf(1), UnsignedInteger.valueOf(3));
        assertEquals(UnsignedInteger.valueOf(2), sub.lastKey());

        SortedMap<UnsignedInteger, String> t = new SplayMap<String>();
        assertThrows(NullPointerException.class, () -> t.subMap(null, UnsignedInteger.valueOf(1)));
    }

    @Test
    public void testSubMapIterator() {
        SplayMap<String> map = new SplayMap<String>();

        UnsignedInteger[] keys = { UnsignedInteger.valueOf(1), UnsignedInteger.valueOf(2), UnsignedInteger.valueOf(3) };
        String[] values = { "one", "two", "three" };
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }

        assertEquals(3, map.size());

        SortedMap<UnsignedInteger, String> subMap = map.subMap(UnsignedInteger.valueOf(0), UnsignedInteger.valueOf(4));
        assertEquals(3, subMap.size());

        Set<Map.Entry<UnsignedInteger, String>> entrySet = subMap.entrySet();
        Iterator<Map.Entry<UnsignedInteger, String>> iter = entrySet.iterator();
        int size = 0;
        while (iter.hasNext()) {
            Map.Entry<UnsignedInteger, String> entry = iter.next();
            assertTrue(map.containsKey(entry.getKey()));
            assertTrue(map.containsValue(entry.getValue()));
            size++;
        }
        assertEquals(map.size(), size);

        Set<UnsignedInteger> keySet = subMap.keySet();
        Iterator<UnsignedInteger> keyIter = keySet.iterator();
        size = 0;
        while (keyIter.hasNext()) {
            UnsignedInteger key = keyIter.next();
            assertTrue(map.containsKey(key));
            size++;
        }
        assertEquals(map.size(), size);
    }

    @Test
    public void testTailMapWithSingleArgument() {
        SortedMap<UnsignedInteger, String> tail = testMap.tailMap(uintArray[900]);
        assertEquals(tail.size(), (uintArray.length - 900), "Returned map of incorrect size : " + tail.size());
        for (int i = 900; i < objArray.length; i++) {
            assertTrue(tail.containsValue(objArray[i]), "Map contains incorrect entries");
        }

        SortedMap<UnsignedInteger, Integer> intMap;

        int size = 16;
        intMap = new SplayMap<Integer>();

        for (int i = 0; i < size; i++) {
            intMap.put(UnsignedInteger.valueOf(i), i);
        }

        final SortedMap<UnsignedInteger, Integer> sub = intMap.tailMap(UnsignedInteger.valueOf(size));
        assertEquals(sub.size(),0, "size should be zero");
        assertTrue(sub.isEmpty(), "SubMap should be empty");

        assertThrows(NoSuchElementException.class, () -> sub.firstKey());
        assertThrows(NullPointerException.class, () -> new SplayMap<String>().tailMap(null));
        assertThrows(NoSuchElementException.class, () -> sub.lastKey());

        // Try with larger more complex tree structure.

        size = 256;
        intMap = new SplayMap<Integer>();
        for (int i = 0; i < size; i++) {
            intMap.put(UnsignedInteger.valueOf(i), i);
        }
        final SortedMap<UnsignedInteger, Integer> sub1 = intMap.tailMap(UnsignedInteger.valueOf(size));

        assertThrows(NoSuchElementException.class, () -> sub1.firstKey());
        assertThrows(NullPointerException.class, () -> new SplayMap<String>().tailMap(null));
        assertThrows(NoSuchElementException.class, () -> sub1.lastKey());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testNavigableKeySetOperationInteractions() throws Exception {
        UnsignedInteger testint9999 = UnsignedInteger.valueOf(9999);
        UnsignedInteger testint10000 = UnsignedInteger.valueOf(10000);
        UnsignedInteger testint100 = UnsignedInteger.valueOf(100);
        UnsignedInteger testint0 = UnsignedInteger.valueOf(0);

        final NavigableSet untypedSet = testMap.navigableKeySet();
        final NavigableSet<UnsignedInteger> set = testMap.navigableKeySet();

        assertFalse(set.contains(testint9999));
        testMap.put(testint9999, testint9999.toString());
        assertTrue(set.contains(testint9999));
        testMap.remove(testint9999);
        assertFalse(set.contains(testint9999));

        assertThrows(UnsupportedOperationException.class, () -> untypedSet.add(new Object()));
        assertThrows(UnsupportedOperationException.class, () -> untypedSet.add(null));
        assertThrows(NullPointerException.class, () -> untypedSet.addAll(null));

        final Collection collection = new ArrayList();
        set.addAll(collection);
        try {
            collection.add(new Object());
            set.addAll(collection);
            fail("should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        set.remove(testint100);
        assertFalse(testMap.containsKey(testint100));
        assertTrue(testMap.containsKey(testint0));

        final Iterator iter = set.iterator();
        iter.next();
        iter.remove();
        assertFalse(testMap.containsKey(testint0));
        collection.add(UnsignedInteger.valueOf(2));
        set.retainAll(collection);
        assertEquals(1, testMap.size());
        set.removeAll(collection);
        assertEquals(0, testMap.size());
        testMap.put(testint10000, testint10000.toString());
        assertEquals(1, testMap.size());
        set.clear();
        assertEquals(0, testMap.size());
    }

    @Test
    public void testEmptySubMap() throws Exception {
        SplayMap<List<Integer>> tm = new SplayMap<>();
        SortedMap<UnsignedInteger, List<Integer>> sm = tm.tailMap(UnsignedInteger.valueOf(1));
        assertTrue(sm.values().size() == 0);

        NavigableMap<UnsignedInteger, List<Integer>> sm1 = tm.descendingMap();
        assertTrue(sm1.values().size() == 0);

        NavigableMap<UnsignedInteger, List<Integer>> sm2 = sm1.tailMap(UnsignedInteger.valueOf(1), true);
        assertTrue(sm2.values().size() == 0);
    }

    @Test
    public void testValuesOneEntrySubMap() {
        SplayMap<String> tm = new SplayMap<String>();
        tm.put(1, "VAL001");
        tm.put(3, "VAL003");
        tm.put(2, "VAL002");

        UnsignedInteger firstKey = tm.firstKey();
        SortedMap<UnsignedInteger, String> subMap = tm.subMap(firstKey, firstKey);
        Iterator<String> iter = subMap.values().iterator();
        assertNotNull(iter);
    }

    @Test
    public void testDescendingMapSubMap() throws Exception {
        SplayMap<Object> tm = new SplayMap<>();
        for (int i = 0; i < 10; ++i) {
            tm.put(i, new Object());
        }

        NavigableMap<UnsignedInteger, Object> descMap = tm.descendingMap();
        assertEquals(7, descMap.subMap(UnsignedInteger.valueOf(8), true, UnsignedInteger.valueOf(1), false).size());
        assertEquals(4, descMap.headMap(UnsignedInteger.valueOf(6), true).size());
        assertEquals(2, descMap.tailMap(UnsignedInteger.valueOf(2), false).size());

        // sub map of sub map of descendingMap
        NavigableMap<UnsignedInteger, Object> mapUIntObj = new SplayMap<Object>();
        for (int i = 0; i < 10; ++i) {
            mapUIntObj.put(UnsignedInteger.valueOf(i), new Object());
        }
        mapUIntObj = mapUIntObj.descendingMap();
        NavigableMap<UnsignedInteger, Object> subMapUIntObj =
            mapUIntObj.subMap(UnsignedInteger.valueOf(9), true, UnsignedInteger.valueOf(5), false);

        assertEquals(4, subMapUIntObj.size());
        subMapUIntObj = subMapUIntObj.subMap(UnsignedInteger.valueOf(9), true, UnsignedInteger.valueOf(5), false);
        assertEquals(4, subMapUIntObj.size());
        subMapUIntObj = subMapUIntObj.subMap(UnsignedInteger.valueOf(6), false, UnsignedInteger.valueOf(5), false);
        assertEquals(0, subMapUIntObj.size());

        subMapUIntObj = mapUIntObj.headMap(UnsignedInteger.valueOf(5), false);
        assertEquals(4, subMapUIntObj.size());
        subMapUIntObj = subMapUIntObj.headMap(UnsignedInteger.valueOf(5), false);
        assertEquals(4, subMapUIntObj.size());
        subMapUIntObj = subMapUIntObj.tailMap(UnsignedInteger.valueOf(5), false);
        assertEquals(0, subMapUIntObj.size());

        subMapUIntObj = mapUIntObj.tailMap(UnsignedInteger.valueOf(5), false);
        assertEquals(5, subMapUIntObj.size());
        subMapUIntObj = subMapUIntObj.tailMap(UnsignedInteger.valueOf(5), false);
        assertEquals(5, subMapUIntObj.size());
        subMapUIntObj = subMapUIntObj.headMap(UnsignedInteger.valueOf(5), false);
        assertEquals(0, subMapUIntObj.size());
    }

    @Test
    public void testEqualsJDKMapTypes() throws Exception {
        // comparing SplayMap with different object types
        Map<UnsignedInteger, String> m1 = new SplayMap<>();
        Map<UnsignedInteger, String> m2 = new SplayMap<>();

        m1.put(UnsignedInteger.valueOf(1), "val1");
        m1.put(UnsignedInteger.valueOf(2), "val2");
        m2.put(UnsignedInteger.valueOf(3), "val1");
        m2.put(UnsignedInteger.valueOf(4), "val2");

        assertNotEquals(m1, m2, "Maps should not be equal 1");
        assertNotEquals(m2, m1, "Maps should not be equal 2");

        // comparing SplayMap with HashMap with equal values
        m1 = new SplayMap<>();
        m2 = new HashMap<>();
        m1.put(UnsignedInteger.valueOf(1), "val");
        m2.put(UnsignedInteger.valueOf(2), "val");
        assertNotEquals(m1, m2, "Maps should not be equal 3");
        assertNotEquals(m2, m1, "Maps should not be equal 4");

        // comparing SplayMap with differing objects inside values
        m1 = new SplayMap<>();
        m2 = new SplayMap<>();
        m1.put(UnsignedInteger.valueOf(1), "val1");
        m2.put(UnsignedInteger.valueOf(1), "val2");
        assertNotEquals(m1, m2, "Maps should not be equal 5");
        assertNotEquals(m2, m1, "Maps should not be equal 6");

        // comparing SplayMap with same objects inside values
        m1 = new SplayMap<>();
        m2 = new SplayMap<>();
        m1.put(UnsignedInteger.valueOf(1), "val1");
        m2.put(UnsignedInteger.valueOf(1), "val1");
        assertTrue(m1.equals(m2), "Maps should be equal 7");
        assertTrue(m2.equals(m1), "Maps should be equal 7");
    }

    @Test
    public void testEntrySetContains() throws Exception {
        SplayMap<String> first = new SplayMap<>();
        SplayMap<String> second = new SplayMap<>();

        first.put(UnsignedInteger.valueOf(1), "one");
        Object[] entry = first.entrySet().toArray();
        assertFalse(second.entrySet().contains(entry[0]),
                    "Empty map should not contain anything from first map");

        Map<UnsignedInteger, String> submap = second.subMap(UnsignedInteger.valueOf(0), UnsignedInteger.valueOf(1));
        entry = first.entrySet().toArray();
        assertFalse(submap.entrySet().contains(entry[0]),
                    "Empty SubMap should not contain the first map's entry");

        second.put(UnsignedInteger.valueOf(1), "one");
        assertTrue(second.entrySet().containsAll(first.entrySet()),
                   "entrySet().containsAll(...) should work with values");

        first.clear();
        first.put(UnsignedInteger.valueOf(1), "two");
        entry = first.entrySet().toArray();
        assertFalse(second.entrySet().contains(entry[0]),
                    "new valued entry should not equal old valued entry");
    }

    @Test
    public void testValues() {
        Collection<String> vals = testMap.values();
        vals.iterator();
        assertEquals(vals.size(), objArray.length, "Returned collection of incorrect size");
        for (String element : objArray) {
            assertTrue(vals.contains(element), "Collection contains incorrect elements");
        }

        assertEquals(1000, vals.size());
        int j = 0;
        for (Iterator<String> iter = vals.iterator(); iter.hasNext(); j++) {
            String element = iter.next();
            assertNotNull(element);
        }
        assertEquals(1000, j);

        vals = testMap.descendingMap().values();
        assertNotNull(vals.iterator());
        assertEquals(vals.size(), objArray.length, "Returned collection of incorrect size");
        for (String element : objArray) {
            assertTrue(vals.contains(element), "Collection contains incorrect elements");
        }
        assertEquals(1000, vals.size());
        j = 0;
        for (Iterator<String> iter = vals.iterator(); iter.hasNext(); j++) {
            String element = iter.next();
            assertNotNull(element);
        }
        assertEquals(1000, j);

        SplayMap<String> myMap = new SplayMap<String>();
        for (int i = 0; i < 100; i++) {
            myMap.put(uintArray[i], objArray[i]);
        }
        Collection<String> values = myMap.values();
        values.remove(UnsignedInteger.ZERO.toString());
        assertTrue(!myMap.containsKey(UnsignedInteger.ZERO), "Removing from the values collection should remove from the original map");
        assertTrue(!myMap.containsValue(UnsignedInteger.ZERO.toString()), "Removing from the values collection should remove from the original map");
        assertEquals(99, values.size());
        j = 0;
        for (Iterator<String> iter = values.iterator(); iter.hasNext(); j++) {
            iter.next();
        }
        assertEquals(99, j);
    }

    @Test
    public void testSubMapValuesSizeMetrics() {
        SplayMap<String> myMap = new SplayMap<>();
        for (int i = 0; i < 1000; i++) {
            myMap.put(i, objArray[i]);
        }

        // Test for method values() in subMaps
        Collection<String> vals = myMap.subMap(UnsignedInteger.valueOf(200), UnsignedInteger.valueOf(400)).values();
        assertEquals(200, vals.size(), "Returned collection of incorrect size");
        for (int i = 200; i < 400; i++) {
            assertTrue(vals.contains(objArray[i]), "Collection contains incorrect elements" + i);
        }
        assertEquals(200, vals.toArray().length);
        vals.remove(objArray[300]);
        assertTrue(!myMap.containsValue(objArray[300]),
            "Removing from the values collection should remove from the original map");
        assertTrue(vals.size() == 199, "Returned collection of incorrect size");
        assertEquals(199, vals.toArray().length);

        myMap.put(300, objArray[300]);
        // Test for method values() in subMaps
        vals = myMap.headMap(UnsignedInteger.valueOf(400)).values();
        assertEquals(vals.size(), 400, "Returned collection of incorrect size");
        for (int i = 0; i < 400; i++) {
            assertTrue(vals.contains(objArray[i]), "Collection contains incorrect elements " + i);
        }
        assertEquals(400,vals.toArray().length);
        vals.remove(objArray[300]);
        assertTrue(!myMap.containsValue(objArray[300]), "Removing from the values collection should remove from the original map");
        assertEquals(vals.size(), 399, "Returned collection of incorrect size");
        assertEquals(399, vals.toArray().length);

        myMap.put(300, objArray[300]);
        // Test for method values() in subMaps
        vals = myMap.tailMap(UnsignedInteger.valueOf(400)).values();
        assertEquals(vals.size(), 600, "Returned collection of incorrect size");
        for (int i = 400; i < 1000; i++) {
            assertTrue(vals.contains(objArray[i]), "Collection contains incorrect elements " + i);
        }
        assertEquals(600, vals.toArray().length);
        vals.remove(objArray[600]);
        assertTrue(!myMap.containsValue(objArray[600]), "Removing from the values collection should remove from the original map");
        assertEquals(vals.size(), 599, "Returned collection of incorrect size");
        assertEquals(599,vals.toArray().length);

        myMap.put(600, objArray[600]);
        // Test for method values() in subMaps
        vals = myMap.descendingMap().headMap(UnsignedInteger.valueOf(400)).values();
        assertEquals(vals.size(), 599, "Returned collection of incorrect size");
        for (int i = 401; i < 1000; i++) {
            assertTrue(vals.contains(objArray[i]), "Collection contains incorrect elements " + i);
        }
        assertEquals(599,vals.toArray().length);
        vals.remove(objArray[600]);
        assertTrue(!myMap.containsValue(objArray[600]), "Removing from the values collection should remove from the original map");
        assertEquals(vals.size(), 598, "Returned collection of incorrect size");
        assertEquals(598,vals.toArray().length);

        myMap.put(600, objArray[600]);
        // Test for method values() in subMaps
        vals = myMap.descendingMap().tailMap(UnsignedInteger.valueOf(400)).values();
        assertEquals(vals.size(), 401, "Returned collection of incorrect size");
        for (int i = 0; i <= 400; i++) {
            assertTrue(vals.contains(objArray[i]), "Collection contains incorrect elements " + i);
        }
        assertEquals(401, vals.toArray().length);
        vals.remove(objArray[300]);
        assertTrue(!myMap.containsValue(objArray[300]), "Removing from the values collection should remove from the original map");
        assertEquals(vals.size(), 400, "Returned collection of incorrect size");
        assertEquals(400,vals.toArray().length);
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
