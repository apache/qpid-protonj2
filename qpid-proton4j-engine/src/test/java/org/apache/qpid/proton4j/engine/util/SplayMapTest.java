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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.junit.Test;

/**
 * Test SplayMap type
 */
public class SplayMapTest {

    @Test
    public void testClear() {
        SplayMap<String> map = new SplayMap<>();

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
        SplayMap<String> map = new SplayMap<>();

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
        SplayMap<String> map = new SplayMap<>();

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
    public void testInsertAndReplace() {
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "foo");
        assertEquals("foo", map.put(2, "two"));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsKey() {
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertTrue(map.containsKey(0));
        assertFalse(map.containsKey(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsValue() {
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertTrue(map.containsValue("zero"));
        assertFalse(map.containsValue("four"));

        assertEquals(3, map.size());
    }

    @Test
    public void testRemove() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testValuesCollection() {
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "one");
        map.put(3, "one");

        Collection<String> values = map.values();
        assertNotNull(values);
        assertEquals(4, values.size());
        assertFalse(values.isEmpty());
    }

    @Test
    public void testValuesIteration() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testValuesIterationFollowUnsignedOrderingExpectations() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testValuesIterationOnEmptyTree() {
        SplayMap<String> map = new SplayMap<>();
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
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        Set<UnsignedInteger> keys = map.keySet();
        assertNotNull(keys);
        assertEquals(4, keys.size());
        assertFalse(keys.isEmpty());
    }

    @Test
    public void testKeysIteration() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testKeysIterationFollowsUnsignedOrderingExpectations() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testKeysIterationOnEmptyTree() {
        SplayMap<String> map = new SplayMap<>();
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
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        Set<Entry<UnsignedInteger, String>> entries= map.entrySet();
        assertNotNull(entries);
        assertEquals(4, entries.size());
        assertFalse(entries.isEmpty());
    }

    @Test
    public void testEntryIteration() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testEntryIterationFollowsUnsignedOrderingExpectations() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testEntrySetIterationOnEmptyTree() {
        SplayMap<String> map = new SplayMap<>();
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
    public void testFirstKey() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testFirstEntry() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testPollFirstEntry() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testLastKey() {
        SplayMap<String> map = new SplayMap<>();

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
    public void testLastEntry() {
        SplayMap<String> map = new SplayMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {-1, -2, 3, 2, 1, 0};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.lastEntry().getPrimitiveKey());
            map.remove(expected);
        }

        assertNull(map.lastKey());
    }

    @Test
    public void testPollLastEntry() {
        SplayMap<String> map = new SplayMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {-1, -2, 3, 2, 1, 0};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        for (int expected : expectedOrder) {
            assertEquals(expected, map.pollLastEntry().getPrimitiveKey());
        }

        assertNull(map.lastKey());
    }
}
