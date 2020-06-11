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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.logging.ProtonLogger;
import org.apache.qpid.proton4j.logging.ProtonLoggerFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the functionality of the {@link SequenceNumberMap}
 */
public class SequenceNumberMapTest {

    protected static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SequenceNumberMapTest.class);

    protected long seed;
    protected Random random;

    @Before
    public void setUp() {
        seed = System.currentTimeMillis();
        random = new Random();
        random.setSeed(seed);
    }

    @Test
    public void testCreateDefaultMap() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertNull(map.get(1));
    }

    @Test
    public void testClear() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testPutIntoDifferentBucjetsDoesNotThrow() {
        final int BUCKET_SIZE = 16;

        SequenceNumberMap<String> map = new SequenceNumberMap<>(BUCKET_SIZE);

        for (int i = 0; i < BUCKET_SIZE * BUCKET_SIZE; i += BUCKET_SIZE) {
            map.put(i, String.valueOf(i));
        }

        assertEquals(BUCKET_SIZE, map.size());
    }

    @Test
    public void testPutUnsignedIntegerIntoDifferentBucjetsDoesNotThrow() {
        final int BUCKET_SIZE = 16;

        SequenceNumberMap<String> map = new SequenceNumberMap<>(BUCKET_SIZE);

        for (int i = 0; i < BUCKET_SIZE * BUCKET_SIZE; i += BUCKET_SIZE) {
            map.put(UnsignedInteger.valueOf(i), String.valueOf(i));
        }

        assertEquals(BUCKET_SIZE, map.size());
    }

    @Test
    public void testNonSequentialPut() {
        final int[] INPUTS = { 569309746, -1316559945, 524283256 };

        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        for (int i = 0; i < INPUTS.length; ++i) {
            map.put(INPUTS[i], String.valueOf(INPUTS[i]));
        }

        for (int i = 0; i < INPUTS.length; ++i) {
            assertEquals(String.valueOf(INPUTS[i]), map.get(INPUTS[i]));
        }
    }

    @Test
    public void testInsertAndReplace() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testGetWhenEmpty() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        assertNull(map.get(0));
    }

    @Test
    public void testGet() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(1024, "1k");
        map.put(-3, "-three");
        map.put(-65535, "-65535");

        assertEquals(5, map.size());

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("1k", map.get(1024));
        assertEquals("-three", map.get(-3));
        assertEquals("-65535", map.get(-65535));

        assertNull(map.get(3));

        assertEquals(5, map.size());
    }

    @Test
    public void testGetWithUnsignedInteger() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(1024, "1k");
        map.put(-3, "-three");
        map.put(-65535, "-65535");

        assertEquals(5, map.size());

        assertEquals("zero", map.get(UnsignedInteger.valueOf(0)));
        assertEquals("one", map.get(UnsignedInteger.valueOf(1)));
        assertEquals("1k", map.get(UnsignedInteger.valueOf(1024)));
        assertEquals("-three", map.get(UnsignedInteger.valueOf(-3)));
        assertEquals("-65535", map.get(UnsignedInteger.valueOf(-65535)));

        assertNull(map.get(3));

        assertEquals(5, map.size());
    }

    @Test
    public void testContainsKeyOnEmptyMap() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        assertFalse(map.containsKey(0));
        assertFalse(map.containsKey(UnsignedInteger.ZERO));
    }

    @Test
    public void testContainsKey() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertTrue(map.containsKey(0));
        assertFalse(map.containsKey(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsKeyUnsignedInteger() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(UnsignedInteger.valueOf(0), "zero");
        map.put(UnsignedInteger.valueOf(1), "one");
        map.put(UnsignedInteger.valueOf(-3), "-three");

        assertTrue(map.containsKey(0));
        assertFalse(map.containsKey(3));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsValue() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(-3, "-three");

        assertTrue(map.containsValue("zero"));
        assertFalse(map.containsValue("four"));

        assertEquals(3, map.size());
    }

    @Test
    public void testContainsValueOnEmptyMap() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        assertFalse(map.containsValue("0"));
    }

    @Test
    public void testRemove() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(9, "nine");
        map.put(7, "seven");
        map.put(-1, "minus one");

        assertNull(map.remove(5));
    }

    @Test
    public void testRemoveFirstEntryTwice() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>(16);

        map.put(0, "zero");
        map.put(16, "sixteen");

        assertNotNull(map.remove(0));
        assertNull(map.remove(0));
    }

    @Test
    public void testRemoveWithInvalidType() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        map.put(0, "zero");

        try {
            map.remove("foo");
            fail("Should not accept incompatible types");
        } catch (ClassCastException ccex) {}
    }

    @Test
    public void testRemoveUnsignedInteger() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testRemoveInteger() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testValuesCollection() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testValuesIterationFollowInsertionOrderExpectations() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {3, 0, -1, 1, -2, 2};

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();
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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testKeysIteration() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testKeysIterationFollowsInsertionOrderExpectations() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {3, 0, -1, 1, -2, 2};

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();
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
    public void testKeysIterationRemove() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void tesEntrySetReturned() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testEntryIteration() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
    public void testEntryIterationFollowsInsertionOrderExpectations() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {3, 0, -1, 1, -2, 2};

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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
        SequenceNumberMap<String> map = new SequenceNumberMap<>();
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
    public void testForEach() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {3, 0, -1, 1, -2, 2};

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
    public void testForEachOverExtendedSparseRange() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>(16);

        final int[] inputValues = {0, 1, 32, 34, 38, 65, 66, 99, 100, 1111, 1112, 65535, 65536, 128569, 128570, 2553560};

        for (int entry : inputValues) {
            map.put(entry, "" + entry);
        }

        final SequenceNumber index = new SequenceNumber(0);
        map.forEach((k, v) -> {
            int value = index.getAndIncrement().intValue();
            assertEquals(inputValues[value], k.intValue());
        });

        assertEquals(index.intValue(), inputValues.length);

        assertEquals(inputValues.length, map.size());
    }

    @Test
    public void testForEachEntry() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int[] inputValues = {3, 0, -1, 1, -2, 2};
        final int[] expectedOrder = {3, 0, -1, 1, -2, 2};

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
    public void testRandomValuePutThenGetThenRemove() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int INSERTIONS = 128;

        try {
            for (int i = 0; i < INSERTIONS; ++i) {
                final int key = random.nextInt();
                map.put(key, String.valueOf(key));
            }

            random.setSeed(seed);

            for (int i = 0; i < INSERTIONS; ++i) {
                final int key = random.nextInt();
                assertEquals(String.valueOf(key), map.get(key));
            }

            random.setSeed(seed);

            for (int i = 0; i < INSERTIONS; ++i) {
                final int key = random.nextInt();
                assertEquals(String.valueOf(key), map.remove(key));
            }

            assertTrue(map.isEmpty());
            assertEquals(0, map.size());
        } catch (AssertionError error) {
            dumpRandomDataSet(INSERTIONS, false);
            throw error;
        }
    }

    @Test
    public void testRandomPutAndThenValuesIteration() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int INSERTIONS = 128;

        try {
            for (int i = 0; i < INSERTIONS; ++i) {
                final int key = random.nextInt();
                map.put(key, String.valueOf(key));
            }

            random.setSeed(seed);

            Collection<String> values = map.values();
            for (String value : values) {
                assertEquals(String.valueOf(random.nextInt()), value);
            }

            map.clear();

            assertTrue(map.isEmpty());
            assertEquals(0, map.size());
        } catch (AssertionError error) {
            dumpRandomDataSet(INSERTIONS, false);
            throw error;
        }
    }

    @Test
    public void testRandomPutAndThenKeysIteration() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int INSERTIONS = 128;

        try {
            for (int i = 0; i < INSERTIONS; ++i) {
                final int key = random.nextInt();
                map.put(key, String.valueOf(key));
            }

            random.setSeed(seed);

            Collection<UnsignedInteger> keys = map.keySet();
            for (UnsignedInteger key : keys) {
                assertEquals(UnsignedInteger.valueOf(random.nextInt()), key);
            }

            map.clear();

            assertTrue(map.isEmpty());
            assertEquals(0, map.size());
        } catch (AssertionError error) {
            dumpRandomDataSet(INSERTIONS, false);
            throw error;
        }
    }

    @Test
    public void testRandomPutAndThenEntriesIteration() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int INSERTIONS = 128;

        try {
            for (int i = 0; i < INSERTIONS; ++i) {
                final int key = random.nextInt();
                map.put(key, String.valueOf(key));
            }

            random.setSeed(seed);

            Collection<Map.Entry<UnsignedInteger, String>> entries = map.entrySet();
            for (Map.Entry<UnsignedInteger, String> entry : entries) {
                final int nextValue = random.nextInt();
                assertEquals(UnsignedInteger.valueOf(nextValue), entry.getKey());
                assertEquals(String.valueOf(nextValue), entry.getValue());
            }

            map.clear();

            assertTrue(map.isEmpty());
            assertEquals(0, map.size());
        } catch (AssertionError error) {
            dumpRandomDataSet(INSERTIONS, false);
            throw error;
        }
    }

    @Test
    public void testRandomProduceAndConsumeWithBacklog() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int INSERTIONS = 8192;
        final String DUMMY_STRING = "test";

        for (int i = 0; i < INSERTIONS; ++i) {
            map.put(UnsignedInteger.valueOf(i), DUMMY_STRING);
        }

        for (int i = 0; i < INSERTIONS; ++i) {
            int p = random.nextInt(INSERTIONS);
            int c = random.nextInt(INSERTIONS);

            map.put(UnsignedInteger.valueOf(p), DUMMY_STRING);
            map.remove(UnsignedInteger.valueOf(c));
        }
    }

    @Test
    public void testRandomPutAndGetIntoEmptyMap() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

        final int INSERTIONS = 8192;
        final String DUMMY_STRING = "test";

        for (int i = 0; i < INSERTIONS; ++i) {
            int p = random.nextInt(INSERTIONS);
            int c = random.nextInt(INSERTIONS);

            map.put(UnsignedInteger.valueOf(p), DUMMY_STRING);
            map.remove(UnsignedInteger.valueOf(c));
        }
    }

    @Test
    public void testPutRandomValueIntoMapThenRemoveInSameOrder() {
        SequenceNumberMap<String> map = new SequenceNumberMap<>();

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

    private void dumpRandomDataSet(int iterations, boolean bounded) {
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
}
