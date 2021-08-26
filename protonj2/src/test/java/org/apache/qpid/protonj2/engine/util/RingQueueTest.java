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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RingQueueTest {

    protected long seed;
    protected Random random;

    @BeforeEach
    public void setUp() {
        seed = System.currentTimeMillis();
        random = new Random();
        random.setSeed(seed);
    }

    @Test
    public void testCreate() {
        Queue<String> testQ = new RingQueue<>(16);

        assertTrue(testQ.isEmpty());
        assertEquals(0, testQ.size());
        assertNull(testQ.peek());
    }

    @Test
    public void testOffer() {
        Queue<String> testQ = new RingQueue<>(3);

        testQ.offer("1");
        testQ.offer("2");
        testQ.offer("3");

        assertFalse(testQ.isEmpty());
        assertEquals(3, testQ.size());
        assertNotNull(testQ.peek());
        assertEquals("1", testQ.peek());
    }

    @Test
    public void testRemoveAll() {
        Queue<String> testQ = new RingQueue<>(3);
        Queue<String> inputQ = new ArrayDeque<>(Arrays.asList("1", "2", "3"));
        assertThrows(UnsupportedOperationException.class, () -> testQ.removeAll(inputQ));
    }

    @Test
    public void testRetainAll() {
        Queue<String> testQ = new RingQueue<>(3);
        Queue<String> inputQ = new ArrayDeque<>(Arrays.asList("1", "2", "3"));
        assertThrows(UnsupportedOperationException.class, () -> testQ.retainAll(inputQ));
    }

    @Test
    @SuppressWarnings({"CollectionAddedToSelf", "ModifyingCollectionWithItself"})
    public void testAddAllWithSelf() {
        Queue<String> testQ = new RingQueue<>(3);
        assertThrows(IllegalArgumentException.class, () -> testQ.addAll(testQ));
    }

    @Test
    public void testAddAllFromLargerCollection() {
        Queue<String> testQ = new RingQueue<>(2);
        Queue<String> inputQ = new ArrayDeque<>(Arrays.asList("1", "2", "3"));

        assertThrows(IllegalStateException.class, () -> testQ.addAll(inputQ));
    }

    @Test
    public void testAddAllFromSmallerCollection() {
        Queue<String> testQ = new RingQueue<>(4);
        Queue<String> inputQ = new ArrayDeque<>(Arrays.asList("1", "2", "3"));

        assertTrue(testQ.addAll(inputQ));

        assertEquals(inputQ.size(), testQ.size());
        inputQ.forEach((element) -> {
            assertEquals(element, testQ.poll());
        });
    }

    @Test
    public void testAddAllFromSameSizeCollection() {
        Queue<String> testQ = new RingQueue<>(3);
        Queue<String> inputQ = new ArrayDeque<>(Arrays.asList("1", "2", "3"));

        assertTrue(testQ.addAll(inputQ));

        assertEquals(inputQ.size(), testQ.size());
        inputQ.forEach((element) -> {
            assertEquals(element, testQ.poll());
        });
    }

    @Test
    public void testAddAllFromEmptyCollection() {
        Queue<String> testQ = new RingQueue<>(3);
        Queue<String> inputQ = new ArrayDeque<>();

        assertFalse(testQ.addAll(inputQ));
    }

    @Test
    public void testRemove() {
        Queue<String> testQ = new RingQueue<>(3);

        testQ.offer("1");
        testQ.offer("2");
        testQ.offer("3");

        assertThrows(UnsupportedOperationException.class, () -> testQ.remove("1"));
    }

    @Test
    public void testPoll() {
        Queue<String> testQ = new RingQueue<>(3);

        testQ.offer("1");
        testQ.offer("2");
        testQ.offer("3");

        assertFalse(testQ.isEmpty());
        assertEquals(3, testQ.size());
        assertNotNull(testQ.peek());
        assertEquals("1", testQ.peek());

        assertEquals("1", testQ.poll());
        assertEquals("2", testQ.poll());
        assertEquals("3", testQ.poll());

        assertTrue(testQ.isEmpty());
        assertEquals(0, testQ.size());
        assertNull(testQ.peek());
    }

    @Test
    public void testOfferAfterDequeueFromFull() {
        Queue<String> testQ = new RingQueue<>(3);

        testQ.offer("1");
        testQ.offer("2");
        testQ.offer("3");

        assertFalse(testQ.isEmpty());
        assertEquals(3, testQ.size());
        assertNotNull(testQ.peek());
        assertEquals("1", testQ.peek());
        assertEquals("1", testQ.poll());
        assertFalse(testQ.isEmpty());
        assertEquals(2, testQ.size());
        assertNotNull(testQ.peek());
        assertEquals("2", testQ.peek());

        testQ.offer("4");

        assertEquals("2", testQ.poll());
        assertEquals("3", testQ.poll());
        assertEquals("4", testQ.poll());

        assertTrue(testQ.isEmpty());
        assertEquals(0, testQ.size());
        assertNull(testQ.peek());
    }

    @Test
    public void testIterateOverFullQueue() {
        Queue<String> testQ = new RingQueue<>(3);

        Queue<String> inputQ = new ArrayDeque<>(Arrays.asList("1", "2", "3"));

        inputQ.forEach(value -> testQ.offer(value));

        assertFalse(testQ.isEmpty());
        assertEquals(3, testQ.size());
        assertNotNull(testQ.peek());

        Iterator<String> iter = testQ.iterator();
        assertTrue(iter.hasNext());

        while (iter.hasNext()) {
            String next = iter.next();
            assertEquals(inputQ.poll(), next);
        }
    }

    @Test
    public void testContains() {
        final int COUNT = 100;
        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.contains("" + random.nextInt()));
        }

        assertFalse(testQ.contains("this-string"));
        assertFalse(testQ.contains(null));
    }

    @Test
    public void testContainsNullElement() {
        Queue<String> testQ = new RingQueue<>(10);

        testQ.offer("1");
        testQ.offer(null);
        testQ.offer("2");
        testQ.offer(null);
        testQ.offer("3");

        assertTrue(testQ.contains("1"));
        assertTrue(testQ.contains(null));

        assertEquals("1", testQ.poll());
        assertEquals(null, testQ.poll());

        assertTrue(testQ.contains("2"));
        assertTrue(testQ.contains(null));
    }

    @Test
    public void testIterateOverQueue() {
        final int COUNT = 100;
        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        testQ.forEach(entry -> assertEquals(entry, String.valueOf(random.nextInt())));

        random.setSeed(seed);  // Reset

        for (int i = 0; i < COUNT / 2; ++i) {
            assertEquals(String.valueOf(random.nextInt()), testQ.poll());
        }

        testQ.forEach(entry -> assertEquals(entry, String.valueOf(random.nextInt())));
    }

    @Test
    public void testIterateOverQueueFilledViaCollection() {
        final int COUNT = 100;
        Queue<String> inputQ = new ArrayDeque<>();

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(inputQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        Queue<String> testQ = new RingQueue<>(inputQ);

        testQ.forEach(entry -> assertEquals(entry, String.valueOf(random.nextInt())));

        random.setSeed(seed);  // Reset

        for (int i = 0; i < COUNT / 2; ++i) {
            assertEquals(String.valueOf(random.nextInt()), testQ.poll());
        }

        testQ.forEach(entry -> assertEquals(entry, String.valueOf(random.nextInt())));
    }

    @Test
    public void testOfferPollAndOffer() {
        final int ITERATIONS = 10;
        final int COUNT = 100;

        final List<String> dataSet = new ArrayList<>(COUNT);
        for (int i = 0; i < COUNT; ++i) {
            dataSet.add("" + random.nextInt());
        }

        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
            testQ.clear();

            for (int i = 0; i < COUNT; ++i) {
                assertTrue(testQ.offer(dataSet.get(i)));
            }

            assertFalse(testQ.isEmpty());

            for (int i = 0; i < COUNT; ++i) {
                assertNotNull(testQ.poll());
            }

            assertTrue(testQ.isEmpty());

            for (int i = 0; i < COUNT; ++i) {
                assertTrue(testQ.offer(dataSet.get(i)));
            }

            assertFalse(testQ.isEmpty());

            for (int i = 0; i < COUNT; ++i) {
                assertTrue(testQ.contains(dataSet.get(0)));
            }
        }
    }

    @Test
    public void testIterateOverQueueThrowsNoSuchElementIfMovedToFar() {
        final int COUNT = 100;
        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        Iterator<String> iterator = testQ.iterator();
        while (iterator.hasNext()) {
            assertEquals(String.valueOf(random.nextInt()), iterator.next());
        }

        assertThrows(NoSuchElementException.class, () -> iterator.next());
    }

    @Test
    public void testIteratorThrowsIfModifiedConcurrently() {
        final int COUNT = 100;
        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        Iterator<String> iterator = testQ.iterator();
        assertEquals(testQ.poll(), "" + random.nextInt());
        assertThrows(ConcurrentModificationException.class, () -> iterator.next());
    }

    @Test
    public void testIteratorThrowsIfModifiedConcurrentlySizeUnchanged() {
        final int COUNT = 100;
        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        Iterator<String> iterator = testQ.iterator();
        assertEquals(testQ.poll(), "" + random.nextInt());
        assertTrue(testQ.offer("" + random.nextInt()));
        assertThrows(ConcurrentModificationException.class, () -> iterator.next());
    }

    @Test
    public void testIteratorDoesNotSupportRemove() {
        final int COUNT = 100;
        Queue<String> testQ = new RingQueue<>(COUNT);

        for (int i = 0; i < COUNT; ++i) {
            assertTrue(testQ.offer("" + random.nextInt()));
        }

        random.setSeed(seed);  // Reset

        Iterator<String> iterator = testQ.iterator();
        assertTrue(iterator.hasNext());
        assertThrows(UnsupportedOperationException.class, () -> iterator.remove());
    }
}
