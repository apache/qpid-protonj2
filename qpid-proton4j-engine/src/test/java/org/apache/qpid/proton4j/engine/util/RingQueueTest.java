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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class RingQueueTest {

    protected long seed;
    protected Random random;

    @Before
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
    }
}
