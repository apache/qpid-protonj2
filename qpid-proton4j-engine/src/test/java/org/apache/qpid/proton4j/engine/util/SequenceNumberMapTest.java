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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.junit.Test;

/**
 * Test the functionality of the {@link SequenceNumberMap}
 */
public class SequenceNumberMapTest {

    @Test
    public void testComparator() {
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
    public void testInsertAndReplace() {
        SplayMap<String> map = new SplayMap<>();

        map.put(0, "zero");
        map.put(1, "one");
        map.put(2, "foo");
        assertEquals("foo", map.put(2, "two"));

        assertEquals("zero", map.get(0));
        assertEquals("one", map.get(1));
        assertEquals("two", map.get(2));

        assertEquals(3, map.size());
    }
}
