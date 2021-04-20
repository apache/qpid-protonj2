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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test LinkedSplayMap type
 */
public class LinkedSplayMapTest extends SplayMapTest {

    protected static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SplayMapTest.class);

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
    }

    @Override
    protected <E> LinkedSplayMap<E> createMap() {
        return new LinkedSplayMap<>();
    }

    /**
     * Test differs from parent as order is insertion based.
     */
    @Override
    @Test
    public void testValuesIterationFollowUnsignedOrderingExpectations() {
        LinkedSplayMap<String> map = createMap();

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

    /**
     * Test differs from parent as order is insertion based.
     */
    @Override
    @Test
    public void testKeysIterationFollowsUnsignedOrderingExpectations() {
        LinkedSplayMap<String> map = createMap();

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

    /**
     * Test differs from parent as order is insertion based.
     */
    @Override
    @Test
    public void testEntryIterationFollowsUnsignedOrderingExpectations() {
        LinkedSplayMap<String> map = createMap();

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

    /**
     * Test differs from parent as order is insertion based.
     */
    @Override
    @Test
    public void testForEach() {
        LinkedSplayMap<String> map = createMap();

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

    /**
     * Test differs from parent as order is insertion based.
     */
    @Override
    @Test
    public void testForEachEntry() {
        LinkedSplayMap<String> map = createMap();

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
}
