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
package org.apache.qpid.protonj2.client.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.protonj2.client.ReconnectLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RecoonectLocationPoolTest {

    private List<ReconnectLocation> entries;

    @BeforeEach
    public void setUp() throws Exception {
        entries = new ArrayList<>();

        entries.add(new ReconnectLocation("192.168.2.1", 5672));
        entries.add(new ReconnectLocation("192.168.2.2", 5672));
        entries.add(new ReconnectLocation("192.168.2.3", 5672));
        entries.add(new ReconnectLocation("192.168.2.4", 5672));
    }

    @Test
    public void testCreateEmptyPool() {
        ReconnectLocationPool pool = new ReconnectLocationPool();

        assertTrue(pool.isEmpty());
        assertEquals(0, pool.size());
        assertNotNull(pool.toString());
    }

    @Test
    public void testCreateEmptyPoolFromNullEntryList() {
        ReconnectLocationPool pool = new ReconnectLocationPool(null);
        assertNull(pool.getNext());
    }

    @Test
    public void testCreateNonEmptyPoolWithEntryList() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);

        assertEquals(entries, pool.getList());
        assertNotNull(pool.getNext());
        assertEquals(entries.get(1), pool.getNext());
    }

    @Test
    public void testGetNextFromEmptyPool() {
        ReconnectLocationPool pool = new ReconnectLocationPool();
        assertNull(pool.getNext());
    }

    @Test
    public void testGetNextFromSingleValuePool() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries.subList(0, 1));

        assertEquals(entries.get(0), pool.getNext());
        assertEquals(entries.get(0), pool.getNext());
        assertEquals(entries.get(0), pool.getNext());

        assertNotNull(pool.toString());
    }

    @Test
    public void testAddEntryToEmptyPool() {
        ReconnectLocationPool pool = new ReconnectLocationPool();
        assertTrue(pool.isEmpty());
        pool.add(entries.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(entries.get(0), pool.getNext());
    }

    @Test
    public void testDuplicatesNotAdded() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);

        assertEquals(entries.size(), pool.size());
        pool.add(entries.get(0));
        assertEquals(entries.size(), pool.size());
        pool.add(entries.get(1));
        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testDuplicatesNotAddedByAddFirst() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);

        assertEquals(entries.size(), pool.size());
        pool.addFirst(entries.get(0));
        assertEquals(entries.size(), pool.size());
        pool.addFirst(entries.get(1));
        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testDuplicatesNotAddedIfPortsMatch() {
        ReconnectLocationPool pool = new ReconnectLocationPool();

        assertTrue(pool.isEmpty());
        pool.add(new ReconnectLocation("127.0.0.1", 5672));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new ReconnectLocation("127.0.0.1", 5672));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new ReconnectLocation("localhost", 5673));
        assertEquals(2, pool.size());
    }

    @Test
    public void testAddEntryToPoolThenShuffle() {
        ReconnectLocation newEntry = new ReconnectLocation("192.168.2." + (entries.size() + 1) , 5672);

        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        pool.add(newEntry);

        pool.shuffle();

        ReconnectLocation found = null;

        for (int i = 0; i < entries.size() + 1; ++i) {
        	ReconnectLocation next = pool.getNext();
            if (newEntry.equals(next)) {
                found = next;
            }
        }

        if (found == null) {
            fail("ReconnectEntry added was not retrieved from the pool");
        }
    }

    @Test
    public void testAddEntryToPoolNotRandomized() {
        ReconnectLocation newEntry = new ReconnectLocation("192.168.2." + (entries.size() + 1) , 5672);

        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        pool.shuffle();
        pool.add(newEntry);

        for (int i = 0; i < entries.size(); ++i) {
            assertNotEquals(newEntry, pool.getNext());
        }

        assertEquals(newEntry, pool.getNext());
    }

    @Test
    public void testAddFirst() {
        ReconnectLocation newEntry = new ReconnectLocation("192.168.2." + (entries.size() + 1) , 5672);

        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        pool.addFirst(newEntry);

        assertEquals(newEntry, pool.getNext());

        for (int i = 0; i < entries.size(); ++i) {
            assertNotEquals(newEntry, pool.getNext());
        }

        assertEquals(newEntry, pool.getNext());
    }

    @Test
    public void testAddFirstHandlesNulls() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        pool.addFirst(null);

        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testAddFirstToEmptyPool() {
        ReconnectLocationPool pool = new ReconnectLocationPool();
        assertTrue(pool.isEmpty());
        pool.addFirst(entries.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(entries.get(0), pool.getNext());
    }

    @Test
    public void testAddAllHandlesNulls() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        pool.addAll(null);

        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testAddAllHandlesEmpty() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        pool.addAll(Collections.emptyList());

        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testAddAll() {
        ReconnectLocationPool pool = new ReconnectLocationPool(null);

        assertEquals(0, pool.size());
        assertFalse(entries.isEmpty());

        pool.addAll(entries);

        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testRemoveEntryFromPool() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);

        ReconnectLocation removed = entries.get(0);

        pool.remove(removed);

        for (int i = 0; i < entries.size() + 1; ++i) {
            if (removed.equals(pool.getNext())) {
                fail("ReconnectEntry was not removed from the pool");
            }
        }
    }

    @Test
    public void testRemoveDoesNotApplyHostResolution() {
        ReconnectLocationPool pool = new ReconnectLocationPool();

        assertTrue(pool.isEmpty());
        pool.add(new ReconnectLocation("127.0.0.1", 5672));
        assertFalse(pool.isEmpty());
        pool.remove(new ReconnectLocation("localhost", 5672));
        assertFalse(pool.isEmpty());
        pool.remove(new ReconnectLocation("127.0.0.1", 5673));
        assertFalse(pool.isEmpty());
    }

    @Test
    public void testConnectedShufflesWhenRandomizing() {
        assertConnectedEffectOnPool(true, true);
    }

    @Test
    public void testConnectedDoesNotShufflesWhenNoRandomizing() {
        assertConnectedEffectOnPool(false, false);
    }

    private void assertConnectedEffectOnPool(boolean randomize, boolean shouldShuffle) {

        ReconnectLocationPool pool = new ReconnectLocationPool(entries);

        if (randomize) {
            pool.shuffle();
        }

        List<ReconnectLocation> current = new ArrayList<>();
        List<ReconnectLocation> previous = new ArrayList<>();

        boolean shuffled = false;

        for (int i = 0; i < 10; ++i) {

            for (int j = 0; j < entries.size(); ++j) {
                current.add(pool.getNext());
            }

            if (randomize) {
                pool.shuffle();
            }

            if (!previous.isEmpty() && !previous.equals(current)) {
                shuffled = true;
                break;
            }

            previous.clear();
            previous.addAll(current);
            current.clear();
        }

        if (shouldShuffle) {
            assertTrue(shuffled, "ReconnectEntry list did not get randomized");
        } else {
            assertFalse(shuffled, "ReconnectEntry list should not get randomized");
        }
    }

    @Test
    public void testAddOrRemoveNullHasNoAffect() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        assertEquals(entries.size(), pool.size());

        pool.add(null);
        assertEquals(entries.size(), pool.size());
        pool.remove(null);
        assertEquals(entries.size(), pool.size());
    }

    @Test
    public void testRemoveAll() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        assertEquals(entries.size(), pool.size());

        pool.removeAll();
        assertTrue(pool.isEmpty());
        assertEquals(0, pool.size());

        pool.removeAll();
    }

    @Test
    public void testReplaceAll() {
        ReconnectLocationPool pool = new ReconnectLocationPool(entries);
        assertEquals(entries.size(), pool.size());

        List<ReconnectLocation> newEntries = new ArrayList<>();

        newEntries.add(new ReconnectLocation("192.168.2.1", 5672));
        newEntries.add(new ReconnectLocation("192.168.2.2", 5672));

        pool.replaceAll(newEntries);
        assertFalse(pool.isEmpty());
        assertEquals(newEntries.size(), pool.size());
        assertEquals(newEntries, pool.getList());

        pool.removeAll();
    }
}
