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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RecoonectionURIPool {

    private List<URI> uris;

    @BeforeEach
    public void setUp() throws Exception {
        uris = new ArrayList<URI>();

        uris.add(new URI("tcp://192.168.2.1:5672"));
        uris.add(new URI("tcp://192.168.2.2:5672"));
        uris.add(new URI("tcp://192.168.2.3:5672"));
        uris.add(new URI("tcp://192.168.2.4:5672"));
    }

    @Test
    public void testCreateEmptyPool() {
        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        assertEquals(0, pool.size());
    }

    @Test
    public void testCreateEmptyPoolFromNullUris() {
        ReconnectionURIPool pool = new ReconnectionURIPool(null);
        assertNull(pool.getNext());
    }

    @Test
    public void testCreateNonEmptyPoolWithURIs() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        assertNotNull(pool.getNext());
        assertEquals(uris.get(1), pool.getNext());
    }

    @Test
    public void testGetNextFromEmptyPool() {
        ReconnectionURIPool pool = new ReconnectionURIPool();
        assertNull(pool.getNext());
    }

    @Test
    public void testGetNextFromSingleValuePool() {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris.subList(0, 1));

        assertEquals(uris.get(0), pool.getNext());
        assertEquals(uris.get(0), pool.getNext());
        assertEquals(uris.get(0), pool.getNext());
    }

    @Test
    public void testAddUriToEmptyPool() {
        ReconnectionURIPool pool = new ReconnectionURIPool();
        assertTrue(pool.isEmpty());
        pool.add(uris.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(uris.get(0), pool.getNext());
    }

    @Test
    public void testDuplicatesNotAdded() {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);

        assertEquals(uris.size(), pool.size());
        pool.add(uris.get(0));
        assertEquals(uris.size(), pool.size());
        pool.add(uris.get(1));
        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testDuplicatesNotAddedByAddFirst() {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);

        assertEquals(uris.size(), pool.size());
        pool.addFirst(uris.get(0));
        assertEquals(uris.size(), pool.size());
        pool.addFirst(uris.get(1));
        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testDuplicatesNotAddedWhenQueryPresent() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://localhost:5672?transport.tcpNoDelay=true"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://localhost:5672?transport.tcpNoDelay=false"));
        assertEquals(1, pool.size());
    }

    @Test
    public void testDuplicatesNotAddedWithHostResolution() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://localhost:5672"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://localhost:5673"));
        assertEquals(2, pool.size());
    }

    @Test
    public void testDuplicatesNotAddedUnresolvable() throws Exception {
        assumeFalse(checkIfResolutionWorks(), "Host resolution works when not expected");

        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://shouldbeunresolvable:5672"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://shouldbeunresolvable:5672"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SHOULDBEUNRESOLVABLE:5672"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SHOULDBEUNRESOLVABLE2:5672"));
        assertEquals(2, pool.size());
    }

    @Test
    public void testDuplicatesNotAddedWhenQueryPresentAndUnresolveable() throws URISyntaxException {
        assumeFalse(checkIfResolutionWorks(), "Host resolution works when not expected");

        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://shouldbeunresolvable:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://shouldbeunresolvable:5672?transport.tcpNoDelay=false"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SHOULDBEUNRESOLVABLE:5672?transport.tcpNoDelay=true"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SHOULDBEUNRESOLVABLE2:5672?transport.tcpNoDelay=true"));
        assertEquals(2, pool.size());
    }

    @Test
    public void testAddUriToPoolThenShuffle() throws URISyntaxException {
        URI newUri = new URI("tcp://192.168.2." + (uris.size() + 1) + ":5672");

        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        pool.add(newUri);

        pool.shuffle();

        URI found = null;

        for (int i = 0; i < uris.size() + 1; ++i) {
            URI next = pool.getNext();
            if (newUri.equals(next)) {
                found = next;
            }
        }

        if (found == null) {
            fail("URI added was not retrieved from the pool");
        }
    }

    @Test
    public void testAddUriToPoolNotRandomized() throws URISyntaxException {
        URI newUri = new URI("tcp://192.168.2." + (uris.size() + 1) + ":5672");

        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        pool.shuffle();
        pool.add(newUri);

        for (int i = 0; i < uris.size(); ++i) {
            assertNotEquals(newUri, pool.getNext());
        }

        assertEquals(newUri, pool.getNext());
    }

    @Test
    public void testAddFirst() throws URISyntaxException {
        URI newUri = new URI("tcp://192.168.2." + (uris.size() + 1) + ":5672");

        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        pool.addFirst(newUri);

        assertEquals(newUri, pool.getNext());

        for (int i = 0; i < uris.size(); ++i) {
            assertNotEquals(newUri, pool.getNext());
        }

        assertEquals(newUri, pool.getNext());
    }

    @Test
    public void testAddFirstHandlesNulls() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        pool.addFirst(null);

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testAddFirstToEmptyPool() {
        ReconnectionURIPool pool = new ReconnectionURIPool();
        assertTrue(pool.isEmpty());
        pool.addFirst(uris.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(uris.get(0), pool.getNext());
    }

    @Test
    public void testAddAllHandlesNulls() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        pool.addAll(null);

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testAddAllHandlesEmpty() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        pool.addAll(Collections.emptyList());

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testAddAll() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(null);

        assertEquals(0, pool.size());
        assertFalse(uris.isEmpty());

        pool.addAll(uris);

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testRemoveURIFromPool() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);

        URI removed = uris.get(0);

        pool.remove(removed);

        for (int i = 0; i < uris.size() + 1; ++i) {
            if (removed.equals(pool.getNext())) {
                fail("URI was not removed from the pool");
            }
        }
    }

    @Test
    public void testRemovedWhenQueryPresent() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://localhost:5672?transport.tcpNoDelay=true"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://localhost:5672?transport.tcpNoDelay=false"));
        assertTrue(pool.isEmpty());
    }

    @Test
    public void testRemoveWithHostResolution() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://localhost:5672"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://localhost:5673"));
        assertFalse(pool.isEmpty());
    }

    @Test
    public void testRemoveWhenUnresolvable() throws URISyntaxException {
        assumeFalse(checkIfResolutionWorks(), "Host resolution works when not expected");

        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://shouldbeunresolvable:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://SHOULDBEUNRESOLVABLE:5672"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://shouldbeunresolvable:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://shouldbeunresolvable:5673"));
        assertFalse(pool.isEmpty());
    }

    @Test
    public void testRemoveWhenQueryPresentAndUnresolveable() throws URISyntaxException {
        assumeFalse(checkIfResolutionWorks(), "Host resolution works when not expected");

        ReconnectionURIPool pool = new ReconnectionURIPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://shouldbeunresolvable:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://SHOULDBEUNRESOLVABLE:5672?transport.tcpNoDelay=true"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://shouldbeunresolvable:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://shouldbeunresolvable:5673?transport.tcpNoDelay=true"));
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

        ReconnectionURIPool pool = new ReconnectionURIPool(uris);

        if (randomize) {
            pool.shuffle();
        }

        List<URI> current = new ArrayList<URI>();
        List<URI> previous = new ArrayList<URI>();

        boolean shuffled = false;

        for (int i = 0; i < 10; ++i) {

            for (int j = 0; j < uris.size(); ++j) {
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
            assertTrue(shuffled, "URIs did not get randomized");
        } else {
            assertFalse(shuffled, "URIs should not get randomized");
        }
    }

    @Test
    public void testAddOrRemoveNullHasNoAffect() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        assertEquals(uris.size(), pool.size());

        pool.add(null);
        assertEquals(uris.size(), pool.size());
        pool.remove(null);
        assertEquals(uris.size(), pool.size());
    }

    private boolean checkIfResolutionWorks() {
        boolean resolutionWorks = false;
        try {
            resolutionWorks = InetAddress.getByName("shouldbeunresolvable") != null;
            resolutionWorks = InetAddress.getByName("SHOULDBEUNRESOLVABLE") != null;
            resolutionWorks = InetAddress.getByName("SHOULDBEUNRESOLVABLE2") != null;
        } catch (Exception e) {
        }

        return resolutionWorks;
    }

    @Test
    public void testRemoveAll() throws URISyntaxException {
        ReconnectionURIPool pool = new ReconnectionURIPool(uris);
        assertEquals(uris.size(), pool.size());

        pool.removeAll();
        assertTrue(pool.isEmpty());
        assertEquals(0, pool.size());

        pool.removeAll();
    }
}
