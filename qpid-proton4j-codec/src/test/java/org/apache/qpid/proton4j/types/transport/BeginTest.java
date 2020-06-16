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
package org.apache.qpid.proton4j.types.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.types.transport.Begin;
import org.apache.qpid.proton4j.types.transport.Performative;
import org.junit.Test;

public class BeginTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.BEGIN, new Begin().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Begin().toString());
    }

    @Test
    public void testIsEmpty() {
        Begin begin = new Begin();

        assertEquals(0, begin.getElementCount());
        assertTrue(begin.isEmpty());
        assertFalse(begin.hasOutgoingWindow());

        begin.setOutgoingWindow(1);

        assertTrue(begin.getElementCount() > 0);
        assertFalse(begin.isEmpty());
        assertTrue(begin.hasOutgoingWindow());

        begin.setOutgoingWindow(0);

        assertTrue(begin.getElementCount() > 0);
        assertFalse(begin.isEmpty());
        assertTrue(begin.hasOutgoingWindow());
    }

    @Test
    public void testIncomingWindowEnforcesRange() {
        Begin begin = new Begin();

        try {
            begin.setIncomingWindow(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            begin.setIncomingWindow(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testOutgoingWindowEnforcesRange() {
        Begin begin = new Begin();

        try {
            begin.setOutgoingWindow(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            begin.setOutgoingWindow(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testHandleMaxEnforcesRange() {
        Begin begin = new Begin();

        try {
            begin.setHandleMax(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            begin.setHandleMax(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testNextOutgoingIdEnforcesRange() {
        Begin begin = new Begin();

        try {
            begin.setNextOutgoingId(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            begin.setNextOutgoingId(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testRemoteChannelEnforcesRange() {
        Begin begin = new Begin();

        try {
            begin.setRemoteChannel(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            begin.setRemoteChannel(Integer.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCopyFromNew() {
        Begin original = new Begin();
        Begin copy = original.copy();

        assertTrue(original.isEmpty());
        assertTrue(copy.isEmpty());

        assertEquals(0, original.getElementCount());
        assertEquals(0, copy.getElementCount());
    }
}
