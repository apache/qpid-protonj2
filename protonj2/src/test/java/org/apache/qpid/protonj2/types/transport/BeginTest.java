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
package org.apache.qpid.protonj2.types.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;

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
    public void testHasMethods() {
        Begin begin = new Begin();

        assertFalse(begin.hasHandleMax());
        assertFalse(begin.hasNextOutgoingId());
        assertFalse(begin.hasDesiredCapabilites());
        assertFalse(begin.hasOfferedCapabilites());
        assertFalse(begin.hasOutgoingWindow());
        assertFalse(begin.hasIncomingWindow());
        assertFalse(begin.hasProperties());
        assertFalse(begin.hasRemoteChannel());

        begin.setDesiredCapabilities(Symbol.valueOf("test"));
        begin.setOfferedCapabilities(Symbol.valueOf("test"));
        begin.setHandleMax(65535);
        begin.setIncomingWindow(255);
        begin.setOutgoingWindow(Integer.MAX_VALUE);
        begin.setRemoteChannel(1);
        begin.setProperties(new HashMap<>());
        begin.setNextOutgoingId(Short.MAX_VALUE);

        assertTrue(begin.hasHandleMax());
        assertTrue(begin.hasNextOutgoingId());
        assertTrue(begin.hasDesiredCapabilites());
        assertTrue(begin.hasOfferedCapabilites());
        assertTrue(begin.hasOutgoingWindow());
        assertTrue(begin.hasIncomingWindow());
        assertTrue(begin.hasProperties());
        assertTrue(begin.hasRemoteChannel());
    }

    @Test
    public void testHandleMaxIfSetIsAlwaysPresent() {
        Begin begin = new Begin();

        assertFalse(begin.hasHandleMax());
        begin.setHandleMax(0);
        assertTrue(begin.hasHandleMax());
        begin.setHandleMax(65535);
        assertTrue(begin.hasHandleMax());
        begin.setHandleMax(UnsignedInteger.MAX_VALUE.longValue());
        assertTrue(begin.hasHandleMax());
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

    @Test
    public void testCopyHandlesProperties() {
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test1"), "one");
        properties.put(Symbol.valueOf("test2"), "two");
        properties.put(Symbol.valueOf("test3"), "three");

        final Begin begin = new Begin();
        begin.setProperties(properties);

        final Begin copied = begin.copy();

        assertTrue(begin.hasProperties());
        assertTrue(copied.hasProperties());

        assertEquals(copied.getProperties(), begin.getProperties());
        assertEquals(copied.getProperties(), properties);
    }

    @Test
    public void testCopyHandlesDesiredCapabilities() {
        Symbol[] desiredCapabilities = { Symbol.valueOf("test1"),
                                         Symbol.valueOf("test2"),
                                         Symbol.valueOf("test3") };

        final Begin begin = new Begin();
        begin.setDesiredCapabilities(desiredCapabilities);

        final Begin copied = begin.copy();

        assertTrue(begin.hasDesiredCapabilites());
        assertTrue(copied.hasDesiredCapabilites());

        assertArrayEquals(copied.getDesiredCapabilities(), begin.getDesiredCapabilities());
        assertArrayEquals(copied.getDesiredCapabilities(), desiredCapabilities);
    }

    @Test
    public void testCopyHandlesOfferedCapabilities() {
        Symbol[] offeredCapabilities = { Symbol.valueOf("test1"),
                                         Symbol.valueOf("test2"),
                                         Symbol.valueOf("test3") };

        final Begin begin = new Begin();
        begin.setOfferedCapabilities(offeredCapabilities);

        final Begin copied = begin.copy();

        assertTrue(begin.hasOfferedCapabilites());
        assertTrue(copied.hasOfferedCapabilites());

        assertArrayEquals(copied.getOfferedCapabilities(), begin.getOfferedCapabilities());
        assertArrayEquals(copied.getOfferedCapabilities(), offeredCapabilities);
    }
}
