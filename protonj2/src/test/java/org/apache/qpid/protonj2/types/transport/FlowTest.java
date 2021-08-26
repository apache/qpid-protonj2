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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.junit.jupiter.api.Test;

public class FlowTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.FLOW, new Flow().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Flow().toString());
    }

    @Test
    public void testSetHandleEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setHandle(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setHandle(-1l));
    }

    @Test
    public void testSetNextIncomingIdEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setNextIncomingId(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setNextIncomingId(-1l));
    }

    @Test
    public void testSetNextOutgoingIdEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setNextOutgoingId(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setNextOutgoingId(-1l));
    }

    @Test
    public void testSetOutgoingWindowEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setOutgoingWindow(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setOutgoingWindow(-1l));
    }

    @Test
    public void testSetIncomingWindowEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setIncomingWindow(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setIncomingWindow(-1l));
    }

    @Test
    public void testSetDeliveryCountEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setDeliveryCount(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setDeliveryCount(-1l));
    }

    @Test
    public void testSetLinkCreditEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setLinkCredit(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setLinkCredit(-1l));
    }

    @Test
    public void testSetAvailableEnforcesLimits() {
        Flow flow = new Flow();

        assertThrows(IllegalArgumentException.class, () -> flow.setAvailable(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> flow.setAvailable(-1l));
    }

    @Test
    public void testCopy() {
        final Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), "test1");

        Flow flow = new Flow();

        flow.setAvailable(1024);
        flow.setDeliveryCount(5);
        flow.setDrain(true);
        flow.setEcho(true);
        flow.setHandle(3);
        flow.setIncomingWindow(1024);
        flow.setLinkCredit(255);
        flow.setNextIncomingId(12);
        flow.setNextOutgoingId(13);
        flow.setOutgoingWindow(2048);
        flow.setProperties(properties);

        Flow copy = flow.copy();

        assertEquals(flow.getAvailable(), copy.getAvailable());
        assertEquals(flow.getDeliveryCount(), copy.getDeliveryCount());
        assertEquals(flow.getDrain(), copy.getDrain());
        assertEquals(flow.getEcho(), copy.getEcho());
        assertEquals(flow.getHandle(), copy.getHandle());
        assertEquals(flow.getIncomingWindow(), copy.getIncomingWindow());
        assertEquals(flow.getOutgoingWindow(), copy.getOutgoingWindow());
        assertEquals(flow.getNextIncomingId(), copy.getNextIncomingId());
        assertEquals(flow.getNextOutgoingId(), copy.getNextOutgoingId());
        assertEquals(flow.getProperties(), copy.getProperties());
        assertEquals(flow.getLinkCredit(), copy.getLinkCredit());
    }

    @Test
    public void testClearFieldsAPI() {
        final Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), "test1");

        Flow flow = new Flow();

        flow.setAvailable(1024);
        flow.setDeliveryCount(5);
        flow.setDrain(true);
        flow.setEcho(true);
        flow.setHandle(3);
        flow.setIncomingWindow(1024);
        flow.setLinkCredit(255);
        flow.setNextIncomingId(12);
        flow.setNextOutgoingId(13);
        flow.setOutgoingWindow(2048);
        flow.setProperties(properties);

        assertEquals(11, flow.getElementCount());
        assertFalse(flow.isEmpty());
        assertTrue(flow.hasAvailable());
        assertTrue(flow.hasDeliveryCount());
        assertTrue(flow.hasDrain());
        assertTrue(flow.hasEcho());
        assertTrue(flow.hasHandle());
        assertTrue(flow.hasIncomingWindow());
        assertTrue(flow.hasLinkCredit());
        assertTrue(flow.hasNextIncomingId());
        assertTrue(flow.hasNextOutgoingId());
        assertTrue(flow.hasOutgoingWindow());
        assertTrue(flow.hasProperties());

        assertNotNull(flow.toString()); // Ensure fully populated toString does not error

        flow.clearAvailable();
        flow.clearDeliveryCount();
        flow.clearDrain();
        flow.clearEcho();
        flow.clearHandle();
        flow.clearIncomingWindow();
        flow.clearLinkCredit();
        flow.clearNextIncomingId();
        flow.clearNextOutgoingId();
        flow.clearOutgoingWindow();
        flow.clearProperties();

        assertEquals(0, flow.getElementCount());
        assertTrue(flow.isEmpty());
        assertFalse(flow.hasAvailable());
        assertFalse(flow.hasDeliveryCount());
        assertFalse(flow.hasDrain());
        assertFalse(flow.hasEcho());
        assertFalse(flow.hasHandle());
        assertFalse(flow.hasIncomingWindow());
        assertFalse(flow.hasLinkCredit());
        assertFalse(flow.hasNextIncomingId());
        assertFalse(flow.hasNextOutgoingId());
        assertFalse(flow.hasOutgoingWindow());
        assertFalse(flow.hasProperties());

        flow.setProperties(properties);
        assertTrue(flow.hasProperties());
        flow.setProperties(null);
        assertFalse(flow.hasProperties());
    }

    @Test
    public void testInitialState() {
        Flow flow = new Flow();

        assertEquals(0, flow.getElementCount());
        assertTrue(flow.isEmpty());
        assertFalse(flow.hasAvailable());
        assertFalse(flow.hasDeliveryCount());
        assertFalse(flow.hasDrain());
        assertFalse(flow.hasEcho());
        assertFalse(flow.hasHandle());
        assertFalse(flow.hasIncomingWindow());
        assertFalse(flow.hasLinkCredit());
        assertFalse(flow.hasNextIncomingId());
        assertFalse(flow.hasNextOutgoingId());
        assertFalse(flow.hasOutgoingWindow());
        assertFalse(flow.hasProperties());
    }

    @Test
    public void testIsEmpty() {
        Flow flow = new Flow();

        assertEquals(0, flow.getElementCount());
        assertTrue(flow.isEmpty());
        assertFalse(flow.hasLinkCredit());

        flow.setLinkCredit(10);

        assertNotNull(flow.toString()); // Ensure partially populated toString does not error
        assertTrue(flow.getElementCount() > 0);
        assertFalse(flow.isEmpty());
        assertTrue(flow.hasLinkCredit());

        flow.setLinkCredit(0);

        assertTrue(flow.getElementCount() > 0);
        assertFalse(flow.isEmpty());
        assertTrue(flow.hasLinkCredit());
    }

    @Test
    public void testCopyFromNew() {
        Flow original = new Flow();
        Flow copy = original.copy();

        assertTrue(original.isEmpty());
        assertTrue(copy.isEmpty());

        assertEquals(0, original.getElementCount());
        assertEquals(0, copy.getElementCount());
    }
}
