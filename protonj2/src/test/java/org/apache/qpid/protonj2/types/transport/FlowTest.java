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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
