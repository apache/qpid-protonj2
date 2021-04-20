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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.junit.jupiter.api.Test;

public class AttachTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.ATTACH, new Attach().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Attach().toString());
    }

    @Test
    public void testInitialState() {
        Attach attach = new Attach();

        assertEquals(0, attach.getElementCount());
        assertTrue(attach.isEmpty());
        assertFalse(attach.hasDesiredCapabilites());
        assertFalse(attach.hasHandle());
        assertFalse(attach.hasIncompleteUnsettled());
        assertFalse(attach.hasInitialDeliveryCount());
        assertFalse(attach.hasMaxMessageSize());
        assertFalse(attach.hasName());
        assertFalse(attach.hasOfferedCapabilites());
        assertFalse(attach.hasProperties());
        assertFalse(attach.hasReceiverSettleMode());
        assertFalse(attach.hasRole());
        assertFalse(attach.hasSenderSettleMode());
        assertFalse(attach.hasSource());
        assertFalse(attach.hasTarget());
    }

    @Test
    public void testIsEmpty() {
        Attach attach = new Attach();

        assertEquals(0, attach.getElementCount());
        assertTrue(attach.isEmpty());
        assertFalse(attach.hasHandle());

        attach.setHandle(0);

        assertTrue(attach.getElementCount() > 0);
        assertFalse(attach.isEmpty());
        assertTrue(attach.hasHandle());

        attach.setHandle(1);

        assertTrue(attach.getElementCount() > 0);
        assertFalse(attach.isEmpty());
        assertTrue(attach.hasHandle());
    }

    @Test
    public void testSetNameRefusesNull() {
        try {
            new Attach().setName(null);
            fail("Link name is mandatory");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testSetRoleRefusesNull() {
        try {
            new Attach().setRole(null);
            fail("Link role is mandatory");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testHandleRangeChecks() {
        Attach attach = new Attach();
        try {
            attach.setHandle(-1l);
            fail("Cannot set negative long handle value");
        } catch (IllegalArgumentException iae) {}

        try {
            attach.setHandle(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Cannot set long handle value bigger than uint max");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testDeliveryCountRangeChecks() {
        Attach attach = new Attach();
        try {
            attach.setInitialDeliveryCount(-1l);
            fail("Cannot set negative long delivery count value");
        } catch (IllegalArgumentException iae) {}

        try {
            attach.setInitialDeliveryCount(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Cannot set long delivery count value bigger than uint max");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testHasTargetOrCoordinator() {
        Attach attach = new Attach();

        assertFalse(attach.hasCoordinator());
        assertFalse(attach.hasTarget());
        assertFalse(attach.hasTargetOrCoordinator());

        attach.setTarget(new Target());

        assertFalse(attach.hasCoordinator());
        assertTrue(attach.hasTarget());
        assertTrue(attach.hasTargetOrCoordinator());

        attach.setTarget(new Coordinator());

        assertTrue(attach.hasCoordinator());
        assertFalse(attach.hasTarget());
        assertTrue(attach.hasTargetOrCoordinator());

        attach.setTarget((Target) null);

        assertFalse(attach.hasCoordinator());
        assertFalse(attach.hasTarget());
        assertFalse(attach.hasTargetOrCoordinator());

        attach.setCoordinator(new Coordinator());

        assertTrue(attach.hasCoordinator());
        assertFalse(attach.hasTarget());
        assertTrue(attach.hasTargetOrCoordinator());
    }

    @Test
    public void testCopyAttachWithTarget() {
        Attach original = new Attach();

        original.setTarget(new Target());

        Attach copy = original.copy();

        assertNotNull(copy.getTarget());
        assertEquals(original.<Target>getTarget(), copy.<Target>getTarget());
    }

    @Test
    public void testCopy() {
        final Map<Binary, DeliveryState> unsettled = new HashMap<>();
        unsettled.put(new Binary(new byte[] {1}), Accepted.getInstance());

        final Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), "test1");

        Attach original = new Attach();

        original.setDesiredCapabilities(Symbol.valueOf("queue"));
        original.setOfferedCapabilities(Symbol.valueOf("queue"), Symbol.valueOf("topic"));
        original.setHandle(1);
        original.setIncompleteUnsettled(true);
        original.setUnsettled(unsettled);
        original.setInitialDeliveryCount(12);
        original.setName("test");
        original.setTarget(new Target());
        original.setSource(new Source());
        original.setRole(Role.RECEIVER);
        original.setSenderSettleMode(SenderSettleMode.SETTLED);
        original.setReceiverSettleMode(ReceiverSettleMode.SECOND);
        original.setMaxMessageSize(1024);
        original.setProperties(properties);

        assertNotNull(original.toString());  // Check no fumble on full populated fields.

        Attach copy = original.copy();

        assertArrayEquals(copy.getDesiredCapabilities(), copy.getDesiredCapabilities());
        assertArrayEquals(copy.getOfferedCapabilities(), copy.getOfferedCapabilities());
        assertEquals(original.<Target>getTarget(), copy.<Target>getTarget());
        assertEquals(original.getIncompleteUnsettled(), copy.getIncompleteUnsettled());
        assertEquals(original.getUnsettled(), copy.getUnsettled());
        assertEquals(original.getInitialDeliveryCount(), copy.getInitialDeliveryCount());
        assertEquals(original.getName(), copy.getName());
        assertEquals(original.getSource(), copy.getSource());
        assertEquals(original.getRole(), copy.getRole());
        assertEquals(original.getSenderSettleMode(), copy.getSenderSettleMode());
        assertEquals(original.getReceiverSettleMode(), copy.getReceiverSettleMode());
        assertEquals(original.getMaxMessageSize(), copy.getMaxMessageSize());
        assertEquals(original.getProperties(), copy.getProperties());
    }

    @Test
    public void testHasFields() {
        final Map<Binary, DeliveryState> unsettled = new HashMap<>();
        unsettled.put(new Binary(new byte[] {1}), Accepted.getInstance());
        final Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), "test1");

        Attach original = new Attach();

        original.setDesiredCapabilities(Symbol.valueOf("queue"));
        original.setOfferedCapabilities(Symbol.valueOf("queue"), Symbol.valueOf("topic"));
        original.setHandle(1);
        original.setIncompleteUnsettled(true);
        original.setUnsettled(unsettled);
        original.setInitialDeliveryCount(12);
        original.setName("test");
        original.setTarget(new Target());
        original.setSource(new Source());
        original.setRole(Role.RECEIVER);
        original.setSenderSettleMode(SenderSettleMode.SETTLED);
        original.setReceiverSettleMode(ReceiverSettleMode.SECOND);
        original.setMaxMessageSize(1024);
        original.setProperties(properties);

        assertTrue(original.hasDesiredCapabilites());
        assertTrue(original.hasOfferedCapabilites());
        assertTrue(original.hasHandle());
        assertTrue(original.hasIncompleteUnsettled());
        assertTrue(original.hasUnsettled());
        assertTrue(original.hasInitialDeliveryCount());
        assertTrue(original.hasName());
        assertTrue(original.hasTarget());
        assertFalse(original.hasCoordinator());
        assertTrue(original.hasSource());
        assertTrue(original.hasRole());
        assertTrue(original.hasSenderSettleMode());
        assertTrue(original.hasReceiverSettleMode());
        assertTrue(original.hasMaxMessageSize());
        assertTrue(original.hasProperties());

        original.setProperties(null);
        original.setSource(null);
        original.setTarget((Coordinator) null);
        original.setMaxMessageSize(null);
        original.setUnsettled(null);
        original.setOfferedCapabilities(null);
        original.setDesiredCapabilities(null);

        assertFalse(original.hasTarget());
        assertFalse(original.hasSource());
        assertFalse(original.hasMaxMessageSize());
        assertFalse(original.hasProperties());
        assertFalse(original.hasUnsettled());

        original.setCoordinator(new Coordinator());
        assertFalse(original.hasTarget());
        assertTrue(original.hasCoordinator());
        original.setCoordinator(null);
        assertFalse(original.hasTarget());
        assertFalse(original.hasCoordinator());
        assertFalse(original.hasDesiredCapabilites());
        assertFalse(original.hasOfferedCapabilites());
    }

    @Test
    public void testSetTargetAndCoordinatorThrowIllegalArguementErrorOnBadInput() {
        Attach original = new Attach();

        assertThrows(IllegalArgumentException.class, () -> original.setTarget(new Source()));
    }

    @Test
    public void testReplaceTargetWithCoordinator() {
        Attach original = new Attach();

        assertFalse(original.hasTarget());
        assertFalse(original.hasCoordinator());

        original.setTarget(new Target());

        assertTrue(original.hasTarget());
        assertFalse(original.hasCoordinator());

        original.setCoordinator(new Coordinator());

        assertFalse(original.hasTarget());
        assertTrue(original.hasCoordinator());
    }

    @Test
    public void testReplaceCoordinatorWithTarget() {
        Attach original = new Attach();

        assertFalse(original.hasTarget());
        assertFalse(original.hasCoordinator());

        original.setCoordinator(new Coordinator());

        assertFalse(original.hasTarget());
        assertTrue(original.hasCoordinator());

        original.setTarget(new Target());

        assertTrue(original.hasTarget());
        assertFalse(original.hasCoordinator());
    }

    @Test
    public void testCopyAttachWithCoordinator() {
        Attach original = new Attach();

        original.setCoordinator(new Coordinator());

        Attach copy = original.copy();

        assertNotNull(copy.getTarget());
        assertEquals(original.<Coordinator>getTarget(), copy.<Coordinator>getTarget(), "Should be equal");

        Coordinator coordinator = copy.getTarget();

        assertNotNull(coordinator);
        assertEquals(original.getTarget(), coordinator);
    }

    @Test
    public void testCopyFromNew() {
        Attach original = new Attach();
        Attach copy = original.copy();

        assertTrue(original.isEmpty());
        assertTrue(copy.isEmpty());

        assertEquals(0, original.getElementCount());
        assertEquals(0, copy.getElementCount());
    }
}
