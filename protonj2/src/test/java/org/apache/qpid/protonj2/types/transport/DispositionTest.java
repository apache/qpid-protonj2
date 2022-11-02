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
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.junit.jupiter.api.Test;

public class DispositionTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.DISPOSITION, new Disposition().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Disposition().toString());
    }

    @Test
    public void testLastValueRangeChecks() {
        Disposition disposition = new Disposition();

        disposition.setLast(0);
        disposition.setLast(Integer.MAX_VALUE);
        disposition.setLast(UnsignedInteger.MAX_VALUE.longValue());

        try {
            disposition.setLast(-1l);
            fail("Should throw on value out of range.");
        } catch (IllegalArgumentException iae) {}

        try {
            disposition.setLast(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Should throw on value out of range.");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testFirstValueRangeChecks() {
        Disposition disposition = new Disposition();

        disposition.setFirst(0);
        disposition.setFirst(Integer.MAX_VALUE);
        disposition.setFirst(UnsignedInteger.MAX_VALUE.longValue());

        try {
            disposition.setFirst(-1l);
            fail("Should throw on value out of range.");
        } catch (IllegalArgumentException iae) {}

        try {
            disposition.setFirst(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Should throw on value out of range.");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testRoleCannotBeSetNull() {
        Disposition disposition = new Disposition();

        assertThrows(NullPointerException.class, () -> disposition.setRole(null));
    }

    @Test
    public void testInitialState() {
        Disposition disposition = new Disposition();

        assertEquals(0, disposition.getElementCount());
        assertTrue(disposition.isEmpty());
        assertFalse(disposition.hasBatchable());
        assertFalse(disposition.hasFirst());
        assertFalse(disposition.hasLast());
        assertFalse(disposition.hasRole());
        assertFalse(disposition.hasSettled());
        assertFalse(disposition.hasState());

        assertFalse(disposition.hasElement(0));
        assertFalse(disposition.hasElement(1));
        assertFalse(disposition.hasElement(2));
        assertFalse(disposition.hasElement(3));
        assertFalse(disposition.hasElement(4));
        assertFalse(disposition.hasElement(5));
    }

    @Test
    public void testClearPayloadAPI() {
        Disposition disposition = new Disposition();

        disposition.setBatchable(true);
        disposition.setFirst(1);
        disposition.setLast(2);
        disposition.setRole(Role.SENDER);
        disposition.setSettled(true);
        disposition.setState(Accepted.getInstance());

        assertFalse(disposition.isEmpty());
        assertTrue(disposition.hasBatchable());
        assertTrue(disposition.hasFirst());
        assertTrue(disposition.hasLast());
        assertTrue(disposition.hasRole());
        assertTrue(disposition.hasSettled());
        assertTrue(disposition.hasState());

        assertTrue(disposition.hasElement(0));
        assertTrue(disposition.hasElement(1));
        assertTrue(disposition.hasElement(2));
        assertTrue(disposition.hasElement(3));
        assertTrue(disposition.hasElement(4));
        assertTrue(disposition.hasElement(5));
        assertFalse(disposition.hasElement(6));

        disposition.clearBatchable();
        disposition.clearFirst();
        disposition.clearLast();
        disposition.clearRole();
        disposition.clearSettled();
        disposition.clearState();

        assertEquals(0, disposition.getElementCount());
        assertTrue(disposition.isEmpty());
        assertFalse(disposition.hasBatchable());
        assertFalse(disposition.hasFirst());
        assertFalse(disposition.hasLast());
        assertFalse(disposition.hasRole());
        assertFalse(disposition.hasSettled());
        assertFalse(disposition.hasState());
    }

    @Test
    public void testIsEmpty() {
        Disposition disposition = new Disposition();

        assertEquals(0, disposition.getElementCount());
        assertTrue(disposition.isEmpty());
        assertFalse(disposition.hasFirst());

        disposition.setFirst(0);

        assertTrue(disposition.getElementCount() > 0);
        assertFalse(disposition.isEmpty());
        assertTrue(disposition.hasFirst());

        disposition.setFirst(1);

        assertTrue(disposition.getElementCount() > 0);
        assertFalse(disposition.isEmpty());
        assertTrue(disposition.hasFirst());
    }

    @Test
    public void testCopyFromNew() {
        Disposition original = new Disposition();
        Disposition copy = original.copy();

        assertTrue(original.isEmpty());
        assertTrue(copy.isEmpty());

        assertEquals(0, original.getElementCount());
        assertEquals(0, copy.getElementCount());
    }
}
