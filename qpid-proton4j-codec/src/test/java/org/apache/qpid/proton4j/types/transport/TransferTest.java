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

import org.apache.qpid.proton4j.types.transport.Performative;
import org.apache.qpid.proton4j.types.transport.Transfer;
import org.junit.Test;

public class TransferTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.TRANSFER, new Transfer().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Transfer().toString());
    }

    @Test
    public void testInitialState() {
        Transfer transfer = new Transfer();

        assertEquals(0, transfer.getElementCount());
        assertTrue(transfer.isEmpty());
        assertFalse(transfer.hasAborted());
        assertFalse(transfer.hasBatchable());
        assertFalse(transfer.hasDeliveryId());
        assertFalse(transfer.hasDeliveryTag());
        assertFalse(transfer.hasHandle());
        assertFalse(transfer.hasMessageFormat());
        assertFalse(transfer.hasMore());
        assertFalse(transfer.hasRcvSettleMode());
        assertFalse(transfer.hasResume());
        assertFalse(transfer.hasSettled());
        assertFalse(transfer.hasState());
    }

    @Test
    public void testIsEmpty() {
        Transfer transfer = new Transfer();

        assertEquals(0, transfer.getElementCount());
        assertTrue(transfer.isEmpty());
        assertFalse(transfer.hasAborted());

        transfer.setAborted(true);

        assertTrue(transfer.getElementCount() > 0);
        assertFalse(transfer.isEmpty());
        assertTrue(transfer.hasAborted());
        assertTrue(transfer.getAborted());

        transfer.setAborted(false);

        assertTrue(transfer.getElementCount() > 0);
        assertFalse(transfer.isEmpty());
        assertTrue(transfer.hasAborted());
        assertFalse(transfer.getAborted());
    }

    @Test
    public void testSetHandleEnforcesRange() {
        Transfer transfer = new Transfer();

        try {
            transfer.setHandle(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            transfer.setHandle(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetDeliveryIdEnforcesRange() {
        Transfer transfer = new Transfer();

        try {
            transfer.setDeliveryId(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            transfer.setDeliveryId(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetMessageFormatEnforcesRange() {
        Transfer transfer = new Transfer();

        try {
            transfer.setMessageFormat(-1);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}

        try {
            transfer.setMessageFormat(Long.MAX_VALUE);
            fail("Should not be able to set out of range value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCopyFromNew() {
        Transfer original = new Transfer();
        Transfer copy = original.copy();

        assertTrue(original.isEmpty());
        assertTrue(copy.isEmpty());

        assertEquals(0, original.getElementCount());
        assertEquals(0, copy.getElementCount());
    }
}
