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
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.junit.jupiter.api.Test;

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
    public void testClearFieldsAPI() {
        Transfer transfer = new Transfer();

        transfer.setAborted(true);
        transfer.setBatchable(true);
        transfer.setDeliveryId(1);
        transfer.setDeliveryTag(new byte[] { 1 });
        transfer.setHandle(2);
        transfer.setMessageFormat(12);
        transfer.setMore(true);
        transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);
        transfer.setResume(true);
        transfer.setSettled(true);
        transfer.setState(Accepted.getInstance());

        assertNotNull(transfer.toString());
        assertEquals(11, transfer.getElementCount());
        assertFalse(transfer.isEmpty());
        assertTrue(transfer.hasAborted());
        assertTrue(transfer.hasBatchable());
        assertTrue(transfer.hasDeliveryId());
        assertTrue(transfer.hasDeliveryTag());
        assertTrue(transfer.hasHandle());
        assertTrue(transfer.hasMessageFormat());
        assertTrue(transfer.hasMore());
        assertTrue(transfer.hasRcvSettleMode());
        assertTrue(transfer.hasResume());
        assertTrue(transfer.hasSettled());
        assertTrue(transfer.hasState());

        assertTrue(transfer.hasElement(0));
        assertTrue(transfer.hasElement(1));
        assertTrue(transfer.hasElement(2));
        assertTrue(transfer.hasElement(3));
        assertTrue(transfer.hasElement(4));
        assertTrue(transfer.hasElement(5));
        assertTrue(transfer.hasElement(6));
        assertTrue(transfer.hasElement(7));
        assertTrue(transfer.hasElement(8));
        assertTrue(transfer.hasElement(9));
        assertTrue(transfer.hasElement(10));
        assertFalse(transfer.hasElement(11));

        transfer.clearAborted();
        transfer.clearBatchable();
        transfer.clearDeliveryId();
        transfer.clearDeliveryTag();
        transfer.clearHandle();
        transfer.clearMessageFormat();
        transfer.clearMore();
        transfer.clearRcvSettleMode();
        transfer.clearResume();
        transfer.clearSettled();
        transfer.clearState();

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

        assertFalse(transfer.hasElement(0));
        assertFalse(transfer.hasElement(1));
        assertFalse(transfer.hasElement(2));
        assertFalse(transfer.hasElement(3));
        assertFalse(transfer.hasElement(4));
        assertFalse(transfer.hasElement(5));
        assertFalse(transfer.hasElement(6));
        assertFalse(transfer.hasElement(7));
        assertFalse(transfer.hasElement(8));
        assertFalse(transfer.hasElement(9));
        assertFalse(transfer.hasElement(10));
        assertFalse(transfer.hasElement(11));

        transfer.setDeliveryTag(new byte[] { 1 });
        assertTrue(transfer.hasDeliveryTag());
        transfer.setDeliveryTag((byte[]) null);
        assertFalse(transfer.hasDeliveryTag());

        transfer.setDeliveryTag(new byte[] { 1 });
        assertTrue(transfer.hasDeliveryTag());
        transfer.setDeliveryTag((DeliveryTag) null);
        assertFalse(transfer.hasDeliveryTag());

        transfer.setDeliveryTag(new byte[] { 1 });
        assertTrue(transfer.hasDeliveryTag());
        transfer.setDeliveryTag((ProtonBuffer) null);
        assertFalse(transfer.hasDeliveryTag());

        transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);
        assertTrue(transfer.hasRcvSettleMode());
        transfer.setRcvSettleMode(null);
        assertFalse(transfer.hasRcvSettleMode());
    }

    @Test
    public void testCopy() {
        Transfer transfer = new Transfer();

        transfer.setAborted(true);
        transfer.setBatchable(true);
        transfer.setDeliveryId(1);
        transfer.setDeliveryTag(new byte[] { 1 });
        transfer.setHandle(2);
        transfer.setMessageFormat(12);
        transfer.setMore(true);
        transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);
        transfer.setResume(true);
        transfer.setSettled(true);
        transfer.setState(Accepted.getInstance());

        Transfer copy = transfer.copy();

        assertEquals(transfer.getAborted(), copy.getAborted());
        assertEquals(transfer.getBatchable(), copy.getBatchable());
        assertEquals(transfer.getDeliveryId(), copy.getDeliveryId());
        assertEquals(transfer.getDeliveryTag(), copy.getDeliveryTag());
        assertEquals(transfer.getHandle(), copy.getHandle());
        assertEquals(transfer.getMessageFormat(), copy.getMessageFormat());
        assertEquals(transfer.getMore(), copy.getMore());
        assertEquals(transfer.getRcvSettleMode(), copy.getRcvSettleMode());
        assertEquals(transfer.getResume(), copy.getResume());
        assertEquals(transfer.getSettled(), copy.getSettled());
        assertEquals(transfer.getState(), copy.getState());
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

        assertNotNull(transfer.toString());
        assertTrue(transfer.getElementCount() > 0);
        assertFalse(transfer.isEmpty());
        assertTrue(transfer.hasAborted());
        assertFalse(transfer.getAborted());
    }

    @Test
    public void testSetHandleEnforcesRange() {
        Transfer transfer = new Transfer();

        try {
            transfer.setHandle(-1l);
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
            transfer.setDeliveryId(-1l);
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
            transfer.setMessageFormat(-1l);
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
