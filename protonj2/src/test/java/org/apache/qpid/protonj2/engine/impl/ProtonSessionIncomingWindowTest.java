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

package org.apache.qpid.protonj2.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Transfer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test the proton incoming window class for specification compliant behavior
 */
@ExtendWith(MockitoExtension.class)
class ProtonSessionIncomingWindowTest {

    private static final int DEFAULT_SESSION_CAPACITY = 100_000;

    private static final int DEFAULT_MAX_FRAME_SIZE = 100_000;

    @Mock
    ProtonConnection connection;

    @Mock
    ProtonSession session;

    @Mock
    ProtonReceiver receiver;

    @Mock
    ProtonIncomingDelivery delivery;

    ProtonSessionIncomingWindow window;

    final Begin remoteBegin = new Begin();
    final Begin localBegin = new Begin();

    @BeforeEach
    public void setUp() throws Exception {
        when(session.getConnection()).thenReturn(connection);
        when(session.getConnection().getMaxFrameSize()).thenReturn((long) DEFAULT_MAX_FRAME_SIZE);

        window = new ProtonSessionIncomingWindow(session);
    }

    @Test
    public void testStateAfterBasicBeginExchange() {
        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(UnsignedInteger.MAX_VALUE.intValue());

        window.configureOutbound(localBegin);
        window.handleBegin(remoteBegin);

        assertEquals(UnsignedInteger.MAX_VALUE.intValue(), window.getNextIncomingId());
        assertEquals(0, window.getIncomingCapacity());
        assertEquals(Integer.MAX_VALUE, window.getRemainingIncomingCapacity());

        window.setIncomingCapacity(DEFAULT_SESSION_CAPACITY);

        assertEquals(DEFAULT_SESSION_CAPACITY, window.getIncomingCapacity());
    }

    @Test
    public void testIncomingWindowUpdates() {
        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(0);

        window.configureOutbound(localBegin);
        assertEquals(Integer.MAX_VALUE, localBegin.getIncomingWindow());
        window.handleBegin(remoteBegin);
        assertEquals(Integer.MAX_VALUE, window.getIncomingWindow());

        // Less than one frame worth of capacity
        window.setIncomingCapacity(1);
        window.configureOutbound(localBegin);
        assertEquals(0, localBegin.getIncomingWindow());

        // Exactly one frame worth of capacity
        window.setIncomingCapacity(DEFAULT_MAX_FRAME_SIZE);
        window.configureOutbound(localBegin);
        assertEquals(1, localBegin.getIncomingWindow());

        // Little more than one frame worth of capacity
        window.setIncomingCapacity(DEFAULT_MAX_FRAME_SIZE + 10);
        window.configureOutbound(localBegin);
        assertEquals(1, localBegin.getIncomingWindow());

        // Two frame worth of capacity
        window.setIncomingCapacity(DEFAULT_MAX_FRAME_SIZE * 2);
        window.configureOutbound(localBegin);
        assertEquals(2, localBegin.getIncomingWindow());
    }

    @Test
    public void testIncomingWindowUpdatesFrameSizeGreaterThanIntMaxVale() {
        when(session.getConnection().getMaxFrameSize()).thenReturn((long) Integer.MAX_VALUE + 100);

        window = new ProtonSessionIncomingWindow(session);

        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(0);

        window.configureOutbound(localBegin);
        assertEquals(Integer.MAX_VALUE, localBegin.getIncomingWindow());
        window.handleBegin(remoteBegin);
        assertEquals(Integer.MAX_VALUE, window.getIncomingWindow());

        // Less than one frame worth of capacity
        window.setIncomingCapacity(Integer.MAX_VALUE);
        window.configureOutbound(localBegin);
        assertEquals(0, localBegin.getIncomingWindow());

        // Exactly one frame worth of capacity
        window.setIncomingCapacity(Integer.MAX_VALUE + 100);
        window.configureOutbound(localBegin);
        assertEquals(Integer.MAX_VALUE, localBegin.getIncomingWindow());

        // Little more than one frame worth of capacity
        window.setIncomingCapacity(Integer.MAX_VALUE + 200);
        window.configureOutbound(localBegin);
        assertEquals(Integer.MAX_VALUE, localBegin.getIncomingWindow());

        // Two frame worth of capacity
        window.setIncomingCapacity(Integer.MAX_VALUE * 2);
        window.configureOutbound(localBegin);
        assertEquals(Integer.MAX_VALUE, localBegin.getIncomingWindow());
    }

    @Test
    public void testTransferHandlingIncomingCapacityUpdates() {
        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(0);

        window.setIncomingCapacity(DEFAULT_SESSION_CAPACITY);
        window.configureOutbound(localBegin);
        window.handleBegin(remoteBegin);

        when(receiver.remoteTransfer(any(), any())).thenReturn(delivery);

        assertEquals(0, window.getNextIncomingId());
        assertEquals(DEFAULT_SESSION_CAPACITY, window.getIncomingCapacity());
        assertEquals(DEFAULT_SESSION_CAPACITY, window.getRemainingIncomingCapacity());

        final Transfer transfer = new Transfer();
        final ProtonBuffer payload = ProtonBufferAllocator.defaultAllocator().allocateHeapBuffer(10).setWriteOffset(10);

        window.handleTransfer(receiver, transfer, payload);

        assertEquals(DEFAULT_SESSION_CAPACITY - payload.getReadableBytes(), window.getRemainingIncomingCapacity());

        window.deliveryRead(delivery, payload.getReadableBytes());

        assertEquals(DEFAULT_SESSION_CAPACITY, window.getIncomingCapacity());
    }

    @Test
    public void testNextIncomingIdStartingAtZero() {
        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(0);

        window.setIncomingCapacity(DEFAULT_SESSION_CAPACITY * 3);
        window.configureOutbound(localBegin);
        window.handleBegin(remoteBegin);

        when(receiver.remoteTransfer(any(), any())).thenReturn(delivery);

        final Transfer transfer = new Transfer();
        final ProtonBuffer payload = ProtonBufferAllocator.defaultAllocator().allocateHeapBuffer();

        payload.writeInt(255);

        assertEquals(0, window.getNextIncomingId());
        assertEquals(3, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(1, window.getNextIncomingId());
        assertEquals(2, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(2, window.getNextIncomingId());
        assertEquals(1, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(3, window.getNextIncomingId());
        assertEquals(0, window.getIncomingWindow());
    }

    @Test
    public void testNextIncomingIdStartingAtMaxUInt() {
        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(UnsignedInteger.MAX_VALUE.intValue());

        window.setIncomingCapacity(DEFAULT_SESSION_CAPACITY * 20);
        window.configureOutbound(localBegin);
        window.handleBegin(remoteBegin);

        when(receiver.remoteTransfer(any(), any())).thenReturn(delivery);

        final Transfer transfer = new Transfer();
        final ProtonBuffer payload = ProtonBufferAllocator.defaultAllocator().allocateHeapBuffer();

        payload.writeLong(32767);

        assertEquals(UnsignedInteger.MAX_VALUE.intValue(), window.getNextIncomingId());
        assertEquals(20, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(0, window.getNextIncomingId());
        assertEquals(19, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(1, window.getNextIncomingId());
        assertEquals(18, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(2, window.getNextIncomingId());
        assertEquals(17, window.getIncomingWindow());
    }

    @Test
    public void testIncomingWindowSetToUIntMaxValueCapsMaxToIntMaxValue() {
        localBegin.setNextOutgoingId(0);
        remoteBegin.setNextOutgoingId(Integer.MAX_VALUE);

        window.setIncomingCapacity(UnsignedInteger.MAX_VALUE.intValue());
        window.configureOutbound(localBegin);
        window.handleBegin(remoteBegin);

        when(receiver.remoteTransfer(any(), any())).thenReturn(delivery);

        final Transfer transfer = new Transfer();
        final ProtonBuffer payload = ProtonBufferAllocator.defaultAllocator().allocateHeapBuffer();

        payload.writeBoolean(false);

        assertEquals(Integer.MAX_VALUE, window.getNextIncomingId());
        assertEquals(Integer.MAX_VALUE, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(Integer.MAX_VALUE + 1, window.getNextIncomingId());
        assertEquals(Integer.MAX_VALUE - 1, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(Integer.MAX_VALUE + 2, window.getNextIncomingId());
        assertEquals(Integer.MAX_VALUE - 2, window.getIncomingWindow());
        window.handleTransfer(receiver, transfer, payload);
        assertEquals(Integer.MAX_VALUE + 3, window.getNextIncomingId());
        assertEquals(Integer.MAX_VALUE - 3, window.getIncomingWindow());
    }
}
