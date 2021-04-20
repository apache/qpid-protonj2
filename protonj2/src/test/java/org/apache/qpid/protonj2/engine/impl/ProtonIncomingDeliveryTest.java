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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ProtonIncomingDeliveryTest extends ProtonEngineTestSupport {

    public static final int DEFAULT_MESSAGE_FORMAT = 0;

    @Test
    public void testToStringOnEmptyDeliveryDoesNotNPE() throws Exception {
        ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            Mockito.mock(ProtonReceiver.class), 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));
        assertNotNull(delivery.toString());
    }

    @Test
    public void testDefaultMessageFormat() throws Exception {
        ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            Mockito.mock(ProtonReceiver.class), 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));
        assertEquals(0L, DEFAULT_MESSAGE_FORMAT, "Unexpected value");
        assertEquals(DEFAULT_MESSAGE_FORMAT, delivery.getMessageFormat(), "Unexpected message format");
    }

    @Test
    public void testAvailable() throws Exception {
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);

        ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            Mockito.mock(ProtonReceiver.class), 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));
        delivery.appendTransferPayload(ProtonByteBufferAllocator.DEFAULT.wrap(data));

        // Check the full data is available
        assertNotNull(delivery, "expected the delivery to be present");
        assertEquals(data.length, delivery.available(), "unexpectd available count");

        // Extract some of the data as the receiver link will, check available gets reduced accordingly.
        int partLength = 2;
        int remainderLength = data.length - partLength;
        assertTrue(partLength < data.length);

        byte[] myRecievedData1 = new byte[partLength];

        delivery.readBytes(myRecievedData1, 0, myRecievedData1.length);
        assertEquals(remainderLength, delivery.available(), "Unexpected data length available");

        // Extract remainder of the data as the receiver link will, check available hits 0.
        byte[] myRecievedData2 = new byte[remainderLength];

        delivery.readBytes(myRecievedData2, 0, remainderLength);
        assertEquals(0, delivery.available(), "Expected no data to remain available");
    }

    @Test
    public void testAvailableWhenEmpty() throws Exception {
        ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            Mockito.mock(ProtonReceiver.class), 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));
        assertEquals(0, delivery.available());
    }

    @Test
    public void testAppendArraysToBuffer() throws Exception {
        ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            Mockito.mock(ProtonReceiver.class), 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        assertTrue(delivery.isFirstTransfer());
        assertEquals(0, delivery.getTransferCount());
        delivery.appendTransferPayload(ProtonByteBufferAllocator.DEFAULT.wrap(data1));
        assertTrue(delivery.isFirstTransfer());
        assertEquals(1, delivery.getTransferCount());
        delivery.appendTransferPayload(ProtonByteBufferAllocator.DEFAULT.wrap(data2));
        assertFalse(delivery.isFirstTransfer());
        assertEquals(2, delivery.getTransferCount());

        assertEquals(data1.length + data2.length, delivery.available());
        assertEquals(data1.length + data2.length, delivery.readAll().getReadableBytes());
        assertNull(delivery.readAll());
    }

    @Test
    public void testClaimAvailableBytesIndicatesAllBytesRead() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver).deliveryRead(delivery, 1024);
        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testReadAllAfterAllClaimedDoesNotClaimMore() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver).deliveryRead(delivery, 1024);

        assertNotNull(delivery.readAll());

        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testReadAllAfterAllClaimedSignalsBytesReadIfMoreDataArrived() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver).deliveryRead(delivery, 1024);

        delivery.appendTransferPayload(createProtonBuffer(512));
        delivery.appendTransferPayload(createProtonBuffer(1024));
        delivery.appendTransferPayload(createProtonBuffer(256));
        delivery.appendTransferPayload(createProtonBuffer(256));

        assertNotNull(delivery.readAll());

        Mockito.verify(receiver).deliveryRead(delivery, 2048);
        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testClaimAvailableBytesDoesNothingOnSecondCall() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver).deliveryRead(delivery, 1024);

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testClaimAvailableBytesIndicatesAllBytesReadAfterNewDelivery() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        delivery.appendTransferPayload(createProtonBuffer(512));

        assertEquals(1024 + 512, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver, times(1)).deliveryRead(delivery, 1024);
        Mockito.verify(receiver, times(1)).deliveryRead(delivery, 512);
        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testClaimAvailableBytesThenReadSomeAndExpectNoMoreClaimed() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        byte[] target = new byte[512];

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver, times(1)).deliveryRead(delivery, 1024);

        delivery.readBytes(target, 0, target.length);
        delivery.readBytes(target, 0, target.length);

        Mockito.verifyNoMoreInteractions(receiver);
    }

    @Test
    public void testClaimThenReadSomeGetMoreAndThenClaimAgain() throws Exception {
        final ProtonReceiver receiver = Mockito.mock(ProtonReceiver.class);
        final ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            receiver, 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));

        delivery.appendTransferPayload(createProtonBuffer(1024));

        byte[] target = new byte[2048];

        assertEquals(1024, delivery.available());
        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verify(receiver, times(1)).deliveryRead(delivery, 1024);

        delivery.appendTransferPayload(createProtonBuffer(1024));

        delivery.readBytes(target, 0, target.length);

        Mockito.verify(receiver, times(2)).deliveryRead(delivery, 1024);
        Mockito.verifyNoMoreInteractions(receiver);

        assertSame(delivery, delivery.claimAvailableBytes());

        Mockito.verifyNoMoreInteractions(receiver);
    }

    private ProtonBuffer createProtonBuffer(int available) {
        byte[] array = new byte[available];
        Arrays.fill(array, (byte) 65);
        return ProtonByteBufferAllocator.DEFAULT.wrap(array);
    }
}
