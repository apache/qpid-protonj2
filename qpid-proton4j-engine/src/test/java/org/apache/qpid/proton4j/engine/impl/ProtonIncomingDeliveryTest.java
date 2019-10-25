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
package org.apache.qpid.proton4j.engine.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.junit.Test;
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
        assertEquals("Unexpected value", 0L, DEFAULT_MESSAGE_FORMAT);
        assertEquals("Unexpected message format", DEFAULT_MESSAGE_FORMAT, delivery.getMessageFormat());
    }

    @Test
    public void testAvailable() throws Exception {
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);

        ProtonIncomingDelivery delivery = new ProtonIncomingDelivery(
            Mockito.mock(ProtonReceiver.class), 1, new DeliveryTag.ProtonDeliveryTag(new byte[] {0}));
        delivery.appendTransferPayload(ProtonByteBufferAllocator.DEFAULT.wrap(data));

        // Check the full data is available
        assertNotNull("expected the delivery to be present", delivery);
        assertEquals("unexpectd available count", data.length, delivery.available());

        // Extract some of the data as the receiver link will, check available gets reduced accordingly.
        int partLength = 2;
        int remainderLength = data.length - partLength;
        assertTrue(partLength < data.length);

        byte[] myRecievedData1 = new byte[partLength];

        delivery.readBytes(myRecievedData1, 0, myRecievedData1.length);
        assertEquals("Unexpected data length available", remainderLength, delivery.available());

        // Extract remainder of the data as the receiver link will, check available hits 0.
        byte[] myRecievedData2 = new byte[remainderLength];

        delivery.readBytes(myRecievedData2, 0, remainderLength);
        assertEquals("Expected no data to remain available", 0, delivery.available());
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
}
