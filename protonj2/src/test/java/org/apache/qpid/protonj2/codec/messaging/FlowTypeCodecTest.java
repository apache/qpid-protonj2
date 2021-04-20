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
package org.apache.qpid.protonj2.codec.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.FlowTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.FlowTypeEncoder;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.junit.jupiter.api.Test;

public class FlowTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Flow.class, new FlowTypeDecoder().getTypeClass());
        assertEquals(Flow.class, new FlowTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Flow.DESCRIPTOR_CODE, new FlowTypeDecoder().getDescriptorCode());
        assertEquals(Flow.DESCRIPTOR_CODE, new FlowTypeEncoder().getDescriptorCode());
        assertEquals(Flow.DESCRIPTOR_SYMBOL, new FlowTypeDecoder().getDescriptorSymbol());
        assertEquals(Flow.DESCRIPTOR_SYMBOL, new FlowTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeFlow() throws IOException {
        doTestDecodeFlowSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfFlows() throws IOException {
        doTestDecodeFlowSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfFlows() throws IOException {
        doTestDecodeFlowSeries(LARGE_SIZE);
    }

    private void doTestDecodeFlowSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Flow flow = new Flow();
        flow.setNextIncomingId(1);
        flow.setIncomingWindow(2047);
        flow.setNextOutgoingId(1);
        flow.setOutgoingWindow(UnsignedInteger.MAX_VALUE.longValue());
        flow.setHandle(0);
        flow.setDeliveryCount(10);
        flow.setLinkCredit(1000);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, flow);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Flow);

            Flow decoded = (Flow) result;

            assertEquals(flow.getNextIncomingId(), decoded.getNextIncomingId());
            assertEquals(flow.getIncomingWindow(), decoded.getIncomingWindow());
            assertEquals(flow.getNextOutgoingId(), decoded.getNextOutgoingId());
            assertEquals(flow.getOutgoingWindow(), decoded.getOutgoingWindow());
            assertEquals(flow.getHandle(), decoded.getHandle());
            assertEquals(flow.getDeliveryCount(), decoded.getDeliveryCount());
            assertEquals(flow.getLinkCredit(), decoded.getLinkCredit());
        }
    }

    @Test
    public void testEncodeDecodeArrayOfDataSections() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Flow flow = new Flow();
        flow.setNextIncomingId(1);
        flow.setIncomingWindow(2047);
        flow.setNextOutgoingId(1);
        flow.setOutgoingWindow(UnsignedInteger.MAX_VALUE.longValue());
        flow.setHandle(0);
        flow.setDeliveryCount(10);
        flow.setLinkCredit(1000);

        Flow[] flowArray = new Flow[3];

        flowArray[0] = flow;
        flowArray[1] = flow;
        flowArray[2] = flow;

        encoder.writeObject(buffer, encoderState, flowArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Flow.class, result.getClass().getComponentType());

        Flow[] resultArray = (Flow[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Flow);
            assertEquals(flowArray[i].getNextIncomingId(), resultArray[i].getNextIncomingId());
            assertEquals(flowArray[i].getIncomingWindow(), resultArray[i].getIncomingWindow());
            assertEquals(flowArray[i].getNextOutgoingId(), resultArray[i].getNextOutgoingId());
            assertEquals(flowArray[i].getOutgoingWindow(), resultArray[i].getOutgoingWindow());
            assertEquals(flowArray[i].getHandle(), resultArray[i].getHandle());
            assertEquals(flowArray[i].getDeliveryCount(), resultArray[i].getDeliveryCount());
            assertEquals(flowArray[i].getLinkCredit(), resultArray[i].getLinkCredit());
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new Flow().setAvailable(100));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Flow.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
    }

    @Test
    public void testDecodeWithInvalidMap32Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32);
    }

    @Test
    public void testDecodeWithInvalidMap8Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8);
    }

    private void doTestDecodeWithInvalidMapType(byte mapType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Flow.DESCRIPTOR_CODE.byteValue());
        if (mapType == EncodingCodes.MAP32) {
            buffer.writeByte(EncodingCodes.MAP32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.MAP8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        }

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testSkipValueWithInvalidMap32Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32);
    }

    @Test
    public void testSkipValueWithInvalidMap8Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8);
    }

    private void doTestSkipValueWithInvalidMapType(byte mapType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Flow.DESCRIPTOR_CODE.byteValue());
        if (mapType == EncodingCodes.MAP32) {
            buffer.writeByte(EncodingCodes.MAP32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.MAP8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        }

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(Flow.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Flow[] array = new Flow[3];

        array[0] = new Flow();
        array[1] = new Flow();
        array[2] = new Flow();

        array[0].setHandle(0).setLinkCredit(0).setDeliveryCount(1).setIncomingWindow(1024).setNextOutgoingId(1).setOutgoingWindow(128);
        array[1].setHandle(1).setLinkCredit(1).setDeliveryCount(1).setIncomingWindow(2048).setNextOutgoingId(2).setOutgoingWindow(256);
        array[2].setHandle(2).setLinkCredit(2).setDeliveryCount(1).setIncomingWindow(4096).setNextOutgoingId(3).setOutgoingWindow(512);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Flow.class, result.getClass().getComponentType());

        Flow[] resultArray = (Flow[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Flow);
            assertEquals(array[i].getHandle(), resultArray[i].getHandle());
        }
    }
}
