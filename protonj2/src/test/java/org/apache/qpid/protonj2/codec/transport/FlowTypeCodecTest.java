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
package org.apache.qpid.protonj2.codec.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.FlowTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.FlowTypeEncoder;
import org.apache.qpid.protonj2.types.UnsignedInteger;
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
    public void testCannotEncodeEmptyPerformative() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Flow input = new Flow();

        try {
            encoder.writeObject(buffer, encoderState, input);
            fail("Cannot omit required fields.");
        } catch (EncodeException encEx) {
        }
    }

    @Test
    public void testEncodeAndDecode() throws IOException {
        doTestEncodeAndDecode(false);
    }

    @Test
    public void testEncodeAndDecodeFromStream() throws IOException {
        doTestEncodeAndDecode(true);
    }

    private void doTestEncodeAndDecode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final Random random = new Random();
        random.setSeed(System.nanoTime());

        final int randomeNextIncomingId = random.nextInt();
        final int randomeNextOutgoingId = random.nextInt();
        final int randomeNextIncomingWindow = random.nextInt();
        final int randomeNextOutgoingWindow = random.nextInt();
        final int randomeHandle = random.nextInt();
        final int randomLinkCredit = random.nextInt();
        final int randomeAvailable = random.nextInt();
        final int randomeDeliveryCount = random.nextInt();

        Flow input = new Flow();

        input.setNextIncomingId(randomeNextIncomingId);
        input.setIncomingWindow(randomeNextIncomingWindow);
        input.setNextOutgoingId(randomeNextOutgoingId);
        input.setOutgoingWindow(randomeNextOutgoingWindow);
        input.setHandle(randomeHandle);
        input.setDeliveryCount(randomeDeliveryCount);
        input.setLinkCredit(randomLinkCredit);
        input.setAvailable(randomeAvailable);
        input.setDrain(true);
        input.setEcho(true);

        encoder.writeObject(buffer, encoderState, input);

        final Flow result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = (Flow) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Flow) decoder.readObject(buffer, decoderState);
        }

        assertEquals(Integer.toUnsignedLong(randomeNextIncomingId), result.getNextIncomingId());
        assertEquals(Integer.toUnsignedLong(randomeNextIncomingWindow), result.getIncomingWindow());
        assertEquals(Integer.toUnsignedLong(randomeNextOutgoingId), result.getNextOutgoingId());
        assertEquals(Integer.toUnsignedLong(randomeNextOutgoingWindow), result.getOutgoingWindow());
        assertEquals(Integer.toUnsignedLong(randomeHandle), result.getHandle());
        assertEquals(Integer.toUnsignedLong(randomeDeliveryCount), result.getDeliveryCount());
        assertEquals(Integer.toUnsignedLong(randomLinkCredit), result.getLinkCredit());
        assertEquals(Integer.toUnsignedLong(randomeAvailable), result.getAvailable());
        assertTrue(input.getDrain());
        assertTrue(input.getEcho());
        assertNull(input.getProperties());
    }

    @Test
    public void testSkipValue() throws IOException {
        testSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        testSkipValue(true);
    }

    private void testSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Flow flow = new Flow();

        flow.setNextIncomingId(1);
        flow.setIncomingWindow(2);
        flow.setNextOutgoingId(3);
        flow.setOutgoingWindow(4);
        flow.setHandle(UnsignedInteger.valueOf(10).longValue());
        flow.setDeliveryCount(5);
        flow.setLinkCredit(6);
        flow.setAvailable(7);
        flow.setDrain(false);
        flow.setEcho(false);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, flow);
        }

        flow.setNextIncomingId(10);
        flow.setIncomingWindow(20);
        flow.setNextOutgoingId(30);
        flow.setOutgoingWindow(40);
        flow.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        flow.setDeliveryCount(50);
        flow.setLinkCredit(60);
        flow.setAvailable(70);
        flow.setDrain(true);
        flow.setEcho(true);

        encoder.writeObject(buffer, encoderState, flow);

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Flow.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Flow.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
            }
        }

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Flow);

        Flow value = (Flow) result;
        assertEquals(10, value.getNextIncomingId());
        assertEquals(20, value.getIncomingWindow());
        assertEquals(30, value.getNextOutgoingId());
        assertEquals(40, value.getOutgoingWindow());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), value.getHandle());
        assertEquals(50, value.getDeliveryCount());
        assertEquals(60, value.getLinkCredit());
        assertEquals(70, value.getAvailable());
        assertTrue(value.getDrain());
        assertTrue(value.getEcho());
        assertNull(value.getProperties());
    }

    @Test
    public void testSkipValueWithInvalidMap32Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32, false);
    }

    @Test
    public void testSkipValueWithInvalidMap8Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8, false);
    }

    @Test
    public void testSkipValueWithInvalidMap32TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32, true);
    }

    @Test
    public void testSkipValueWithInvalidMap8TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8, true);
    }

    private void doTestSkipValueWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Flow.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Flow.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodedWithInvalidMap32Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, false);
    }

    @Test
    public void testDecodeWithInvalidMap8Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, false);
    }

    @Test
    public void testDecodedWithInvalidMap32TypeFromStream() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, true);
    }

    @Test
    public void testDecodeWithInvalidMap8TypeFromStream() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, true);
    }

    private void doTestDecodeWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("Should not decode type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("Should not decode type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        testEncodeDecodeArray(false);
    }

    @Test
    public void testEncodeDecodeArrayFromStream() throws IOException {
        testEncodeDecodeArray(true);
    }

    private void testEncodeDecodeArray(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Flow[] array = new Flow[3];

        array[0] = new Flow();
        array[1] = new Flow();
        array[2] = new Flow();

        array[0].setHandle(0).setLinkCredit(0).setDeliveryCount(1).setIncomingWindow(1024).setNextOutgoingId(1).setOutgoingWindow(128);
        array[1].setHandle(1).setLinkCredit(1).setDeliveryCount(1).setIncomingWindow(2048).setNextOutgoingId(2).setOutgoingWindow(256);
        array[2].setHandle(2).setLinkCredit(2).setDeliveryCount(1).setIncomingWindow(4096).setNextOutgoingId(3).setOutgoingWindow(512);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Flow.class, result.getClass().getComponentType());

        Flow[] resultArray = (Flow[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Flow);
            assertEquals(array[i].getHandle(), resultArray[i].getHandle());
            assertEquals(array[i].getLinkCredit(), resultArray[i].getLinkCredit());
            assertEquals(array[i].getDeliveryCount(), resultArray[i].getDeliveryCount());
        }
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList8() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST8, false);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList32() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST32, false);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList0FromStream() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST0, true);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList8FromStream() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList32FromStream() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST32, true);
    }

    private void doTestDecodeWithNotEnoughListEntriesList32(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Flow.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else if (listType == EncodingCodes.LIST8) {
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.LIST0);
        }

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodeWithToManyListEntriesList8() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST8, false);
    }

    @Test
    public void testDecodeWithToManyListEntriesList32() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST32, false);
    }

    @Test
    public void testDecodeWithToManyListEntriesList8FromStream() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeWithToManyListEntriesList32FromStream() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST32, true);
    }

    private void doTestDecodeWithToManyListEntriesList32(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Flow.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt(128);  // Size
            buffer.writeInt(127);  // Count
        } else if (listType == EncodingCodes.LIST8) {
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 128);  // Size
            buffer.writeByte((byte) 127);  // Count
        }

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        }
    }
}
