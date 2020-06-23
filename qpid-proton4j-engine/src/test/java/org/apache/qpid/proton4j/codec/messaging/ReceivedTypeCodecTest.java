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
package org.apache.qpid.proton4j.codec.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ReceivedTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ReceivedTypeEncoder;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.messaging.Modified;
import org.apache.qpid.proton4j.types.messaging.Received;
import org.junit.Test;

/**
 * Test codec handling of Received types.
 */
public class ReceivedTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Received.class, new ReceivedTypeDecoder().getTypeClass());
        assertEquals(Received.class, new ReceivedTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Received.DESCRIPTOR_CODE, new ReceivedTypeDecoder().getDescriptorCode());
        assertEquals(Received.DESCRIPTOR_CODE, new ReceivedTypeEncoder().getDescriptorCode());
        assertEquals(Received.DESCRIPTOR_SYMBOL, new ReceivedTypeDecoder().getDescriptorSymbol());
        assertEquals(Received.DESCRIPTOR_SYMBOL, new ReceivedTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void TestDecodeRecieved() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Received value = new Received();
        value.setSectionNumber(UnsignedInteger.ONE);
        value.setSectionOffset(UnsignedLong.ZERO);

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Received);
    }

    @Test
    public void TestDecodeReceivedWithList8() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Received.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 5);  // Size
        buffer.writeByte((byte) 2);  // Count
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(0);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Received);
    }

    @Test
    public void TestDecodeReceivedWithList32() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Received.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(8);  // Size
        buffer.writeInt(2);  // Count
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(0);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Received);
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Received received = new Received();
        received.setSectionNumber(UnsignedInteger.ONE);
        received.setSectionOffset(UnsignedLong.ZERO);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, received);
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Received.class, typeDecoder.getTypeClass());
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
        buffer.writeByte(Received.DESCRIPTOR_CODE.byteValue());
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
        buffer.writeByte(Received.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Received.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Received[] array = new Received[3];

        array[0] = new Received();
        array[1] = new Received();
        array[2] = new Received();

        array[0].setSectionNumber(UnsignedInteger.ONE).setSectionOffset(UnsignedLong.ZERO);
        array[1].setSectionNumber(UnsignedInteger.ONE).setSectionOffset(UnsignedLong.ZERO);
        array[2].setSectionNumber(UnsignedInteger.ZERO).setSectionOffset(UnsignedLong.ZERO);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Received.class, result.getClass().getComponentType());

        Received[] resultArray = (Received[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Received);
            assertEquals(array[i].getSectionNumber(), resultArray[i].getSectionNumber());
            assertEquals(array[i].getSectionOffset(), resultArray[i].getSectionOffset());
        }
    }
}
