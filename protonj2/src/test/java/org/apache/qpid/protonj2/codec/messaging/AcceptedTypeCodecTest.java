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
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.AcceptedTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.AcceptedTypeEncoder;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

/**
 * Test codec handling of Accepted types.
 */
public class AcceptedTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Accepted.class, new AcceptedTypeDecoder().getTypeClass());
        assertEquals(Accepted.class, new AcceptedTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Accepted.DESCRIPTOR_CODE, new AcceptedTypeDecoder().getDescriptorCode());
        assertEquals(Accepted.DESCRIPTOR_CODE, new AcceptedTypeEncoder().getDescriptorCode());
        assertEquals(Accepted.DESCRIPTOR_SYMBOL, new AcceptedTypeDecoder().getDescriptorSymbol());
        assertEquals(Accepted.DESCRIPTOR_SYMBOL, new AcceptedTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeAccepted() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Accepted value = Accepted.getInstance();

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Accepted);

        Accepted decoded = (Accepted) result;

        assertEquals(value, decoded);
    }

    @Test
    public void testDecodeAcceptedFromStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Accepted value = Accepted.getInstance();

        encoder.writeObject(buffer, encoderState, value);

        final Object result = streamDecoder.readObject(stream, streamDecoderState);

        assertNotNull(result);
        assertTrue(result instanceof Accepted);

        Accepted decoded = (Accepted) result;

        assertEquals(value, decoded);
    }

    @Test
    public void testDecodeAcceptedWithList8() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 0);  // Size
        buffer.writeByte((byte) 0);  // Count

        Accepted value = Accepted.getInstance();

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Accepted);

        Accepted decoded = (Accepted) result;

        assertEquals(value, decoded);
    }

    @Test
    public void testDecodeAcceptedWithList8FromStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 0);  // Size
        buffer.writeByte((byte) 0);  // Count

        Accepted value = Accepted.getInstance();

        final Object result = streamDecoder.readObject(stream, streamDecoderState);

        assertNotNull(result);
        assertTrue(result instanceof Accepted);

        Accepted decoded = (Accepted) result;

        assertEquals(value, decoded);
    }

    @Test
    public void testDecodeAcceptedWithList32() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt((byte) 0);  // Size
        buffer.writeInt((byte) 0);  // Count

        Accepted value = Accepted.getInstance();

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Accepted);

        Accepted decoded = (Accepted) result;

        assertEquals(value, decoded);
    }

    @Test
    public void testDecodeAcceptedWithList32FromStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt((byte) 0);  // Size
        buffer.writeInt((byte) 0);  // Count

        Accepted value = Accepted.getInstance();

        final Object result = streamDecoder.readObject(stream, streamDecoderState);

        assertNotNull(result);
        assertTrue(result instanceof Accepted);

        Accepted decoded = (Accepted) result;

        assertEquals(value, decoded);
    }

    @Test
    public void testDecodeAcceptedWithInvalidMap32Type() throws IOException {
        doTestDecodeAcceptedWithInvalidMapType(EncodingCodes.MAP32, false);
    }

    @Test
    public void testDecodeAcceptedWithInvalidMap8Type() throws IOException {
        doTestDecodeAcceptedWithInvalidMapType(EncodingCodes.MAP8, false);
    }

    @Test
    public void testDecodeAcceptedWithInvalidMap32TypeFromStream() throws IOException {
        doTestDecodeAcceptedWithInvalidMapType(EncodingCodes.MAP32, true);
    }

    @Test
    public void testDecodeAcceptedWithInvalidMap8TypeFromStream() throws IOException {
        doTestDecodeAcceptedWithInvalidMapType(EncodingCodes.MAP8, true);
    }

    private void doTestDecodeAcceptedWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
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
            if (fromStream) {
                streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoder.readObject(buffer, decoderState);
            }
            fail("Should not decode type with invalid encoding");
        } catch (DecodeException ex) {}
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
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
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Accepted.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Accepted.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, Accepted.getInstance());
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Accepted.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Accepted[] array = new Accepted[3];

        array[0] = Accepted.getInstance();
        array[1] = Accepted.getInstance();
        array[2] = Accepted.getInstance();

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Accepted.class, result.getClass().getComponentType());

        Accepted[] resultArray = (Accepted[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Accepted);
        }
    }
}
