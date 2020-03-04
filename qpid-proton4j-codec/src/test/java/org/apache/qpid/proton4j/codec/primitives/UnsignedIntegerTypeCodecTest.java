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
package org.apache.qpid.proton4j.codec.primitives;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.junit.Test;

public class UnsignedIntegerTypeCodecTest extends CodecTestSupport {

    @Test
    public void testLookupTypeDecoderForType() throws Exception {
        TypeDecoder<?> result = decoder.getTypeDecoder(UnsignedInteger.valueOf(127));

        assertNotNull(result);
        assertEquals(UnsignedInteger.class, result.getTypeClass());
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeByte(EncodingCodes.ULONG);

        try {
            decoder.readUnsignedInteger(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}

        try {
            decoder.readUnsignedInteger(buffer, decoderState, (long) 0);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}

        try {
            decoder.readUnsignedInteger(buffer, decoderState, 0);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT0);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeInt(42);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(0, decoder.readUnsignedInteger(buffer, decoderState).intValue());
        assertEquals(42, decoder.readUnsignedInteger(buffer, decoderState).intValue());
        assertEquals(43, decoder.readUnsignedInteger(buffer, decoderState, 42));
        assertNull(decoder.readUnsignedInteger(buffer, decoderState));
        assertEquals(42, decoder.readUnsignedInteger(buffer, decoderState, 42));
        assertEquals(42, decoder.readUnsignedInteger(buffer, decoderState, (long) 42));
    }

    @Test
    public void testEncodeDecodeUnsignedInteger() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(640));

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(640, ((UnsignedInteger) result).intValue());
    }

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 640);
        encoder.writeUnsignedInteger(buffer, encoderState, 0);
        encoder.writeUnsignedInteger(buffer, encoderState, 255);
        encoder.writeUnsignedInteger(buffer, encoderState, 254);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(640, ((UnsignedInteger) result).intValue());
        result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(0, ((UnsignedInteger) result).intValue());
        result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(255, ((UnsignedInteger) result).intValue());
        result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(254, ((UnsignedInteger) result).intValue());
    }

    @Test
    public void testEncodeDecodeLong() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 640l);
        encoder.writeUnsignedInteger(buffer, encoderState, 0l);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(640, ((UnsignedInteger) result).intValue());
        result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(0, ((UnsignedInteger) result).intValue());

        try {
            encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Should fail on value out of range");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testEncodeDecodeByte() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, (byte) 64);
        encoder.writeUnsignedInteger(buffer, encoderState, (byte) 0);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(64, ((UnsignedInteger) result).byteValue());
        result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(0, ((UnsignedInteger) result).byteValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedIntegers() throws IOException {
        doTestDecodeUnsignedIntegerSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedIntegers() throws IOException {
        doTestDecodeUnsignedIntegerSeries(LARGE_SIZE);
    }

    private void doTestDecodeUnsignedIntegerSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedInteger(buffer, encoderState, i);
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedInteger result = decoder.readUnsignedInteger(buffer, decoderState);

            assertNotNull(result);
            assertEquals(i, result.intValue());
        }
    }

    @Test
    public void testArrayOfUnsignedIntegerObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        UnsignedInteger[] source = new UnsignedInteger[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedInteger.valueOf(i);
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedInteger[] array = (UnsignedInteger[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfUnsignedIntegerObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UnsignedInteger[] source = new UnsignedInteger[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedInteger[] array = (UnsignedInteger[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(Integer.MAX_VALUE - i));
            encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(i));
        }

        UnsignedInteger expected = UnsignedInteger.valueOf(42);

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnsignedInteger);

        UnsignedInteger value = (UnsignedInteger) result;
        assertEquals(expected, value);
    }
}
