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

import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.junit.Test;

public class UnsignedByteTypeCodecTest extends CodecTestSupport {

    @Test
    public void testLookupTypeDecoderForType() throws Exception {
        TypeDecoder<?> result = decoder.getTypeDecoder(UnsignedByte.valueOf((byte) 127));

        assertNotNull(result);
        assertEquals(UnsignedByte.class, result.getTypeClass());
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readUnsignedByte(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}

        try {
            decoder.readUnsignedByte(buffer, decoderState, (byte) 0);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 42);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(42, decoder.readUnsignedByte(buffer, decoderState).intValue());
        assertEquals(43, decoder.readUnsignedByte(buffer, decoderState, (byte) 42));
        assertNull(decoder.readUnsignedByte(buffer, decoderState));
        assertEquals(42, decoder.readUnsignedByte(buffer, decoderState, (byte) 42));
    }

    @Test
    public void testEncodeDecodeUnsignedByte() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedByte(buffer, encoderState, UnsignedByte.valueOf((byte) 64));

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedByte);
        assertEquals(64, ((UnsignedByte) result).byteValue());
    }

    @Test
    public void testEncodeDecodeByte() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 64);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedByte);
        assertEquals(64, ((UnsignedByte) result).byteValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedBytes() throws IOException {
        doTestDecodeUnsignedByteSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedBytes() throws IOException {
        doTestDecodeUnsignedByteSeries(LARGE_SIZE);
    }

    private void doTestDecodeUnsignedByteSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedByte(buffer, encoderState, (byte)(i % 255));
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedByte result = decoder.readUnsignedByte(buffer, decoderState);

            assertNotNull(result);
            assertEquals((byte)(i % 255), result.byteValue());
        }
    }

    @Test
    public void testArrayOfUnsignedByteObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        UnsignedByte[] source = new UnsignedByte[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedByte.valueOf((byte) (i % 255));
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedByte[] array = (UnsignedByte[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfUnsignedByteObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UnsignedByte[] source = new UnsignedByte[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedByte[] array = (UnsignedByte[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeEncodedBytes() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte(255);

        UnsignedByte result1 = decoder.readUnsignedByte(buffer, decoderState);
        UnsignedByte result2 = decoder.readUnsignedByte(buffer, decoderState);
        UnsignedByte result3 = decoder.readUnsignedByte(buffer, decoderState);

        assertEquals(UnsignedByte.valueOf((byte) 0), result1);
        assertEquals(UnsignedByte.valueOf((byte) 127), result2);
        assertEquals(UnsignedByte.valueOf((byte) 255), result3);
    }

    @Test
    public void testDecodeEncodedBytesAsPrimitive() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte(255);

        byte result1 = decoder.readUnsignedByte(buffer, decoderState, (byte) 1);
        byte result2 = decoder.readUnsignedByte(buffer, decoderState, (byte) 105);
        byte result3 = decoder.readUnsignedByte(buffer, decoderState, (byte) 200);

        assertEquals((byte) 0, result1);
        assertEquals((byte) 127, result2);
        assertEquals((byte) 255, result3);
    }

    @Test
    public void testDecodeBooleanFromNullEncoding() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 1);
        encoder.writeNull(buffer, encoderState);

        UnsignedByte result1 = decoder.readUnsignedByte(buffer, decoderState);
        UnsignedByte result2 = decoder.readUnsignedByte(buffer, decoderState);

        assertEquals(UnsignedByte.valueOf((byte) 1), result1);
        assertNull(result2);
    }

    @Test
    public void testDecodeBooleanAsPrimitiveWithDefault() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 27);
        encoder.writeNull(buffer, encoderState);

        byte result = decoder.readUnsignedByte(buffer, decoderState, (byte) 0);
        assertEquals((byte) 27, result);
        result = decoder.readUnsignedByte(buffer, decoderState, (byte) 127);
        assertEquals((byte) 127, result);
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeUnsignedByte(buffer, encoderState, UnsignedByte.valueOf((byte) i));
        }

        UnsignedByte expected = UnsignedByte.valueOf((byte) 42);

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedByte.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnsignedByte);

        UnsignedByte value = (UnsignedByte) result;
        assertEquals(expected, value);
    }
}
