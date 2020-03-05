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

import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.junit.Test;

public class UnsignedLongTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readUnsignedLong(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}

        try {
            decoder.readUnsignedLong(buffer, decoderState, (short) 0);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.ULONG0);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(42);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(0, decoder.readUnsignedLong(buffer, decoderState).intValue());
        assertEquals(42, decoder.readUnsignedLong(buffer, decoderState).intValue());
        assertEquals(43, decoder.readUnsignedLong(buffer, decoderState, 42));
        assertNull(decoder.readUnsignedLong(buffer, decoderState));
        assertEquals(42, decoder.readUnsignedLong(buffer, decoderState, 42));
    }

    @Test
    public void testEncodeDecodeUnsignedLong() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(640));

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedLong);
        assertEquals(640, ((UnsignedLong) result).intValue());
    }

    @Test
    public void testEncodeDecodePrimitive() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, 640l);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedLong);
        assertEquals(640, ((UnsignedLong) result).intValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedLongs() throws IOException {
        doTestDecodeUnsignedLongSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedLongs() throws IOException {
        doTestDecodeUnsignedLongSeries(LARGE_SIZE);
    }

    private void doTestDecodeUnsignedLongSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(i));
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedLong result = decoder.readUnsignedLong(buffer, decoderState);

            assertNotNull(result);
            assertEquals(i, result.intValue());
        }
    }

    @Test
    public void testArrayOfUnsignedLongObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        UnsignedLong[] source = new UnsignedLong[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedLong.valueOf(i);
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedLong[] array = (UnsignedLong[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfUnsignedLongObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UnsignedLong[] source = new UnsignedLong[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedLong[] array = (UnsignedLong[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeEncodedBytes() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.ULONG0);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(255);

        UnsignedLong result1 = decoder.readUnsignedLong(buffer, decoderState);
        UnsignedLong result2 = decoder.readUnsignedLong(buffer, decoderState);
        UnsignedLong result3 = decoder.readUnsignedLong(buffer, decoderState);

        assertEquals(UnsignedLong.valueOf(0), result1);
        assertEquals(UnsignedLong.valueOf(127), result2);
        assertEquals(UnsignedLong.valueOf(255), result3);
    }

    @Test
    public void testDecodeEncodedBytesAsPrimitive() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.ULONG0);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(255);

        long result1 = decoder.readUnsignedLong(buffer, decoderState, 1);
        long result2 = decoder.readUnsignedLong(buffer, decoderState, 105);
        long result3 = decoder.readUnsignedLong(buffer, decoderState, 200);

        assertEquals(0, result1);
        assertEquals(127, result2);
        assertEquals(255, result3);
    }

    @Test
    public void testDecodeBooleanFromNullEncoding() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, (byte) 1);
        encoder.writeNull(buffer, encoderState);

        UnsignedLong result1 = decoder.readUnsignedLong(buffer, decoderState);
        UnsignedLong result2 = decoder.readUnsignedLong(buffer, decoderState);

        assertEquals(UnsignedLong.valueOf(1), result1);
        assertNull(result2);
    }

    @Test
    public void testDecodeBooleanAsPrimitiveWithDefault() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, 27);
        encoder.writeNull(buffer, encoderState);

        long result = decoder.readUnsignedLong(buffer, decoderState, 0);
        assertEquals(27, result);
        result = decoder.readUnsignedLong(buffer, decoderState, 127);
        assertEquals(127, result);
    }

    @Test
    public void testWriteLongZeroEncodesAsOneByte() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, 0l);
        assertEquals(1, buffer.getReadableBytes());
        assertEquals(EncodingCodes.ULONG0, buffer.readByte());
    }

    @Test
    public void testWriteLongValuesInSmallULongRange() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, 1l);
        assertEquals(2, buffer.getReadableBytes());
        assertEquals(EncodingCodes.SMALLULONG, buffer.readByte());
        assertEquals((byte) 1, buffer.readByte());
        encoder.writeUnsignedLong(buffer, encoderState, 64l);
        assertEquals(2, buffer.getReadableBytes());
        assertEquals(EncodingCodes.SMALLULONG, buffer.readByte());
        assertEquals((byte) 64, buffer.readByte());
        encoder.writeUnsignedLong(buffer, encoderState, 255l);
        assertEquals(2, buffer.getReadableBytes());
        assertEquals(EncodingCodes.SMALLULONG, buffer.readByte());
        assertEquals((byte) 255, buffer.readByte());
    }

    @Test
    public void testWriteLongValuesOutsideOfSmallULongRange() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, 314l);
        assertEquals(9, buffer.getReadableBytes());
        assertEquals(EncodingCodes.ULONG, buffer.readByte());
        assertEquals(314l, buffer.readLong());
    }

    @Test
    public void testWriteByteZeroEncodesAsOneByte() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, (byte) 0);
        assertEquals(1, buffer.getReadableBytes());
        assertEquals(EncodingCodes.ULONG0, buffer.readByte());
    }

    @Test
    public void testWriteByteInSmallULongRange() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, (byte) 64);
        assertEquals(2, buffer.getReadableBytes());
        assertEquals(EncodingCodes.SMALLULONG, buffer.readByte());
        assertEquals((byte) 64, buffer.readByte());
    }

    @Test
    public void testWriteByteAsZeroULong() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, (byte) 0);
        assertEquals(1, buffer.getReadableBytes());
        assertEquals(EncodingCodes.ULONG0, buffer.readByte());
        assertFalse(buffer.isReadable());
    }

    @Test
    public void testReadULongZeroDoesNotTouchBuffer() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);

        buffer.writeByte(EncodingCodes.ULONG0);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
        assertFalse(buffer.isReadable());
        assertEquals(UnsignedLong.ZERO, typeDecoder.readValue(buffer, decoderState));
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(Long.MAX_VALUE - i));
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(i));
        }

        UnsignedLong expected = UnsignedLong.valueOf(42);

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnsignedLong);

        UnsignedLong value = (UnsignedLong) result;
        assertEquals(expected, value);
    }
}
