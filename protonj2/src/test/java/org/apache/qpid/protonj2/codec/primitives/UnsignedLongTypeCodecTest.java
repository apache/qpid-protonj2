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
package org.apache.qpid.protonj2.codec.primitives;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.Test;

public class UnsignedLongTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(false);
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisTypeFS() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(true);
    }

    private void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        if (fromStream) {
            try {
                streamDecoder.readUnsignedLong(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readUnsignedLong(stream, streamDecoderState, (short) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readUnsignedLong(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readUnsignedLong(buffer, decoderState, (short) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        testReadUByteFromEncodingCode(false);
    }

    @Test
    public void testReadUByteFromEncodingCodeFS() throws IOException {
        testReadUByteFromEncodingCode(true);
    }

    private void testReadUByteFromEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.ULONG0);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(42);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertEquals(0, streamDecoder.readUnsignedLong(stream, streamDecoderState).intValue());
            assertEquals(42, streamDecoder.readUnsignedLong(stream, streamDecoderState).intValue());
            assertEquals(43, streamDecoder.readUnsignedLong(stream, streamDecoderState, 42));
            assertNull(streamDecoder.readUnsignedLong(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readUnsignedLong(stream, streamDecoderState, 42));
        } else {
            assertEquals(0, decoder.readUnsignedLong(buffer, decoderState).intValue());
            assertEquals(42, decoder.readUnsignedLong(buffer, decoderState).intValue());
            assertEquals(43, decoder.readUnsignedLong(buffer, decoderState, 42));
            assertNull(decoder.readUnsignedLong(buffer, decoderState));
            assertEquals(42, decoder.readUnsignedLong(buffer, decoderState, 42));
        }
    }

    @Test
    public void testEncodeDecodeUnsignedLong() throws Exception {
        testEncodeDecodeUnsignedLong(false);
    }

    @Test
    public void testEncodeDecodeUnsignedLongFS() throws Exception {
        testEncodeDecodeUnsignedLong(true);
    }

    private void testEncodeDecodeUnsignedLong(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(640));

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedLong);
        assertEquals(640, ((UnsignedLong) result).intValue());
    }

    @Test
    public void testEncodeDecodePrimitive() throws Exception {
        testEncodeDecodePrimitive(false);
    }

    @Test
    public void testEncodeDecodePrimitiveFS() throws Exception {
        testEncodeDecodePrimitive(true);
    }

    private void testEncodeDecodePrimitive(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeUnsignedLong(buffer, encoderState, 640l);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedLong);
        assertEquals(640, ((UnsignedLong) result).intValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedLongs() throws IOException {
        doTestDecodeUnsignedLongSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedLongs() throws IOException {
        doTestDecodeUnsignedLongSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedLongsFS() throws IOException {
        doTestDecodeUnsignedLongSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedLongsFS() throws IOException {
        doTestDecodeUnsignedLongSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeUnsignedLongSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(i));
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedLong result;
            if (fromStream) {
                result = streamDecoder.readUnsignedLong(stream, streamDecoderState);
            } else {
                result = decoder.readUnsignedLong(buffer, decoderState);
            }

            assertNotNull(result);
            assertEquals(i, result.intValue());
        }
    }

    @Test
    public void testArrayOfObjects() throws IOException {
        testArrayOfObjects(false);
    }

    @Test
    public void testArrayOfObjectsFS() throws IOException {
        testArrayOfObjects(true);
    }

    private void testArrayOfObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int size = 10;

        UnsignedLong[] source = new UnsignedLong[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedLong.valueOf(i);
        }

        encoder.writeArray(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

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
    public void testZeroSizedArrayOfObjects() throws IOException {
        testZeroSizedArrayOfObjects(false);
    }

    @Test
    public void testZeroSizedArrayOfObjectsFS() throws IOException {
        testZeroSizedArrayOfObjects(true);
    }

    private void testZeroSizedArrayOfObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UnsignedLong[] source = new UnsignedLong[0];

        encoder.writeArray(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedLong[] array = (UnsignedLong[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeEncodedBytes() throws Exception {
        testDecodeEncodedBytes(false);
    }

    @Test
    public void testDecodeEncodedBytesFS() throws Exception {
        testDecodeEncodedBytes(true);
    }

    private void testDecodeEncodedBytes(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.ULONG0);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(255);

        if (fromStream) {
            UnsignedLong result1 = streamDecoder.readUnsignedLong(stream, streamDecoderState);
            UnsignedLong result2 = streamDecoder.readUnsignedLong(stream, streamDecoderState);
            UnsignedLong result3 = streamDecoder.readUnsignedLong(stream, streamDecoderState);

            assertEquals(UnsignedLong.valueOf(0), result1);
            assertEquals(UnsignedLong.valueOf(127), result2);
            assertEquals(UnsignedLong.valueOf(255), result3);
        } else {
            UnsignedLong result1 = decoder.readUnsignedLong(buffer, decoderState);
            UnsignedLong result2 = decoder.readUnsignedLong(buffer, decoderState);
            UnsignedLong result3 = decoder.readUnsignedLong(buffer, decoderState);

            assertEquals(UnsignedLong.valueOf(0), result1);
            assertEquals(UnsignedLong.valueOf(127), result2);
            assertEquals(UnsignedLong.valueOf(255), result3);
        }
    }

    @Test
    public void testDecodeEncodedBytesAsPrimitive() throws Exception {
        testDecodeEncodedBytesAsPrimitive(false);
    }

    @Test
    public void testDecodeEncodedBytesAsPrimitiveFS() throws Exception {
        testDecodeEncodedBytesAsPrimitive(true);
    }

    private void testDecodeEncodedBytesAsPrimitive(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.ULONG0);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(255);

        if (fromStream) {
            long result1 = streamDecoder.readUnsignedLong(stream, streamDecoderState, 1);
            long result2 = streamDecoder.readUnsignedLong(stream, streamDecoderState, 105);
            long result3 = streamDecoder.readUnsignedLong(stream, streamDecoderState, 200);

            assertEquals(0, result1);
            assertEquals(127, result2);
            assertEquals(255, result3);
        } else {
            long result1 = decoder.readUnsignedLong(buffer, decoderState, 1);
            long result2 = decoder.readUnsignedLong(buffer, decoderState, 105);
            long result3 = decoder.readUnsignedLong(buffer, decoderState, 200);

            assertEquals(0, result1);
            assertEquals(127, result2);
            assertEquals(255, result3);
        }
    }

    @Test
    public void testDecodeBooleanFromNullEncoding() throws Exception {
        testDecodeBooleanFromNullEncoding(false);
    }

    @Test
    public void testDecodeBooleanFromNullEncodingFS() throws Exception {
        testDecodeBooleanFromNullEncoding(true);
    }

    private void testDecodeBooleanFromNullEncoding(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeUnsignedLong(buffer, encoderState, (byte) 1);
        encoder.writeNull(buffer, encoderState);

        if (fromStream) {
            UnsignedLong result1 = streamDecoder.readUnsignedLong(stream, streamDecoderState);
            UnsignedLong result2 = streamDecoder.readUnsignedLong(stream, streamDecoderState);

            assertEquals(UnsignedLong.valueOf(1), result1);
            assertNull(result2);
        } else {
            UnsignedLong result1 = decoder.readUnsignedLong(buffer, decoderState);
            UnsignedLong result2 = decoder.readUnsignedLong(buffer, decoderState);

            assertEquals(UnsignedLong.valueOf(1), result1);
            assertNull(result2);
        }
    }

    @Test
    public void testDecodeBooleanAsPrimitiveWithDefault() throws Exception {
        testDecodeBooleanAsPrimitiveWithDefault(false);
    }

    @Test
    public void testDecodeBooleanAsPrimitiveWithDefaultFS() throws Exception {
        testDecodeBooleanAsPrimitiveWithDefault(true);
    }

    private void testDecodeBooleanAsPrimitiveWithDefault(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeUnsignedLong(buffer, encoderState, 27);
        encoder.writeNull(buffer, encoderState);

        if (fromStream) {
            long result = streamDecoder.readUnsignedLong(stream, streamDecoderState, 0);
            assertEquals(27, result);
            result = streamDecoder.readUnsignedLong(stream, streamDecoderState, 127);
            assertEquals(127, result);
        } else {
            long result = decoder.readUnsignedLong(buffer, decoderState, 0);
            assertEquals(27, result);
            result = decoder.readUnsignedLong(buffer, decoderState, 127);
            assertEquals(127, result);
        }
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
        testReadULongZeroDoesNotTouchBuffer(false);
    }

    @Test
    public void testReadULongZeroDoesNotTouchBufferFS() throws IOException {
        testReadULongZeroDoesNotTouchBuffer(true);
    }

    private void testReadULongZeroDoesNotTouchBuffer(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.ULONG0);

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
            assertFalse(stream.available() > 0);
            assertEquals(UnsignedLong.ZERO, typeDecoder.readValue(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
            assertFalse(buffer.isReadable());
            assertEquals(UnsignedLong.ZERO, typeDecoder.readValue(buffer, decoderState));
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        doTestSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        doTestSkipValue(true);
    }

    public void doTestSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(0));

        for (int i = 1; i <= 10; ++i) {
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(Long.MAX_VALUE - i));
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(i));
        }

        UnsignedLong expected = UnsignedLong.valueOf(42);

        encoder.writeObject(buffer, encoderState, expected);

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.ULONG0 & 0xFF);
            typeDecoder.skipValue(stream, streamDecoderState);
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.ULONG0 & 0xFF);
            typeDecoder.skipValue(buffer, decoderState);
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.ULONG & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.SMALLULONG & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.ULONG & 0xFF);
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(UnsignedLong.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.SMALLULONG & 0xFF);
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
        assertTrue(result instanceof UnsignedLong);

        UnsignedLong value = (UnsignedLong) result;
        assertEquals(expected, value);
    }
}
