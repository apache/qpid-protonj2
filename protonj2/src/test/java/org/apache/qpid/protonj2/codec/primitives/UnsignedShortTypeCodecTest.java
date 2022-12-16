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
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.junit.jupiter.api.Test;

public class UnsignedShortTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(false);
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisTypeFS() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(true);
    }

    private void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            try {
                streamDecoder.readUnsignedShort(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readUnsignedShort(stream, streamDecoderState, (short) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readUnsignedShort(stream, streamDecoderState, 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readUnsignedShort(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readUnsignedShort(buffer, decoderState, (short) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readUnsignedShort(buffer, decoderState, 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testTypeFromEncodingCode() throws IOException {
        testTypeFromEncodingCode(false);
    }

    @Test
    public void testTypeFromEncodingCodeFS() throws IOException {
        testTypeFromEncodingCode(true);
    }

    public void testTypeFromEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.USHORT);
        buffer.writeShort((short) 42);
        buffer.writeByte(EncodingCodes.USHORT);
        buffer.writeShort((short) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            assertEquals(42, streamDecoder.readUnsignedShort(stream, streamDecoderState).shortValue());
            assertEquals(43, streamDecoder.readUnsignedShort(stream, streamDecoderState, (short) 42));
            assertNull(streamDecoder.readUnsignedShort(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readUnsignedShort(stream, streamDecoderState, (short) 42));
            assertEquals(43, streamDecoder.readUnsignedShort(stream, streamDecoderState, 43));
        } else {
            assertEquals(42, decoder.readUnsignedShort(buffer, decoderState).shortValue());
            assertEquals(43, decoder.readUnsignedShort(buffer, decoderState, (short) 42));
            assertNull(decoder.readUnsignedShort(buffer, decoderState));
            assertEquals(42, decoder.readUnsignedShort(buffer, decoderState, (short) 42));
            assertEquals(43, decoder.readUnsignedShort(buffer, decoderState, 43));
        }
    }

    @Test
    public void testEncodeDecodeUnsignedShort() throws Exception {
        testEncodeDecodeUnsignedShort(false);
    }

    @Test
    public void testEncodeDecodeUnsignedShortFS() throws Exception {
        testEncodeDecodeUnsignedShort(true);
    }

    public void testEncodeDecodeUnsignedShort(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedShort(buffer, encoderState, UnsignedShort.valueOf((byte) 64));

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedShort);
        assertEquals(64, ((UnsignedShort) result).byteValue());
    }

    @Test
    public void testEncodeDecodeUnsignedShortAbove32k() throws Exception {
        testEncodeDecodeUnsignedShortAbove32k(false);
    }

    @Test
    public void testEncodeDecodeUnsignedShortAbove32kFS() throws Exception {
        testEncodeDecodeUnsignedShortAbove32k(true);
    }

    private void testEncodeDecodeUnsignedShortAbove32k(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedShort(buffer, encoderState, UnsignedShort.valueOf((short) 33565));

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedShort);
        assertTrue(((UnsignedShort) result).shortValue() < 0);
        assertEquals(33565, ((UnsignedShort) result).intValue());
    }

    @Test
    public void testEncodeDecodeUnsignedShortFromInt() throws Exception {
        testEncodeDecodeUnsignedShortFromInt(false);
    }

    @Test
    public void testEncodeDecodeUnsignedShortFromIntFS() throws Exception {
        testEncodeDecodeUnsignedShortFromInt(true);
    }

    private void testEncodeDecodeUnsignedShortFromInt(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedShort(buffer, encoderState, 33565);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedShort);
        assertTrue(((UnsignedShort) result).shortValue() < 0);
        assertEquals(33565, ((UnsignedShort) result).intValue());

        try {
            encoder.writeUnsignedShort(buffer, encoderState, 65536);
            fail("Should not be able to write illegal out of range value");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testEncodeDecodeShort() throws Exception {
        testEncodeDecodeShort(false);
    }

    @Test
    public void testEncodeDecodeShortFS() throws Exception {
        testEncodeDecodeShort(true);
    }

    private void testEncodeDecodeShort(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedShort(buffer, encoderState, (short) 64);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedShort);
        assertEquals(64, ((UnsignedShort) result).shortValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedShorts() throws IOException {
        doTestDecodeUnsignedShortSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedShorts() throws IOException {
        doTestDecodeUnsignedShortSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedShortsFS() throws IOException {
        doTestDecodeUnsignedShortSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedShortsFS() throws IOException {
        doTestDecodeUnsignedShortSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeUnsignedShortSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedShort(buffer, encoderState, (byte)(i % 255));
        }

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedShort result;
            if (fromStream) {
                result = streamDecoder.readUnsignedShort(stream, streamDecoderState);
            } else {
                result = decoder.readUnsignedShort(buffer, decoderState);
            }

            assertNotNull(result);
            assertEquals((byte)(i % 255), result.byteValue());
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        UnsignedShort[] source = new UnsignedShort[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedShort.valueOf((byte) (i % 255));
        }

        encoder.writeArray(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedShort[] array = (UnsignedShort[]) result;
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        UnsignedShort[] source = new UnsignedShort[0];

        encoder.writeArray(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedShort[] array = (UnsignedShort[]) result;
        assertEquals(source.length, array.length);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeUnsignedShort(buffer, encoderState, UnsignedShort.valueOf(i));
        }

        UnsignedShort expected = UnsignedShort.valueOf(42);

        encoder.writeObject(buffer, encoderState, expected);

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(UnsignedShort.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.USHORT & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(UnsignedShort.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.USHORT & 0xFF);
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
        assertTrue(result instanceof UnsignedShort);

        UnsignedShort value = (UnsignedShort) result;
        assertEquals(expected, value);
    }
}
