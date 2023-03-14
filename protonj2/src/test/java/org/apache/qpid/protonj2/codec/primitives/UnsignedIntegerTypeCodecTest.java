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
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;

public class UnsignedIntegerTypeCodecTest extends CodecTestSupport {

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

        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeByte(EncodingCodes.ULONG);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            try {
                streamDecoder.readUnsignedInteger(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readUnsignedInteger(stream, streamDecoderState, (long) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readUnsignedInteger(stream, streamDecoderState, 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readUnsignedInteger(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readUnsignedInteger(buffer, decoderState, (long) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readUnsignedInteger(buffer, decoderState, 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadTypeFromEncodingCode() throws IOException {
        testReadTypeFromEncodingCode(false);
    }

    @Test
    public void testReadTypeFromEncodingCodeFS() throws IOException {
        testReadTypeFromEncodingCode(true);
    }

    public void testReadTypeFromEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UINT0);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeInt(42);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            assertEquals(0, streamDecoder.readUnsignedInteger(stream, streamDecoderState).intValue());
            assertEquals(42, streamDecoder.readUnsignedInteger(stream, streamDecoderState).intValue());
            assertEquals(43, streamDecoder.readUnsignedInteger(stream, streamDecoderState, 42));
            assertNull(streamDecoder.readUnsignedInteger(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readUnsignedInteger(stream, streamDecoderState, 42));
            assertEquals(42, streamDecoder.readUnsignedInteger(stream, streamDecoderState, (long) 42));
        } else {
            assertEquals(0, decoder.readUnsignedInteger(buffer, decoderState).intValue());
            assertEquals(42, decoder.readUnsignedInteger(buffer, decoderState).intValue());
            assertEquals(43, decoder.readUnsignedInteger(buffer, decoderState, 42));
            assertNull(decoder.readUnsignedInteger(buffer, decoderState));
            assertEquals(42, decoder.readUnsignedInteger(buffer, decoderState, 42));
            assertEquals(42, decoder.readUnsignedInteger(buffer, decoderState, (long) 42));
        }
    }

    @Test
    public void testEncodeDecodeUnsignedInteger() throws Exception {
        testEncodeDecodeUnsignedInteger(false);
    }

    @Test
    public void testEncodeDecodeUnsignedIntegerFS() throws Exception {
        testEncodeDecodeUnsignedInteger(true);
    }

    private void testEncodeDecodeUnsignedInteger(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(640));

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            Object result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(640, ((UnsignedInteger) result).intValue());
        } else {
            Object result = decoder.readObject(buffer, decoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(640, ((UnsignedInteger) result).intValue());
        }
    }

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        testEncodeDecodeInteger(false);
    }

    @Test
    public void testEncodeDecodeIntegerFS() throws Exception {
        testEncodeDecodeInteger(true);
    }

    private void testEncodeDecodeInteger(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 640);
        encoder.writeUnsignedInteger(buffer, encoderState, 0);
        encoder.writeUnsignedInteger(buffer, encoderState, 255);
        encoder.writeUnsignedInteger(buffer, encoderState, 254);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            Object result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(640, ((UnsignedInteger) result).intValue());
            result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(0, ((UnsignedInteger) result).intValue());
            result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(255, ((UnsignedInteger) result).intValue());
            result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(254, ((UnsignedInteger) result).intValue());
        } else {
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
    }

    @Test
    public void testEncodeDecodeLong() throws Exception {
        testEncodeDecodeLong(false);
    }

    @Test
    public void testEncodeDecodeLongFS() throws Exception {
        testEncodeDecodeLong(true);
    }

    private void testEncodeDecodeLong(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 640l);
        encoder.writeUnsignedInteger(buffer, encoderState, 0l);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            Object result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(640, ((UnsignedInteger) result).intValue());
            result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(0, ((UnsignedInteger) result).intValue());
        } else {
            Object result = decoder.readObject(buffer, decoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(640, ((UnsignedInteger) result).intValue());
            result = decoder.readObject(buffer, decoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(0, ((UnsignedInteger) result).intValue());
        }

        try {
            encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Should fail on value out of range");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testEncodeDecodeByte() throws Exception {
        testEncodeDecodeByte(false);
    }

    @Test
    public void testEncodeDecodeByteFS() throws Exception {
        testEncodeDecodeByte(true);
    }

    private void testEncodeDecodeByte(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, (byte) 64);
        encoder.writeUnsignedInteger(buffer, encoderState, (byte) 0);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            Object result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(64, ((UnsignedInteger) result).byteValue());
            result = streamDecoder.readObject(stream, streamDecoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(0, ((UnsignedInteger) result).byteValue());
        } else {
            Object result = decoder.readObject(buffer, decoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(64, ((UnsignedInteger) result).byteValue());
            result = decoder.readObject(buffer, decoderState);
            assertTrue(result instanceof UnsignedInteger);
            assertEquals(0, ((UnsignedInteger) result).byteValue());
        }
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedIntegers() throws IOException {
        doTestDecodeUnsignedIntegerSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedIntegers() throws IOException {
        doTestDecodeUnsignedIntegerSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedIntegersFS() throws IOException {
        doTestDecodeUnsignedIntegerSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedIntegersFS() throws IOException {
        doTestDecodeUnsignedIntegerSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeUnsignedIntegerSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedInteger(buffer, encoderState, i);
        }

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedInteger result;
            if (fromStream) {
                result = streamDecoder.readUnsignedInteger(stream, streamDecoderState);
            } else {
                result = decoder.readUnsignedInteger(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        UnsignedInteger[] source = new UnsignedInteger[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedInteger.valueOf(i);
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

        UnsignedInteger[] array = (UnsignedInteger[]) result;
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

        UnsignedInteger[] source = new UnsignedInteger[0];

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

        UnsignedInteger[] array = (UnsignedInteger[]) result;
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

        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(0));

        for (int i = 1; i <= 10; ++i) {
            encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(Integer.MAX_VALUE - i));
            encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(i));
        }

        UnsignedInteger expected = UnsignedInteger.valueOf(42);

        encoder.writeObject(buffer, encoderState, expected);

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UINT0 & 0xFF);
            typeDecoder.skipValue(stream, streamDecoderState);
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UINT0 & 0xFF);
            typeDecoder.skipValue(buffer, decoderState);
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UINT & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.SMALLUINT & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UINT & 0xFF);
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(UnsignedInteger.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.SMALLUINT & 0xFF);
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
        assertTrue(result instanceof UnsignedInteger);

        UnsignedInteger value = (UnsignedInteger) result;
        assertEquals(expected, value);
    }

    @Test
    public void testReadSeizeFromEncoding() throws IOException {
        doTestReadSeizeFromEncoding(false);
    }

    @Test
    public void testReadSeizeFromEncodingInStream() throws IOException {
        doTestReadSeizeFromEncoding(true);
    }

    private void doTestReadSeizeFromEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UINT0);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte((byte) 127);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeInt(255);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(0, typeDecoder.readSize(stream, streamDecoderState));
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(1, typeDecoder.readSize(stream, streamDecoderState));
            typeDecoder.readValue(stream, streamDecoderState);
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(4, typeDecoder.readSize(stream, streamDecoderState));
            typeDecoder.readValue(stream, streamDecoderState);
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(0, typeDecoder.readSize(buffer, decoderState));
            typeDecoder.readValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(1, typeDecoder.readSize(buffer, decoderState));
            typeDecoder.readValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(4, typeDecoder.readSize(buffer, decoderState));
            typeDecoder.readValue(buffer, decoderState);
        }
    }
}
