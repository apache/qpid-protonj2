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
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.junit.jupiter.api.Test;

public class UnsignedByteTypeCodecTest extends CodecTestSupport {

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

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            try {
                streamDecoder.readUnsignedByte(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readUnsignedByte(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readUnsignedByte(buffer, decoderState, (byte) 0);
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

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 42);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            assertEquals(42, streamDecoder.readUnsignedByte(stream, streamDecoderState).intValue());
            assertEquals(43, streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 42));
            assertNull(streamDecoder.readUnsignedByte(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 42));
        } else {
            assertEquals(42, decoder.readUnsignedByte(buffer, decoderState).intValue());
            assertEquals(43, decoder.readUnsignedByte(buffer, decoderState, (byte) 42));
            assertNull(decoder.readUnsignedByte(buffer, decoderState));
            assertEquals(42, decoder.readUnsignedByte(buffer, decoderState, (byte) 42));
        }
    }

    @Test
    public void testEncodeDecodeUnsignedByte() throws Exception {
        testEncodeDecodeUnsignedByte(false);
    }

    @Test
    public void testEncodeDecodeUnsignedByteFS() throws Exception {
        testEncodeDecodeUnsignedByte(true);
    }

    public void testEncodeDecodeUnsignedByte(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedByte(buffer, encoderState, UnsignedByte.valueOf((byte) 64));

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedByte);
        assertEquals(64, ((UnsignedByte) result).byteValue());
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

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 64);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnsignedByte);
        assertEquals(64, ((UnsignedByte) result).byteValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedBytes() throws IOException {
        doTestDecodeUnsignedByteSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedBytes() throws IOException {
        doTestDecodeUnsignedByteSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedBytesFS() throws IOException {
        doTestDecodeUnsignedByteSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedBytesFS() throws IOException {
        doTestDecodeUnsignedByteSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeUnsignedByteSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedByte(buffer, encoderState, (byte)(i % 255));
        }

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedByte result;
            if (fromStream) {
                result = streamDecoder.readUnsignedByte(stream, streamDecoderState);
            } else {
                result = decoder.readUnsignedByte(buffer, decoderState);
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

        UnsignedByte[] source = new UnsignedByte[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedByte.valueOf((byte) (i % 255));
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

        UnsignedByte[] array = (UnsignedByte[]) result;
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

        UnsignedByte[] source = new UnsignedByte[0];

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

        UnsignedByte[] array = (UnsignedByte[]) result;
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 0);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 127);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 255);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            UnsignedByte result1 = streamDecoder.readUnsignedByte(stream, streamDecoderState);
            UnsignedByte result2 = streamDecoder.readUnsignedByte(stream, streamDecoderState);
            UnsignedByte result3 = streamDecoder.readUnsignedByte(stream, streamDecoderState);

            assertEquals(UnsignedByte.valueOf((byte) 0), result1);
            assertEquals(UnsignedByte.valueOf((byte) 127), result2);
            assertEquals(UnsignedByte.valueOf((byte) 255), result3);
        } else {
            UnsignedByte result1 = decoder.readUnsignedByte(buffer, decoderState);
            UnsignedByte result2 = decoder.readUnsignedByte(buffer, decoderState);
            UnsignedByte result3 = decoder.readUnsignedByte(buffer, decoderState);

            assertEquals(UnsignedByte.valueOf((byte) 0), result1);
            assertEquals(UnsignedByte.valueOf((byte) 127), result2);
            assertEquals(UnsignedByte.valueOf((byte) 255), result3);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 0);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 127);
        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 255);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            byte result1 = streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 1);
            byte result2 = streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 105);
            byte result3 = streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 200);

            assertEquals((byte) 0, result1);
            assertEquals((byte) 127, result2);
            assertEquals((byte) 255, result3);
        } else {
            byte result1 = decoder.readUnsignedByte(buffer, decoderState, (byte) 1);
            byte result2 = decoder.readUnsignedByte(buffer, decoderState, (byte) 105);
            byte result3 = decoder.readUnsignedByte(buffer, decoderState, (byte) 200);

            assertEquals((byte) 0, result1);
            assertEquals((byte) 127, result2);
            assertEquals((byte) 255, result3);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 1);
        encoder.writeNull(buffer, encoderState);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            UnsignedByte result1 = streamDecoder.readUnsignedByte(stream, streamDecoderState);
            UnsignedByte result2 = streamDecoder.readUnsignedByte(stream, streamDecoderState);

            assertEquals(UnsignedByte.valueOf((byte) 1), result1);
            assertNull(result2);
        } else {
            UnsignedByte result1 = decoder.readUnsignedByte(buffer, decoderState);
            UnsignedByte result2 = decoder.readUnsignedByte(buffer, decoderState);

            assertEquals(UnsignedByte.valueOf((byte) 1), result1);
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

    public void testDecodeBooleanAsPrimitiveWithDefault(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 27);
        encoder.writeNull(buffer, encoderState);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            byte result = streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 0);
            assertEquals((byte) 27, result);
            result = streamDecoder.readUnsignedByte(stream, streamDecoderState, (byte) 127);
            assertEquals((byte) 127, result);
        } else {
            byte result = decoder.readUnsignedByte(buffer, decoderState, (byte) 0);
            assertEquals((byte) 27, result);
            result = decoder.readUnsignedByte(buffer, decoderState, (byte) 127);
            assertEquals((byte) 127, result);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeUnsignedByte(buffer, encoderState, UnsignedByte.valueOf((byte) i));
        }

        UnsignedByte expected = UnsignedByte.valueOf((byte) 42);

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
                assertEquals(UnsignedByte.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UBYTE & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(UnsignedByte.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UBYTE & 0xFF);
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
        assertTrue(result instanceof UnsignedByte);

        UnsignedByte value = (UnsignedByte) result;
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

        buffer.writeByte(EncodingCodes.UBYTE);
        buffer.writeByte((byte) 127);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(1, typeDecoder.readSize(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(1, typeDecoder.readSize(buffer, decoderState));
        }
    }
}
