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
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.junit.jupiter.api.Test;

public class UUIDTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(false);
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisTypeFromStream() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(true);
    }

    private void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.UINT);

        if (fromStream) {
            try {
                streamDecoder.readUUID(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readUUID(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        testReadFromNullEncodingCode(false);
    }

    @Test
    public void testReadFromNullEncodingCodeFromStream() throws IOException {
        testReadFromNullEncodingCode(true);
    }

    private void testReadFromNullEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        if (fromStream) {
            assertNull(streamDecoder.readUUID(stream, streamDecoderState));
            assertEquals(value, streamDecoder.readUUID(stream, streamDecoderState));
        } else {
            assertNull(decoder.readUUID(buffer, decoderState));
            assertEquals(value, decoder.readUUID(buffer, decoderState));
        }
    }

    @Test
    public void testEncodeDecodeUUID() throws IOException {
        doTestEncodeDecodeUUIDSeries(1, false);
    }

    @Test
    public void testEncodeDecodeSmallSeriesOfUUIDs() throws IOException {
        doTestEncodeDecodeUUIDSeries(SMALL_SIZE, false);
    }

    @Test
    public void testEncodeDecodeLargeSeriesOfUUIDs() throws IOException {
        doTestEncodeDecodeUUIDSeries(LARGE_SIZE, false);
    }

    @Test
    public void testEncodeDecodeUUIDFromStream() throws IOException {
        doTestEncodeDecodeUUIDSeries(1, true);
    }

    @Test
    public void testEncodeDecodeSmallSeriesOfUUIDsFromStream() throws IOException {
        doTestEncodeDecodeUUIDSeries(SMALL_SIZE, true);
    }

    @Test
    public void testEncodeDecodeLargeSeriesOfUUIDsFromStream() throws IOException {
        doTestEncodeDecodeUUIDSeries(LARGE_SIZE, true);
    }

    private void doTestEncodeDecodeUUIDSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
        }

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, source[i]);
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof UUID);

            UUID decoded = (UUID) result;

            assertEquals(source[i], decoded);
        }
    }

    @Test
    public void testDecodeSmallUUIDArray() throws IOException {
        doTestDecodeUUIDArrayType(SMALL_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeLargeUUIDArray() throws IOException {
        doTestDecodeUUIDArrayType(LARGE_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeSmallUUIDArrayFromStream() throws IOException {
        doTestDecodeUUIDArrayType(SMALL_ARRAY_SIZE, true);
    }

    @Test
    public void testDecodeLargeUUIDArrayFromStream() throws IOException {
        doTestDecodeUUIDArrayType(LARGE_ARRAY_SIZE, true);
    }

    private void doTestDecodeUUIDArrayType(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
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

        UUID[] array = (UUID[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testWriteUUIDArrayWithMixedNullAndNotNullValues() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[2];
        source[0] = UUID.randomUUID();
        source[1] = null;

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should not be able to encode array with mixed null and non-null values");
        } catch (Exception e) {}

        source = new UUID[2];
        source[0] = null;
        source[1] = UUID.randomUUID();

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should not be able to encode array with mixed null and non-null values");
        } catch (Exception e) {}
    }

    @Test
    public void testWriteUUIDArrayWithZeroSize() throws IOException {
        testWriteUUIDArrayWithZeroSize(false);
    }

    @Test
    public void testWriteUUIDArrayWithZeroSizeFromStream() throws IOException {
        testWriteUUIDArrayWithZeroSize(true);
    }

    private void testWriteUUIDArrayWithZeroSize(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[] source = new UUID[0];
        encoder.writeArray(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        UUID[] array = (UUID[]) result;
        assertEquals(0, array.length);
    }

    @Test
    public void testObjectArrayContainingUUID() throws IOException {
        testObjectArrayContainingUUID(false);
    }

    @Test
    public void testObjectArrayContainingUUIDFromStream() throws IOException {
        testObjectArrayContainingUUID(true);
    }

    private void testObjectArrayContainingUUID(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Object[] source = new Object[10];
        for (int i = 0; i < 10; ++i) {
            source[i] = UUID.randomUUID();
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

        UUID[] array = (UUID[]) result;
        assertEquals(10, array.length);

        for (int i = 0; i < 10; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testWriteArrayOfUUIDArrayWithZeroSize() throws IOException {
        testWriteArrayOfUUIDArrayWithZeroSize(false);
    }

    @Test
    public void testWriteArrayOfUUIDArrayWithZeroSizeFromStream() throws IOException {
        testWriteArrayOfUUIDArrayWithZeroSize(true);
    }

    private void testWriteArrayOfUUIDArrayWithZeroSize(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[][] source = new UUID[2][0];
        try {
            encoder.writeArray(buffer, encoderState, source);
        } catch (Exception e) {
            fail("Should be able to encode array with no size");
        }

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Object[] resultArray = (Object[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            Object nested = resultArray[i];
            assertNotNull(result);
            assertTrue(nested.getClass().isArray());

            UUID[] uuids = (UUID[]) nested;
            assertEquals(0, uuids.length);
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
            encoder.writeUUID(buffer, encoderState, UUID.randomUUID());
        }

        UUID expected = UUID.randomUUID();

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UUID & 0xFF);
                assertEquals(UUID.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.UUID & 0xFF);
                assertEquals(UUID.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof UUID);

        UUID value = (UUID) result;
        assertEquals(expected, value);
    }

    @Test
    public void testArrayOfObjects() throws IOException {
        testArrayOfObjects(false);
    }

    @Test
    public void testArrayOfObjectsFromStream() throws IOException {
        testArrayOfObjects(true);
    }

    private void testArrayOfObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int size = 10;

        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
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

        UUID[] array = (UUID[]) result;
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
    public void testZeroSizedArrayOfObjectsFromStream() throws IOException {
        testZeroSizedArrayOfObjects(true);
    }

    private void testZeroSizedArrayOfObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[] source = new UUID[0];

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

        UUID[] array = (UUID[]) result;
        assertEquals(source.length, array.length);
    }
}
