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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.junit.jupiter.api.Test;

public class UUIDTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readUUID(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertNull(decoder.readUUID(buffer, decoderState));
        assertEquals(value, decoder.readUUID(buffer, decoderState));
    }

    @Test
    public void testEncodeDecodeUUID() throws IOException {
        doTestEncodeDecodeUUIDSeries(1);
    }

    @Test
    public void testEncodeDecodeSmallSeriesOfUUIDs() throws IOException {
        doTestEncodeDecodeUUIDSeries(SMALL_SIZE);
    }

    @Test
    public void testEncodeDecodeLargeSeriesOfUUIDs() throws IOException {
        doTestEncodeDecodeUUIDSeries(LARGE_SIZE);
    }

    private void doTestEncodeDecodeUUIDSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
        }

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, source[i]);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof UUID);

            UUID decoded = (UUID) result;

            assertEquals(source[i], decoded);
        }
    }

    @Test
    public void testDecodeSmallUUIDArray() throws IOException {
        doTestDecodeUUDIArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeUUDIArray() throws IOException {
        doTestDecodeUUDIArrayType(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeUUDIArrayType(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[0];
        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        UUID[] array = (UUID[]) result;
        assertEquals(0, array.length);
    }

    @Test
    public void testObjectArrayContainingUUID() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Object[] source = new Object[10];
        for (int i = 0; i < 10; ++i) {
            source[i] = UUID.randomUUID();
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[][] source = new UUID[2][0];
        try {
            encoder.writeArray(buffer, encoderState, source);
        } catch (Exception e) {
            fail("Should be able to encode array with no size");
        }

        Object result = decoder.readObject(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeUUID(buffer, encoderState, UUID.randomUUID());
        }

        UUID expected = UUID.randomUUID();

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(UUID.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UUID);

        UUID value = (UUID) result;
        assertEquals(expected, value);
    }
}
