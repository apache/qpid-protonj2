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
import java.io.InputStream;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Long8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.LongTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.LongTypeEncoder;
import org.junit.jupiter.api.Test;

public class LongTypeCodecTest extends CodecTestSupport {

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
                streamDecoder.readLong(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readLong(stream, streamDecoderState, 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readLong(stream, streamDecoderState, 0l);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readLong(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readLong(buffer, decoderState, 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readLong(buffer, decoderState, 0l);
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

        buffer.writeByte(EncodingCodes.LONG);
        buffer.writeLong(42);
        buffer.writeByte(EncodingCodes.LONG);
        buffer.writeLong(44);
        buffer.writeByte(EncodingCodes.SMALLLONG);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            assertEquals(42, streamDecoder.readLong(stream, streamDecoderState).intValue());
            assertEquals(44, streamDecoder.readLong(stream, streamDecoderState, 42));
            assertEquals(43, streamDecoder.readLong(stream, streamDecoderState, 42));
            assertNull(streamDecoder.readLong(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readLong(stream, streamDecoderState, 42l));
        } else {
            assertEquals(42, decoder.readLong(buffer, decoderState).intValue());
            assertEquals(44, decoder.readLong(buffer, decoderState, 42));
            assertEquals(43, decoder.readLong(buffer, decoderState, 42));
            assertNull(decoder.readLong(buffer, decoderState));
            assertEquals(42, decoder.readLong(buffer, decoderState, 42l));
        }
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.LONG, (byte) new LongTypeDecoder().getTypeCode());
        assertEquals(EncodingCodes.SMALLLONG, (byte) new Long8TypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Long.class, new LongTypeEncoder().getTypeClass());
        assertEquals(Long.class, new Long8TypeDecoder().getTypeClass());
    }

    @Test
    public void testReadLongFromEncodingCodeLong() throws IOException {
        testReadLongFromEncodingCodeLong(false);
    }

    @Test
    public void testReadLongFromEncodingCodeLongFS() throws IOException {
        testReadLongFromEncodingCodeLong(true);
    }

    private void testReadLongFromEncodingCodeLong(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.LONG);
        buffer.writeLong(42);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            assertEquals(42l, streamDecoder.readLong(stream, streamDecoderState).longValue());
        } else {
            assertEquals(42l, decoder.readLong(buffer, decoderState).longValue());
        }
    }

    @Test
    public void testReadLongFromEncodingCodeSmallLong() throws IOException {
        testReadLongFromEncodingCodeSmallLong(false);
    }

    @Test
    public void testReadLongFromEncodingCodeSmallLongFS() throws IOException {
        testReadLongFromEncodingCodeSmallLong(true);
    }

    private void testReadLongFromEncodingCodeSmallLong(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.SMALLLONG);
        buffer.writeByte((byte) 42);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            assertEquals(42l, streamDecoder.readLong(stream, streamDecoderState).longValue());
        } else {
            assertEquals(42l, decoder.readLong(buffer, decoderState).longValue());
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
            encoder.writeLong(buffer, encoderState, Long.MAX_VALUE);
            encoder.writeLong(buffer, encoderState, 16);
        }

        long expected = 42l;

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
                assertEquals(Long.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Long);

        Long value = (Long) result;
        assertEquals(expected, value.intValue());
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
        Random random = new Random();
        random.setSeed(System.nanoTime());

        final int size = 10;

        Long[] source = new Long[size];
        for (int i = 0; i < size; ++i) {
            source[i] = Long.valueOf(random.nextLong());
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
        assertTrue(result.getClass().getComponentType().isPrimitive());

        long[] array = (long[]) result;
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

        Long[] source = new Long[0];

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
        assertTrue(result.getClass().getComponentType().isPrimitive());

        long[] array = (long[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testReadLongArray() throws IOException {
        doTestReadLongArray(EncodingCodes.LONG, false);
    }

    @Test
    public void testReadLongArrayFromStream() throws IOException {
        doTestReadLongArray(EncodingCodes.LONG, true);
    }

    @Test
    public void testReadSmallLongArray() throws IOException {
        doTestReadLongArray(EncodingCodes.SMALLLONG, false);
    }

    @Test
    public void testReadSmallLongArrayFromStream() throws IOException {
        doTestReadLongArray(EncodingCodes.SMALLLONG, true);
    }

    public void doTestReadLongArray(byte encoding, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        if (encoding == EncodingCodes.LONG) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(25);  // Size
            buffer.writeInt(2);   // Count
            buffer.writeByte(EncodingCodes.LONG);
            buffer.writeLong(1l);   // [0]
            buffer.writeLong(2l);   // [1]
        } else if (encoding == EncodingCodes.SMALLLONG) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(11);  // Size
            buffer.writeInt(2);   // Count
            buffer.writeByte(EncodingCodes.SMALLLONG);
            buffer.writeByte((byte) 1);   // [0]
            buffer.writeByte((byte) 2);   // [1]
        }

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        long[] array = (long[]) result;

        assertEquals(2, array.length);
        assertEquals(1, array[0]);
        assertEquals(2, array[1]);
    }
}
