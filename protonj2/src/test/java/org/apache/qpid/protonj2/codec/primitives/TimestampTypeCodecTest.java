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
import java.util.Date;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.TimestampTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.TimestampTypeEncoder;
import org.junit.jupiter.api.Test;

public class TimestampTypeCodecTest extends CodecTestSupport {

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
                streamDecoder.readTimestamp(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readTimestamp(stream, streamDecoderState, 42l);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readTimestamp(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readTimestamp(buffer, decoderState, 42l);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        testReadFromNullEncodingCode(false);
    }

    @Test
    public void testReadFromNullEncodingCodeFS() throws IOException {
        testReadFromNullEncodingCode(true);
    }

    private void testReadFromNullEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertNull(streamDecoder.readTimestamp(stream, streamDecoderState));
            assertEquals(42l, streamDecoder.readTimestamp(stream, streamDecoderState, 42l));
        } else {
            assertNull(decoder.readTimestamp(buffer, decoderState));
            assertEquals(42l, decoder.readTimestamp(buffer, decoderState, 42l));
        }
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.TIMESTAMP, (byte) new TimestampTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Date.class, new TimestampTypeEncoder().getTypeClass());
        assertEquals(Long.class, new TimestampTypeDecoder().getTypeClass());
    }

    @Test
    public void testReadFromEncodingCode() throws IOException {
        testReadFromEncodingCode(false);
    }

    @Test
    public void testReadFromEncodingCodeFS() throws IOException {
        testReadFromEncodingCode(true);
    }

    private void testReadFromEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.TIMESTAMP);
        buffer.writeLong(42);

        if (fromStream) {
            assertEquals(42, streamDecoder.readTimestamp(stream, streamDecoderState).longValue());
        } else {
            assertEquals(42, decoder.readTimestamp(buffer, decoderState).longValue());
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

        for (int i = 0; i < 10; ++i) {
            encoder.writeTimestamp(buffer, encoderState, Long.MAX_VALUE);
            encoder.writeTimestamp(buffer, encoderState, 16);
        }

        long expected = 42;

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.TIMESTAMP & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.TIMESTAMP & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.TIMESTAMP & 0xFF);
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Long.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.TIMESTAMP & 0xFF);
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
        assertEquals(expected, value.shortValue());
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
        Random random = new Random();
        random.setSeed(System.nanoTime());

        final int size = 10;

        Date[] source = new Date[size];
        for (int i = 0; i < size; ++i) {
            source[i] = new Date(random.nextLong());
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
        assertTrue(result.getClass().getComponentType().isPrimitive());

        long[] array = (long[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i].getTime(), array[i]);
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

        Date[] source = new Date[0];

        encoder.writeArray(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
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
}
