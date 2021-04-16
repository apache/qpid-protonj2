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
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Decimal64TypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.Decimal64TypeEncoder;
import org.apache.qpid.protonj2.types.Decimal64;
import org.junit.jupiter.api.Test;

public class Decimal64TypeCodecTest extends CodecTestSupport {

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

        if (fromStream) {
            try {
                streamDecoder.readDecimal64(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readDecimal64(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.DECIMAL64);
        buffer.writeLong(42);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertEquals(42, streamDecoder.readDecimal64(stream, streamDecoderState).getBits());
            assertNull(streamDecoder.readDecimal64(stream, streamDecoderState));
        } else {
            assertEquals(42, decoder.readDecimal64(buffer, decoderState).getBits());
            assertNull(decoder.readDecimal64(buffer, decoderState));
        }
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.DECIMAL64, (byte) new Decimal64TypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Decimal64.class, new Decimal64TypeEncoder().getTypeClass());
        assertEquals(Decimal64.class, new Decimal64TypeDecoder().getTypeClass());
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

        buffer.writeByte(EncodingCodes.DECIMAL64);
        buffer.writeLong(42);

        if (fromStream) {
            assertEquals(42, streamDecoder.readDecimal64(stream, streamDecoderState).getBits());
        } else {
            assertEquals(42, decoder.readDecimal64(buffer, decoderState).getBits());
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        testSkipValue(false);
    }

    @Test
    public void testSkipValueFS() throws IOException {
        testSkipValue(true);
    }

    private void testSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < 10; ++i) {
            encoder.writeDecimal64(buffer, encoderState, new Decimal64(Long.MAX_VALUE - i));
            encoder.writeDecimal64(buffer, encoderState, new Decimal64(i));
        }

        Decimal64 expected = new Decimal64(42);

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Decimal64.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Decimal64.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Decimal64.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Decimal64.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Decimal64);

        Decimal64 value = (Decimal64) result;
        assertEquals(expected, value);
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

        Decimal64[] source = new Decimal64[size];
        for (int i = 0; i < size; ++i) {
            source[i] = new Decimal64(random.nextLong());
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

        Decimal64[] array = (Decimal64[]) result;
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

        Decimal64[] source = new Decimal64[0];

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

        Decimal64[] array = (Decimal64[]) result;
        assertEquals(source.length, array.length);
    }
}
