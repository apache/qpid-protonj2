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
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ByteTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.ByteTypeEncoder;
import org.junit.jupiter.api.Test;

public class ByteTypeCodecTest extends CodecTestSupport {

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
                streamDecoder.readByte(stream, streamDecoderState);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readByte(stream, streamDecoderState, (byte) 0);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readByte(buffer, decoderState);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}

            try {
                decoder.readByte(buffer, decoderState, (byte) 0);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.BYTE, (byte) new ByteTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Byte.class, new ByteTypeEncoder().getTypeClass());
        assertEquals(Byte.class, new ByteTypeDecoder().getTypeClass());
    }

    @Test
    public void testPeekNextTypeDecoder() throws IOException {
        testPeekNextTypeDecoder(false);
    }

    @Test
    public void testPeekNextTypeDecoderFS() throws IOException {
        testPeekNextTypeDecoder(true);
    }

    private void testPeekNextTypeDecoder(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 42);

        if (fromStream) {
            assertEquals(Byte.class, streamDecoder.peekNextTypeDecoder(stream, streamDecoderState).getTypeClass());
            assertEquals(42, streamDecoder.readByte(stream, streamDecoderState).intValue());
        } else {
            assertEquals(Byte.class, decoder.peekNextTypeDecoder(buffer, decoderState).getTypeClass());
            assertEquals(42, decoder.readByte(buffer, decoderState).intValue());
        }
    }

    @Test
    public void testReadByteFromEncodingCode() throws IOException {
        testReadByteFromEncodingCode(false);
    }

    @Test
    public void testReadByteFromEncodingCodeFS() throws IOException {
        testReadByteFromEncodingCode(true);
    }

    private void testReadByteFromEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 42);
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertEquals(42, streamDecoder.readByte(stream, streamDecoderState).intValue());
            assertEquals(43, streamDecoder.readByte(stream, streamDecoderState, (byte) 42));
            assertNull(streamDecoder.readByte(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readByte(stream, streamDecoderState, (byte) 42));
        } else {
            assertEquals(42, decoder.readByte(buffer, decoderState).intValue());
            assertEquals(43, decoder.readByte(buffer, decoderState, (byte) 42));
            assertNull(decoder.readByte(buffer, decoderState));
            assertEquals(42, decoder.readByte(buffer, decoderState, (byte) 42));
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
            encoder.writeByte(buffer, encoderState, Byte.MAX_VALUE);
            encoder.writeByte(buffer, encoderState, (byte) 16);
        }

        byte expected = 42;

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Byte.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Byte.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Byte.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Byte.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Byte);

        Byte value = (Byte) result;
        assertEquals(expected, value.byteValue());
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

        Byte[] source = new Byte[size];
        for (int i = 0; i < size; ++i) {
            source[i] = Byte.valueOf((byte) (random.nextInt() & 0xFF));
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

        byte[] array = (byte[]) result;
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

        Byte[] source = new Byte[0];

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

        byte[] array = (byte[]) result;
        assertEquals(source.length, array.length);
    }
}
