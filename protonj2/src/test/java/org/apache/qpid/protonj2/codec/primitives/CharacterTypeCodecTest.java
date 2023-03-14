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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.CharacterTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.CharacterTypeEncoder;
import org.junit.jupiter.api.Test;

public class CharacterTypeCodecTest extends CodecTestSupport {

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
                streamDecoder.readCharacter(stream, streamDecoderState);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readCharacter(stream, streamDecoderState, (char) 0);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readCharacter(buffer, decoderState);
                fail("Should not allow read of integer type as byte");
            } catch (DecodeException e) {}

            try {
                decoder.readCharacter(buffer, decoderState, (char) 0);
                fail("Should not allow read of integer type as byte");
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

        buffer.writeByte(EncodingCodes.CHAR);
        buffer.writeInt(42);
        buffer.writeByte(EncodingCodes.CHAR);
        buffer.writeInt(43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            assertEquals(42, streamDecoder.readCharacter(stream, streamDecoderState).charValue());
            assertEquals(43, streamDecoder.readCharacter(stream, streamDecoderState, (char) 42));
            assertNull(streamDecoder.readCharacter(stream, streamDecoderState));
            assertEquals(42, streamDecoder.readCharacter(stream, streamDecoderState, (char) 42));
        } else {
            assertEquals(42, decoder.readCharacter(buffer, decoderState).charValue());
            assertEquals(43, decoder.readCharacter(buffer, decoderState, (char) 42));
            assertNull(decoder.readCharacter(buffer, decoderState));
            assertEquals(42, decoder.readCharacter(buffer, decoderState, (char) 42));
        }
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.CHAR, (byte) new CharacterTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Character.class, new CharacterTypeEncoder().getTypeClass());
        assertEquals(Character.class, new CharacterTypeDecoder().getTypeClass());
    }

    @Test
    public void testReadCharFromEncodingCode() throws IOException {
        testReadCharFromEncodingCode(false);
    }

    @Test
    public void testReadCharFromEncodingCodeFS() throws IOException {
        testReadCharFromEncodingCode(true);
    }

    private void testReadCharFromEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.CHAR);
        buffer.writeInt(42);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            assertEquals(42, streamDecoder.readCharacter(stream, streamDecoderState).charValue());
        } else {
            assertEquals(42, decoder.readCharacter(buffer, decoderState).charValue());
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeCharacter(buffer, encoderState, Character.MAX_VALUE);
            encoder.writeCharacter(buffer, encoderState, (char) 16);
        }

        char expected = 42;

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
                assertEquals(Character.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Character.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Character.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Character.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Character);

        Character value = (Character) result;
        assertEquals(expected, value.charValue());
    }

    @Test
    public void testArrayOfCharacterObjects() throws IOException {
        testArrayOfCharacterObjects(false);
    }

    @Test
    public void testArrayOfCharacterObjectsFS() throws IOException {
        testArrayOfCharacterObjects(true);
    }

    private void testArrayOfCharacterObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        Character[] source = new Character[size];
        for (int i = 0; i < size; ++i) {
            source[i] = Character.valueOf((char) i);
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

        char[] array = (char[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfCharacterObjects() throws IOException {
        testZeroSizedArrayOfCharacterObjects(false);
    }

    @Test
    public void testZeroSizedArrayOfCharacterObjectsFS() throws IOException {
        testZeroSizedArrayOfCharacterObjects(true);
    }

    private void testZeroSizedArrayOfCharacterObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Character[] source = new Character[0];

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

        char[] array = (char[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeSmallCharArray() throws IOException {
        doTestDecodeCharArrayType(SMALL_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeLargeCharArray() throws IOException {
        doTestDecodeCharArrayType(LARGE_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeSmallCharArrayFS() throws IOException {
        doTestDecodeCharArrayType(SMALL_ARRAY_SIZE, true);
    }

    @Test
    public void testDecodeLargeCharArrayFS() throws IOException {
        doTestDecodeCharArrayType(LARGE_ARRAY_SIZE, true);
    }

    private void doTestDecodeCharArrayType(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        char[] source = new char[size];
        for (int i = 0; i < size; ++i) {
            source[i] = Character.valueOf((char) i);
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

        char[] array = (char[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
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

        buffer.writeByte(EncodingCodes.CHAR);
        buffer.writeInt(127);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(4, typeDecoder.readSize(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(4, typeDecoder.readSize(buffer, decoderState));
        }
    }
}
