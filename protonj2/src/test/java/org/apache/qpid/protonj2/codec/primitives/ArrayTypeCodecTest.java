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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.junit.jupiter.api.Test;

/**
 * Test decoding of AMQP Array types
 */
public class ArrayTypeCodecTest extends CodecTestSupport {

    @Test
    public void testWriteOfZeroSizedGenericArrayFails() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Object[] source = new Object[0];

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should reject attempt to write zero sized array of unknown type.");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testWriteOfGenericArrayOfObjectsFails() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Object[] source = new Object[2];

        source[0] = new Object();
        source[1] = new Object();

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should reject attempt to write array of unknown type.");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testArrayOfArraysOfMixedTypes() throws IOException {
        doTestArrayOfArraysOfMixedTypes(false);
    }

    @Test
    public void testArrayOfArraysOfMixedTypesFromStream() throws IOException {
        doTestArrayOfArraysOfMixedTypes(true);
    }

    private void doTestArrayOfArraysOfMixedTypes(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        Object[][] source = new Object[2][size];
        for (int i = 0; i < size; ++i) {
            source[0][i] = Short.valueOf((short) i);
            source[1][i] = Integer.valueOf(i);
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

        Object[] resultArray = (Object[]) result;

        assertNotNull(resultArray);
        assertEquals(2, resultArray.length);

        assertTrue(resultArray[0].getClass().isArray());
        assertTrue(resultArray[1].getClass().isArray());
    }

    @Test
    public void testArrayOfArraysOfArraysOfShortTypes() throws IOException {
        testArrayOfArraysOfArraysOfShortTypes(false);
    }

    @Test
    public void testArrayOfArraysOfArraysOfShortTypesFromStream() throws IOException {
        testArrayOfArraysOfArraysOfShortTypes(true);
    }

    private void testArrayOfArraysOfArraysOfShortTypes(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        Object[][][] source = new Object[2][2][size];
        for (int i = 0; i < source.length; ++i) {
            for (int j = 0; j < source[i].length; ++j) {
                for (int k = 0; k < source[i][j].length; ++k) {
                    source[i][j][k] = (short) k;
                }
             }
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

        Object[] resultArray = (Object[]) result;

        assertNotNull(resultArray);
        assertEquals(2, resultArray.length);

        for (int i = 0; i < resultArray.length; ++i) {
            assertTrue(resultArray[i].getClass().isArray());

            Object[] dimension2 = (Object[]) resultArray[i];
            assertEquals(2, dimension2.length);

            for (int j = 0; j < dimension2.length; ++j) {
                short[] dimension3 = (short[]) dimension2[j];
                assertEquals(size, dimension3.length);

                for (int k = 0; k < dimension3.length; ++k) {
                    assertEquals(source[i][j][k], dimension3[k]);
                }
             }
        }
    }

    @Test
    public void testWriteArrayOfArraysStrings() throws IOException {
        testWriteArrayOfArraysStrings(false);
    }

    @Test
    public void testWriteArrayOfArraysStringsFromStream() throws IOException {
        testWriteArrayOfArraysStrings(true);
    }

    private void testWriteArrayOfArraysStrings(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        String[][] stringArray = new String[2][1];

        stringArray[0][0] = "short-string";
        stringArray[1][0] = "long-string-entry:" + UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString();

        encoder.writeArray(buffer, encoderState, stringArray);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Object[] array = (Object[]) result;
        assertEquals(2, array.length);

        assertTrue(array[0] instanceof String[]);
        assertTrue(array[1] instanceof String[]);

        String[] element1Array = (String[]) array[0];
        String[] element2Array = (String[]) array[1];

        assertEquals(1, element1Array.length);
        assertEquals(1, element2Array.length);

        assertEquals(stringArray[0][0], element1Array[0]);
        assertEquals(stringArray[1][0], element2Array[0]);
    }

    @Test
    public void testEncodeArrayWithNullEntriesMatchesLegacy() throws Exception {
        Symbol[] input1 = new Symbol[] { null };
        Symbol[] input2 = new Symbol[] { AmqpError.DECODE_ERROR, null };

        try {
            legacyCodec.encodeUsingLegacyEncoder(input1);
            fail("Should fail as no type encoder can be deduced");
        } catch (NullPointerException npe) {
            // Expected
        }

        try {
            ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
            encoder.writeObject(buffer, encoderState, input1);
            fail("Should fail as no type encoder can be deduced");
        } catch (NullPointerException npe) {
            // Expected
        }

        try {
            legacyCodec.encodeUsingLegacyEncoder(input2);
            fail("Should fail as no type encoder cannot handle null elements");
        } catch (NullPointerException npe) {
            // Expected
        }

        try {
            ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
            encoder.writeObject(buffer, encoderState, input2);
            fail("Should fail as no type encoder cannot handle null elements");
        } catch (NullPointerException npe) {
            // Expected
        }
    }

    @Test
    public void testEncodeStringArrayWithNewCodecAndDecodeWithOldCodec() throws Exception {
        String[] input = new String[] { "test", "legacy", "codec" };

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);

        assertNotNull(buffer);

        Object decoded = decoder.readObject(buffer, decoderState);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(String.class, decoded.getClass().getComponentType());
        String[] result = (String[]) decoded;
        assertArrayEquals(input, result);
    }

    @Test
    public void testEncodeStringArrayUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        String[] input = new String[] { "test", "legacy", "codec" };

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);

        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(String.class, decoded.getClass().getComponentType());
        String[] result = (String[]) decoded;
        assertArrayEquals(input, result);
    }

    @Test
    public void testEncodeAndDecodeArrayOfListsUsingReadMultiple() throws Exception {
        testEncodeAndDecodeArrayOfListsUsingReadMultiple(false);
    }

    @Test
    public void testEncodeAndDecodeArrayOfListsUsingReadMultipleFromStream() throws Exception {
        testEncodeAndDecodeArrayOfListsUsingReadMultiple(true);
    }

    private void testEncodeAndDecodeArrayOfListsUsingReadMultiple(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        @SuppressWarnings("rawtypes")
        List[] lists = new List[3];

        ArrayList<String> content1 = new ArrayList<>();
        ArrayList<String> content2 = new ArrayList<>();
        ArrayList<String> content3 = new ArrayList<>();

        content1.add("test-1");
        content2.add("test-2");
        content3.add("test-3");

        lists[0] = content1;
        lists[1] = content2;
        lists[2] = content3;

        encoder.writeObject(buffer, encoderState, lists);

        @SuppressWarnings("rawtypes")
        final List[] decoded;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            decoded = streamDecoder.readMultiple(stream, streamDecoderState, List.class);
        } else {
            decoded = decoder.readMultiple(buffer, decoderState, List.class);
        }

        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(List.class, decoded.getClass().getComponentType());
        assertArrayEquals(lists, decoded);
    }

    @Test
    public void testEncodeAndDecodeArrayOfMapsUsingReadMultiple() throws Exception {
        testEncodeAndDecodeArrayOfMapsUsingReadMultiple(false);
    }

    @Test
    public void testEncodeAndDecodeArrayOfMapsUsingReadMultipleFromStream() throws Exception {
        testEncodeAndDecodeArrayOfMapsUsingReadMultiple(true);
    }

    private void testEncodeAndDecodeArrayOfMapsUsingReadMultiple(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        @SuppressWarnings("rawtypes")
        Map[] maps = new Map[3];

        Map<String, Object> content1 = new LinkedHashMap<>();
        Map<String, Object> content2 = new LinkedHashMap<>();
        Map<String, Object> content3 = new LinkedHashMap<>();

        content1.put("test-1", UUID.randomUUID());
        content2.put("test-2", "String");
        content3.put("test-3", Boolean.FALSE);

        maps[0] = content1;
        maps[1] = content2;
        maps[2] = content3;

        encoder.writeObject(buffer, encoderState, maps);

        @SuppressWarnings("rawtypes")
        final Map[] decoded;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            decoded = streamDecoder.readMultiple(stream, streamDecoderState, Map.class);
        } else {
            decoded = decoder.readMultiple(buffer, decoderState, Map.class);
        }

        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(Map.class, decoded.getClass().getComponentType());
        assertArrayEquals(maps, decoded);
    }

    @Test
    public void testEncodeDecodeBooleanArray100() throws Throwable {
        // boolean array8 less than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(100, false);
    }

    @Test
    public void testEncodeDecodeBooleanArray192() throws Throwable {
        // boolean array8 greater than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(192, false);
    }

    @Test
    public void testEncodeDecodeBooleanArray384() throws Throwable {
        // boolean array32
        doEncodeDecodeBooleanArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeBooleanArray100FS() throws Throwable {
        // boolean array8 less than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeBooleanArray192FS() throws Throwable {
        // boolean array8 greater than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(192, true);
    }

    @Test
    public void testEncodeDecodeBooleanArray384FS() throws Throwable {
        // boolean array32
        doEncodeDecodeBooleanArrayTestImpl(384, true);
    }

    private void doEncodeDecodeBooleanArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        boolean[] source = createPayloadArrayBooleans(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 254 ? 1 : 4; // less than 254 and not 256, since we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + count; // variable width for element count + byte type descriptor + number of elements
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x56); // 'boolean' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                byte booleanCode = (byte) (source[i] ? 0x01 : 0x00); //  0x01 true, 0x00 false.
                expectedEncodingWrapper.writeByte(booleanCode);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");

            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(boolean.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (boolean[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static boolean[] createPayloadArrayBooleans(int length) {
        Random rand = new Random(System.currentTimeMillis());

        boolean[] payload = new boolean[length];
        for (int i = 0; i < length; i++) {
            payload[i] = rand.nextBoolean();
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeByteArray100() throws Throwable {
        // byte array8 less than 128 bytes
        doEncodeDecodeByteArrayTestImpl(100, false);
    }

    @Test
    public void testEncodeDecodeByteArray192() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(192, false);
    }

    @Test
    public void testEncodeDecodeByteArray254() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(254, false);
    }

    @Test
    public void testEncodeDecodeByteArray255() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(255, false);
    }

    @Test
    public void testEncodeDecodeByteArray384() throws Throwable {
        // byte array32
        doEncodeDecodeByteArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeByteArray100FS() throws Throwable {
        // byte array8 less than 128 bytes
        doEncodeDecodeByteArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeByteArray192FS() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(192, true);
    }

    @Test
    public void testEncodeDecodeByteArray254FS() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(254, true);
    }

    @Test
    public void testEncodeDecodeByteArray255FS() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(255, true);
    }

    @Test
    public void testEncodeDecodeByteArray384FS() throws Throwable {
        // byte array32
        doEncodeDecodeByteArrayTestImpl(384, true);
    }

    private void doEncodeDecodeByteArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        byte[] source = createPayloadArrayBytes(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 254 ? 1 : 4; // less than 254 and not 256, since we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize = encodingWidth + 1 + count; // variable width for element count + byte type descriptor + number of elements
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x51); // 'byte' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.writeByte(source[i]);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");
            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(byte.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (byte[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static byte[] createPayloadArrayBytes(int length) {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeShortArray50() throws Throwable {
        // short array8 less than 128 bytes
        doEncodeDecodeShortArrayTestImpl(50, false);
    }

    @Test
    public void testEncodeDecodeShortArray100() throws Throwable {
        // short array8 greater than 128 bytes
        doEncodeDecodeShortArrayTestImpl(100, false);
    }

    @Test
    public void testEncodeDecodeShortArray384() throws Throwable {
        // short array32
        doEncodeDecodeShortArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeShortArray50FS() throws Throwable {
        // short array8 less than 128 bytes
        doEncodeDecodeShortArrayTestImpl(50, true);
    }

    @Test
    public void testEncodeDecodeShortArray100FS() throws Throwable {
        // short array8 greater than 128 bytes
        doEncodeDecodeShortArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeShortArray384FS() throws Throwable {
        // short array32
        doEncodeDecodeShortArrayTestImpl(384, true);
    }

    private void doEncodeDecodeShortArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        short[] source = createPayloadArrayShorts(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 127 ? 1 : 4; // less than 127, since each element is 2 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 2); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x61); // 'short' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.writeShort(source[i]);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");

            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(short.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (short[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static short[] createPayloadArrayShorts(int length) {
        Random rand = new Random(System.currentTimeMillis());

        short[] payload = new short[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (short) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeIntArray10() throws Throwable {
        // int array8 less than 128 bytes
        doEncodeDecodeIntArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeIntArray50() throws Throwable {
        // int array8 greater than 128 bytes
        doEncodeDecodeIntArrayTestImpl(50, false);
    }

    @Test
    public void testEncodeDecodeIntArray384() throws Throwable {
        // int array32
        doEncodeDecodeIntArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeIntArray10FS() throws Throwable {
        // int array8 less than 128 bytes
        doEncodeDecodeIntArrayTestImpl(10, true);
    }

    @Test
    public void testEncodeDecodeIntArray50FS() throws Throwable {
        // int array8 greater than 128 bytes
        doEncodeDecodeIntArrayTestImpl(50, true);
    }

    @Test
    public void testEncodeDecodeIntArray384FS() throws Throwable {
        // int array32
        doEncodeDecodeIntArrayTestImpl(384, true);
    }

    private void doEncodeDecodeIntArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        int[] source = createPayloadArrayInts(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 63 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int elementWidth = 4;
            int arrayPayloadSize =  encodingWidth + 1 + (count * elementWidth); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x71); // 'int' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                int j = source[i];
                expectedEncodingWrapper.writeInt(j);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");
            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(int.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (int[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static int[] createPayloadArrayInts(int length) {
        Random rand = new Random(System.currentTimeMillis());

        int[] payload = new int[length];
        for (int i = 0; i < length; i++) {
            payload[i] = 128 + 1 + rand.nextInt(9);
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeLongArray10() throws Throwable {
        // long array8 less than 128 bytes
        doEncodeDecodeLongArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeLongArray25() throws Throwable {
        // long array8 greater than 128 bytes
        doEncodeDecodeLongArrayTestImpl(25, false);
    }

    @Test
    public void testEncodeDecodeLongArray384() throws Throwable {
        // long array32
        doEncodeDecodeLongArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeLongArray10FS() throws Throwable {
        // long array8 less than 128 bytes
        doEncodeDecodeLongArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeLongArray25FS() throws Throwable {
        // long array8 greater than 128 bytes
        doEncodeDecodeLongArrayTestImpl(25, false);
    }

    @Test
    public void testEncodeDecodeLongArray384FS() throws Throwable {
        // long array32
        doEncodeDecodeLongArrayTestImpl(384, false);
    }

    private void doEncodeDecodeLongArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        long[] source = createPayloadArrayLongs(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 31 ? 1 : 4; // less than 31, since each element is 8 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int elementWidth = 8;

            int arrayPayloadSize = encodingWidth + 1 + (count * elementWidth); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x81); // 'long' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                long j = source[i];
                expectedEncodingWrapper.writeLong(j);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");
            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(long.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (long[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static long[] createPayloadArrayLongs(int length) {
        Random rand = new Random(System.currentTimeMillis());

        long[] payload = new long[length];
        for (int i = 0; i < length; i++) {
            payload[i] = 128 + 1 + rand.nextInt(9);
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeFloatArray25() throws Throwable {
        // float array8 less than 128 bytes
        doEncodeDecodeFloatArrayTestImpl(25, false);
    }

    @Test
    public void testEncodeDecodeFloatArray50() throws Throwable {
        // float array8 greater than 128 bytes
        doEncodeDecodeFloatArrayTestImpl(50, false);
    }

    @Test
    public void testEncodeDecodeFloatArray384() throws Throwable {
        // float array32
        doEncodeDecodeFloatArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeFloatArray25FS() throws Throwable {
        // float array8 less than 128 bytes
        doEncodeDecodeFloatArrayTestImpl(25, true);
    }

    @Test
    public void testEncodeDecodeFloatArray50FS() throws Throwable {
        // float array8 greater than 128 bytes
        doEncodeDecodeFloatArrayTestImpl(50, true);
    }

    @Test
    public void testEncodeDecodeFloatArray384FS() throws Throwable {
        // float array32
        doEncodeDecodeFloatArrayTestImpl(384, true);
    }

    private void doEncodeDecodeFloatArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        float[] source = createPayloadArrayFloats(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 63 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 4); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x72); // 'float' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.writeFloat(source[i]);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");
            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(float.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (float[]) decoded, 0.0F, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static float[] createPayloadArrayFloats(int length) {
        Random rand = new Random(System.currentTimeMillis());

        float[] payload = new float[length];
        for (int i = 0; i < length; i++) {
            payload[i] = 64 + 1 + rand.nextInt(9);
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeDoubleArray10() throws Throwable {
        // double array8 less than 128 bytes
        doEncodeDecodeDoubleArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeDoubleArray25() throws Throwable {
        // double array8 greater than 128 bytes
        doEncodeDecodeDoubleArrayTestImpl(25, false);
    }

    @Test
    public void testEncodeDecodeDoubleArray384() throws Throwable {
        // double array32
        doEncodeDecodeDoubleArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeDoubleArray10FS() throws Throwable {
        // double array8 less than 128 bytes
        doEncodeDecodeDoubleArrayTestImpl(10, true);
    }

    @Test
    public void testEncodeDecodeDoubleArray25FS() throws Throwable {
        // double array8 greater than 128 bytes
        doEncodeDecodeDoubleArrayTestImpl(25, true);
    }

    @Test
    public void testEncodeDecodeDoubleArray384FS() throws Throwable {
        // double array32
        doEncodeDecodeDoubleArrayTestImpl(384, true);
    }

    private void doEncodeDecodeDoubleArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        double[] source = createPayloadArrayDoubles(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 31 ? 1 : 4; // less than 31, since each element is 8 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 8); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x82); // 'double' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.writeDouble(source[i]);
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");

            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(double.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (double[]) decoded, 0.0F, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static double[] createPayloadArrayDoubles(int length) {
        Random rand = new Random(System.currentTimeMillis());

        double[] payload = new double[length];
        for (int i = 0; i < length; i++) {
            payload[i] = 64 + 1 + rand.nextInt(9);
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeCharArray25() throws Throwable {
        // char array8 less than 128 bytes
        doEncodeDecodeCharArrayTestImpl(25, false);
    }

    @Test
    public void testEncodeDecodeCharArray50() throws Throwable {
        // char array8 greater than 128 bytes
        doEncodeDecodeCharArrayTestImpl(50, false);
    }

    @Test
    public void testEncodeDecodeCharArray384() throws Throwable {
        // char array32
        doEncodeDecodeCharArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeCharArray25FS() throws Throwable {
        // char array8 less than 128 bytes
        doEncodeDecodeCharArrayTestImpl(25, true);
    }

    @Test
    public void testEncodeDecodeCharArray50FS() throws Throwable {
        // char array8 greater than 128 bytes
        doEncodeDecodeCharArrayTestImpl(50, true);
    }

    @Test
    public void testEncodeDecodeCharArray384FS() throws Throwable {
        // char array32
        doEncodeDecodeCharArrayTestImpl(384, true);
    }

    private void doEncodeDecodeCharArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        char[] source = createPayloadArrayChars(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = count < 63 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 4); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            ProtonBuffer expectedEncodingWrapper = ProtonBufferAllocator.defaultAllocator().allocate(expectedEncodedArraySize);
            expectedEncodingWrapper.setWriteOffset(0);

            // Write the array encoding code, array size, and element count
            if (count < 254) {
                expectedEncodingWrapper.writeByte((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.writeByte((byte) arrayPayloadSize);
                expectedEncodingWrapper.writeByte((byte) count);
            } else {
                expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.writeInt(arrayPayloadSize);
                expectedEncodingWrapper.writeInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0x73); // 'char' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.writeInt(source[i]); //4 byte encoding
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");
            final byte[] expectedEncoding = ProtonBufferUtils.toByteArray(expectedEncodingWrapper);

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadOffset(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            final int oldPos = buffer.getReadOffset();
            buffer.readBytes(actualEncoding, 0, actualEncoding.length);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.setReadOffset(oldPos);

            final Object decoded;
            if (fromStream) {
                InputStream stream = new ProtonBufferInputStream(buffer);
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(char.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (char[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static char[] createPayloadArrayChars(int length) {
        Random rand = new Random(System.currentTimeMillis());

        char[] payload = new char[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (char) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    public void testSkipValueSmallByteArray() throws IOException {
        doTestSkipValueOnArrayOfSize(200, false);
    }

    @Test
    public void testSkipValueLargeByteArray() throws IOException {
        doTestSkipValueOnArrayOfSize(1024, false);
    }

    @Test
    public void testSkipValueSmallByteArrayFromStream() throws IOException {
        doTestSkipValueOnArrayOfSize(200, true);
    }

    @Test
    public void testSkipValueLargeByteArrayFromStream() throws IOException {
        doTestSkipValueOnArrayOfSize(1024, true);
    }

    private void doTestSkipValueOnArrayOfSize(int arraySize, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        byte[] bytes = new byte[arraySize];
        filler.nextBytes(bytes);

        for (int i = 0; i < 10; ++i) {
            encoder.writeArray(buffer, encoderState, bytes);
        }

        byte[] expected = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        encoder.writeObject(buffer, encoderState, expected);

        final Object result;

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            for (int i = 0; i < 10; ++i) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Object.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder.isArrayType());
                typeDecoder.skipValue(stream, streamDecoderState);
            }

            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            for (int i = 0; i < 10; ++i) {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Object.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder.isArrayType());
                typeDecoder.skipValue(buffer, decoderState);
            }

            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof byte[]);

        byte[] value = (byte[]) result;
        assertArrayEquals(expected, value);
    }

    @Test
    public void testArrayOfInts() throws IOException {
        doTestArrayOfInts(false);
    }

    @Test
    public void testArrayOfIntsFromStream() throws IOException {
        doTestArrayOfInts(true);
    }

    public void doTestArrayOfInts(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        int[] source = new int[size];
        for (int i = 0; i < size; ++i) {
            source[i] = random.nextInt();
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

        int[] array = (int[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }
}
