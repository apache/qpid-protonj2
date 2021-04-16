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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
import org.apache.qpid.protonj2.types.Binary;
import org.junit.jupiter.api.Test;

public class MapTypeCodecTest extends CodecTestSupport {

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
                streamDecoder.readMap(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readMap(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testDecodeSmallSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfMapsFromStream() throws IOException {
        doTestDecodeMapSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfMapsFromStream() throws IOException {
        doTestDecodeMapSeries(LARGE_SIZE, true);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeMapSeries(int size, boolean fromStream) throws IOException {
        String myBoolKey = "myBool";
        boolean myBool = true;
        String myByteKey = "myByte";
        byte myByte = 4;
        String myBytesKey = "myBytes";
        byte[] myBytes = myBytesKey.getBytes();
        String myCharKey = "myChar";
        char myChar = 'd';
        String myDoubleKey = "myDouble";
        double myDouble = 1234567890123456789.1234;
        String myFloatKey = "myFloat";
        float myFloat = 1.1F;
        String myIntKey = "myInt";
        int myInt = Integer.MAX_VALUE;
        String myLongKey = "myLong";
        long myLong = Long.MAX_VALUE;
        String myShortKey = "myShort";
        short myShort = 25;
        String myStringKey = "myString";
        String myString = myStringKey;

        Map<String, Object> map = new LinkedHashMap<>();
        map.put(myBoolKey, myBool);
        map.put(myByteKey, myByte);
        map.put(myBytesKey, new Binary(myBytes));
        map.put(myCharKey, myChar);
        map.put(myDoubleKey, myDouble);
        map.put(myFloatKey, myFloat);
        map.put(myIntKey, myInt);
        map.put(myLongKey, myLong);
        map.put(myShortKey, myShort);
        map.put(myStringKey, myString);

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, map);
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof Map);

            Map<String, Object> resultMap = (Map<String, Object>) result;

            assertEquals(map.size(), resultMap.size());
        }
    }

    @Test
    public void testArrayOfMApsOfStringToUUIDs() throws IOException {
        testArrayOfMApsOfStringToUUIDs(false);
    }

    @Test
    public void testArrayOfMApsOfStringToUUIDsFS() throws IOException {
        testArrayOfMApsOfStringToUUIDs(true);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void testArrayOfMApsOfStringToUUIDs(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Map<String, UUID>[] source = new LinkedHashMap[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = new LinkedHashMap<>();
            source[i].put("1", UUID.randomUUID());
            source[i].put("2", UUID.randomUUID());
            source[i].put("3", UUID.randomUUID());
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

        Map[] map = (Map[]) result;
        assertEquals(source.length, map.length);

        for (int i = 0; i < map.length; ++i) {
            assertEquals(source[i], map[i]);
        }
    }

    @Test
    public void testMapOfArraysOfUUIDsIndexedByString() throws IOException {
        testMapOfArraysOfUUIDsIndexedByString(false);
    }

    @Test
    public void testMapOfArraysOfUUIDsIndexedByStringFS() throws IOException {
        testMapOfArraysOfUUIDsIndexedByString(true);
    }

    @SuppressWarnings({ "unchecked" })
    private void testMapOfArraysOfUUIDsIndexedByString(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[] element1 = new UUID[] { UUID.randomUUID() };
        UUID[] element2 = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };
        UUID[] element3 = new UUID[] { UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() };

        UUID[][] expected = new UUID[][] { element1, element2, element3 };

        Map<String, UUID[]> source = new LinkedHashMap<>();
        source.put("1", element1);
        source.put("2", element2);
        source.put("3", element3);

        encoder.writeMap(buffer, encoderState, source);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Map);

        Map<String, UUID[]> map = (Map<String, UUID[]>) result;
        assertEquals(source.size(), map.size());

        for (int i = 1; i <= map.size(); ++i) {
            Object entry = map.get(Integer.toString(i));
            assertNotNull(entry);
            assertTrue(entry.getClass().isArray());
            UUID[] uuids = (UUID[]) entry;
            assertEquals(i, uuids.length);
            assertArrayEquals(expected[i - 1], uuids);
        }
    }

    @Test
    public void testSizeToLargeValidationMAP32() throws IOException {
        dotestSizeToLargeValidation(EncodingCodes.MAP32, true);
    }

    @Test
    public void testSizeToLargeValidationMAP8() throws IOException {
        dotestSizeToLargeValidation(EncodingCodes.MAP8, true);
    }

    private void dotestSizeToLargeValidation(byte encodingCode, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(encodingCode);
        if (encodingCode == EncodingCodes.MAP32) {
            buffer.writeInt(Integer.MAX_VALUE);
            buffer.writeInt(4);
        } else {
            buffer.writeByte(Byte.MAX_VALUE);
            buffer.writeByte(4);
        }
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(4);
        buffer.writeBytes("test".getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(5);
        buffer.writeBytes("value".getBytes(StandardCharsets.UTF_8));

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.peekNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Map.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), encodingCode & 0xFF);
        } else {
            TypeDecoder<?> typeDecoder = decoder.peekNextTypeDecoder(buffer, decoderState);
            assertEquals(Map.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), encodingCode & 0xFF);
        }

        if (fromStream) {
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("should throw an IllegalArgumentException");
            } catch (IllegalArgumentException iae) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("should throw an IllegalArgumentException");
            } catch (IllegalArgumentException iae) {}
        }
    }

    @Test
    public void testOddElementCountDetectedMAP32() throws IOException {
        doTestOddElementCountDetected(EncodingCodes.MAP32, false);
    }

    @Test
    public void testOddElementCountDetectedMAP8() throws IOException {
        doTestOddElementCountDetected(EncodingCodes.MAP8, false);
    }

    @Test
    public void testOddElementCountDetectedMAP32FS() throws IOException {
        doTestOddElementCountDetected(EncodingCodes.MAP32, true);
    }

    @Test
    public void testOddElementCountDetectedMAP8FS() throws IOException {
        doTestOddElementCountDetected(EncodingCodes.MAP8, true);
    }

    private void doTestOddElementCountDetected(byte encodingCode, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(encodingCode);
        if (encodingCode == EncodingCodes.MAP32) {
            buffer.writeInt(17);
            buffer.writeInt(1);
        } else {
            buffer.writeByte(17);
            buffer.writeByte(1);
        }
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(4);
        buffer.writeBytes("test".getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(5);
        buffer.writeBytes("value".getBytes(StandardCharsets.UTF_8));

        if (fromStream) {
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("should throw an IllegalArgumentException");
            } catch (IllegalArgumentException iae) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("should throw an IllegalArgumentException");
            } catch (IllegalArgumentException iae) {}
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

    @SuppressWarnings("unchecked")
    public void doTestSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Map<String, UUID> skip = new HashMap<>();
        for (int i = 0; i < 10; ++i) {
            skip.put(UUID.randomUUID().toString(), UUID.randomUUID());
        }

        for (int i = 0; i < 10; ++i) {
            encoder.writeMap(buffer, encoderState, skip);
        }

        Map<String, UUID> expected = new LinkedHashMap<>();
        expected.put(UUID.randomUUID().toString(), UUID.randomUUID());

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Map.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Map.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Map);

        Map<String, UUID> value = (Map<String, UUID>) result;
        assertEquals(expected, value);
    }

    @Test
    public void testEncodeMapWithUnknownEntryValueType() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("unknown", new MyUnknownTestType());

        doTestEncodeMapWithUnknownEntryValueTypeTestImpl(map);
    }

    @Test
    public void testEncodeSubMapWithUnknownEntryValueType() throws Exception {
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("unknown", new MyUnknownTestType());

        Map<String, Object> map = new HashMap<>();
        map.put("submap", subMap);

        doTestEncodeMapWithUnknownEntryValueTypeTestImpl(map);
    }

    private void doTestEncodeMapWithUnknownEntryValueTypeTestImpl(Map<String, Object> map) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        try {
            encoder.writeMap(buffer, encoderState, map);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("Cannot find encoder for type"));
            assertThat(iae.getMessage(), containsString(MyUnknownTestType.class.getSimpleName()));
        }
    }

    @Test
    public void testEncodeMapWithUnknownEntryKeyType() throws Exception {
        Map<Object, String> map = new HashMap<>();
        map.put(new MyUnknownTestType(), "unknown");

        doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(map);
    }

    @Test
    public void testEncodeSubMapWithUnknownEntryKeyType() throws Exception {
        Map<Object, String> subMap = new HashMap<>();
        subMap.put(new MyUnknownTestType(), "unknown");

        Map<String, Object> map = new HashMap<>();
        map.put("submap", subMap);

        doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(map);
    }

    private void doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(Map<?, ?> map) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        try {
            encoder.writeMap(buffer, encoderState, map);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("Cannot find encoder for type"));
            assertThat(iae.getMessage(), containsString(MyUnknownTestType.class.getSimpleName()));
        }
    }

    private static class MyUnknownTestType {

    }
}
