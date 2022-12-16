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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

/**
 * Test for the Proton List encoder / decoder
 */
@Timeout(20)
public class ListTypeCodecTest extends CodecTestSupport {

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

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            try {
                streamDecoder.readList(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readList(buffer, decoderState);
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

        buffer.writeByte(EncodingCodes.NULL);

        buffer.writeByte(EncodingCodes.LIST0);

        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 4);
        buffer.writeByte((byte) 2);
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 1);
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 2);

        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(4);
        buffer.writeInt(2);
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 1);
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 2);

        List<Byte> expected = new ArrayList<>();

        expected.add(Byte.valueOf((byte) 1));
        expected.add(Byte.valueOf((byte) 2));

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            assertNull(streamDecoder.readList(stream, streamDecoderState));
            assertEquals(Collections.EMPTY_LIST, streamDecoder.readList(stream, streamDecoderState));
            assertEquals(expected, streamDecoder.readList(stream, streamDecoderState));
            assertEquals(expected, streamDecoder.readList(stream, streamDecoderState));
        } else {
            assertNull(decoder.readList(buffer, decoderState));
            assertEquals(Collections.EMPTY_LIST, decoder.readList(buffer, decoderState));
            assertEquals(expected, decoder.readList(buffer, decoderState));
            assertEquals(expected, decoder.readList(buffer, decoderState));
        }
    }

    @Test
    public void testDecodeSmallSeriesOfSymbolLists() throws IOException {
        doTestDecodeSymbolListSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfSymbolLists() throws IOException {
        doTestDecodeSymbolListSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfSymbolListsFromStream() throws IOException {
        doTestDecodeSymbolListSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfSymbolListsFromStream() throws IOException {
        doTestDecodeSymbolListSeries(LARGE_SIZE, true);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeSymbolListSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        List<Object> list = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            list.add(Symbol.valueOf(String.valueOf(i)));
        }

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, list);
        }

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof List);

            List<Object> resultList = (List<Object>) result;

            assertEquals(list.size(), resultList.size());
        }
    }

    @Test
    public void testDecodeSmallSeriesOfLists() throws IOException {
        doTestDecodeListSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfLists() throws IOException {
        doTestDecodeListSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfListsFS() throws IOException {
        doTestDecodeListSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfListsFS() throws IOException {
        doTestDecodeListSeries(LARGE_SIZE, true);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeListSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        List<Object> list = new ArrayList<>();

        Date timeNow = new Date(System.currentTimeMillis());

        list.add("ID:Message-1:1:1:0");
        list.add(new Binary(new byte[1]));
        list.add("queue:work");
        list.add(Symbol.valueOf("text/UTF-8"));
        list.add(Symbol.valueOf("text"));
        list.add(timeNow);
        list.add(UnsignedInteger.valueOf(1));
        list.add(UUID.randomUUID());

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, list);
        }

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof List);

            List<Object> resultList = (List<Object>) result;

            assertEquals(list.size(), resultList.size());
        }
    }

    @Test
    public void testArrayOfListsOfUUIDs() throws IOException {
        doTestArrayOfListsOfUUIDs(false);
    }

    @Test
    public void testArrayOfListsOfUUIDsFromStream() throws IOException {
        doTestArrayOfListsOfUUIDs(true);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void doTestArrayOfListsOfUUIDs(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        ArrayList<UUID>[] source = new ArrayList[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = new ArrayList<>(3);
            source[i].add(UUID.randomUUID());
            source[i].add(UUID.randomUUID());
            source[i].add(UUID.randomUUID());
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

        List[] list = (List[]) result;
        assertEquals(source.length, list.length);

        for (int i = 0; i < list.length; ++i) {
            assertEquals(source[i], list[i]);
        }
    }

    @Test
    public void testCountExceedsRemainingDetectedList32() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(8);
        buffer.writeInt(Integer.MAX_VALUE);

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCountExceedsRemainingDetectedList8() throws IOException {
        testCountExceedsRemainingDetectedList8(false);
    }

    @Test
    public void testCountExceedsRemainingDetectedList8FS() throws IOException {
        testCountExceedsRemainingDetectedList8(true);
    }

    private void testCountExceedsRemainingDetectedList8(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 4);
        buffer.writeByte(Byte.MAX_VALUE);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

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
    public void testDecodeEmptyList() throws IOException {
        testDecodeEmptyList(false);
    }

    @Test
    public void testDecodeEmptyListFS() throws IOException {
        testDecodeEmptyList(true);
    }

    @SuppressWarnings("unchecked")
    private void testDecodeEmptyList(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.LIST0);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.peekNextTypeDecoder(stream, streamDecoderState);
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(EncodingCodes.LIST0 & 0xff, ((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode());

            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            TypeDecoder<?> typeDecoder = decoder.peekNextTypeDecoder(buffer, decoderState);
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(EncodingCodes.LIST0 & 0xff, ((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode());

            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof List);

        List<UUID> value = (List<UUID>) result;
        assertEquals(0, value.size());
    }

    @Test
    public void testEncodeEmptyListIsList0() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeList(buffer, encoderState, new ArrayList<>());

        assertEquals(1, buffer.getReadableBytes());
        assertEquals(EncodingCodes.LIST0, buffer.readByte());
    }

    @Test
    public void testDecodeFailsEarlyOnInvalidLengthList8() throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(16).implicitGrowthLimit(16);

        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 255);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(List.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.readValue(buffer, decoderState);
            fail("Should not be able to read list with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(2, buffer.getReadOffset());
    }

    @Test
    public void testDecodeFailsEarlyOnInvalidLengthList32() throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(16).implicitGrowthLimit(16);

        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(Integer.MAX_VALUE);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(List.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.readValue(buffer, decoderState);
            fail("Should not be able to read list with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(5, buffer.getReadOffset());
    }

    @Test
    public void testDecodeFailsEarlyOnInvalidElementCountForList8() throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(16).implicitGrowthLimit(16);

        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 255);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(List.class, typeDecoder.getTypeClass());
        assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
        assertEquals(EncodingCodes.LIST8 & 0xff, ((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode());

        try {
            typeDecoder.readValue(buffer, decoderState);
            fail("Should not be able to read list with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(3, buffer.getReadOffset());
    }

    @Test
    public void testDecodeFailsEarlyOnInvalidElementLengthList32() throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(16).implicitGrowthLimit(16);

        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(2);
        buffer.writeInt(Integer.MAX_VALUE);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(List.class, typeDecoder.getTypeClass());
        assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
        assertEquals(EncodingCodes.LIST32 & 0xff, ((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode());

        try {
            typeDecoder.readValue(buffer, decoderState);
            fail("Should not be able to read list with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(9, buffer.getReadOffset());
    }

    @Test
    public void testSkipValue() throws IOException {
        testSkipValue(false);
    }

    @Test
    public void testSkipValueFS() throws IOException {
        testSkipValue(true);
    }

    @SuppressWarnings("unchecked")
    private void testSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        List<UUID> skip = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            skip.add(UUID.randomUUID());
        }

        for (int i = 0; i < 10; ++i) {
            encoder.writeList(buffer, encoderState, skip);
        }

        List<UUID> expected = new ArrayList<>();
        expected.add(UUID.randomUUID());

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
                assertEquals(List.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(List.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof List);

        List<UUID> value = (List<UUID>) result;
        assertEquals(expected, value);
    }

    @Test
    public void testEncodeListWithUnknownEntryType() throws Exception {
        List<Object> list = new ArrayList<>();
        list.add(new MyUnknownTestType());

        doTestEncodeListWithUnknownEntryTypeTestImpl(list);
    }

    @Test
    public void testEncodeSubListWithUnknownEntryType() throws Exception {
        List<Object> subList = new ArrayList<>();
        subList.add(new MyUnknownTestType());

        List<Object> list = new ArrayList<>();
        list.add(subList);

        doTestEncodeListWithUnknownEntryTypeTestImpl(list);
    }

    @Test
    public void testStreamSkipOfEncodingHandlesIOException() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        List<UUID> skip = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            skip.add(UUID.randomUUID());
        }

        encoder.writeList(buffer, encoderState, skip);

        InputStream stream = new ProtonBufferInputStream(buffer);
        StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
        assertEquals(List.class, typeDecoder.getTypeClass());

        stream = Mockito.spy(stream);

        Mockito.when(stream.skip(Mockito.anyLong())).thenThrow(EOFException.class);

        try {
            typeDecoder.skipValue(stream, streamDecoderState);
            fail("Expected an exception on skip of encoded list failure.");
        } catch (DecodeException dex) {}
    }

    private void doTestEncodeListWithUnknownEntryTypeTestImpl(List<Object> list) {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        try {
            encoder.writeObject(buffer, encoderState, list);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("Cannot find encoder for type"));
            assertThat(iae.getMessage(), containsString(MyUnknownTestType.class.getSimpleName()));
        }
    }

    private static class MyUnknownTestType {

    }
}
