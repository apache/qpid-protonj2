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
package org.apache.qpid.protonj2.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.util.NoLocalType;
import org.apache.qpid.protonj2.types.UnknownDescribedType;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.Test;

/**
 * Tests the handling of UnknownDescribedType instances.
 */
public class UnknownDescribedTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecodeUnknownDescribedType() throws Exception {
        doTestDecodeUnknownDescribedType(false);
    }

    @Test
    public void testDecodeUnknownDescribedTypeFromStream() throws Exception {
        doTestDecodeUnknownDescribedType(true);
    }

    private void doTestDecodeUnknownDescribedType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        encoder.writeObject(buffer, encoderState, NoLocalType.NO_LOCAL);

        final Object result;
        if (fromStream) {
            final InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof UnknownDescribedType);
        UnknownDescribedType resultTye = (UnknownDescribedType) result;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testUnknownDescribedTypeInList() throws IOException {
        doTestUnknownDescribedTypeInList(false);
    }

    @Test
    public void testUnknownDescribedTypeInListFromStream() throws IOException {
        doTestUnknownDescribedTypeInList(true);
    }

    @SuppressWarnings("unchecked")
    private void doTestUnknownDescribedTypeInList(boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        List<Object> listOfUnknowns = new ArrayList<>();

        listOfUnknowns.add(NoLocalType.NO_LOCAL);

        encoder.writeList(buffer, encoderState, listOfUnknowns);

        final Object result;
        if (fromStream) {
            final InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof List);

        final List<Object> decodedList = (List<Object>) result;
        assertEquals(1, decodedList.size());

        final Object listEntry = decodedList.get(0);
        assertTrue(listEntry instanceof UnknownDescribedType);

        UnknownDescribedType resultTye = (UnknownDescribedType) listEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testUnknownDescribedTypeInMap() throws IOException {
        doTestUnknownDescribedTypeInMap(false);
    }

    @Test
    public void testUnknownDescribedTypeInMapFromStream() throws IOException {
        doTestUnknownDescribedTypeInMap(true);
    }

    @SuppressWarnings("unchecked")
    private void doTestUnknownDescribedTypeInMap(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Object, Object> mapOfUnknowns = new HashMap<>();

        mapOfUnknowns.put(NoLocalType.NO_LOCAL.getDescriptor(), NoLocalType.NO_LOCAL);

        encoder.writeMap(buffer, encoderState, mapOfUnknowns);

        final Object result;
        if (fromStream) {
            final InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Map);

        final Map<Object, Object> decodedMap = (Map<Object, Object>) result;
        assertEquals(1, decodedMap.size());

        final Object mapEntry = decodedMap.get(NoLocalType.NO_LOCAL.getDescriptor());
        assertTrue(mapEntry instanceof UnknownDescribedType);

        UnknownDescribedType resultTye = (UnknownDescribedType) mapEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testUnknownDescribedTypeInArray() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        NoLocalType[] arrayOfUnknown = new NoLocalType[1];

        arrayOfUnknown[0] = NoLocalType.NO_LOCAL;

        try {
            encoder.writeArray(buffer, encoderState, arrayOfUnknown);
            fail("Should not be able to write an array of unregistered described type");
        } catch (IllegalArgumentException iae) {}

        try {
            encoder.writeObject(buffer, encoderState, arrayOfUnknown);
            fail("Should not be able to write an array of unregistered described type");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testDecodeSmallSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfUnknownDescribedTypesFromStream() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfUnknownDescribedTypesFromStream() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeUnknownDescribedTypeSeries(int size, boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, NoLocalType.NO_LOCAL);
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
            assertTrue(result instanceof UnknownDescribedType);

            UnknownDescribedType resultTye = (UnknownDescribedType) result;
            assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
        }
    }

    @Test
    public void testDecodingOfDeeplyNestedUnknownDescribedTypesFromBuffer() throws Exception {
        doTestDecodingOfDeeplyNestedUnknownDescribedTypes(false, 10);
    }

    @Test
    public void testDecodingOfDeeplyNestedUnknownDescribedTypesFromStream() throws Exception {
        doTestDecodingOfDeeplyNestedUnknownDescribedTypes(true, 10);
    }

    private void doTestDecodingOfDeeplyNestedUnknownDescribedTypes(boolean fromStream, int depthLimit) throws IOException {
        final UnknownDescribedType toEncode = createNode(depthLimit + 1, 0);
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, toEncode);

        streamDecoderState.setDepthLimit(depthLimit);
        decoderState.setDepthLimit(depthLimit);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertThrows(DecodeException.class, () -> streamDecoder.readObject(stream, streamDecoderState));
        } else {
            assertThrows(DecodeException.class, () -> decoder.readObject(buffer.copy(true), decoderState));
        }

        // Encode up to the limit instead which should work
        streamDecoderState.reset();
        decoderState.reset();

        streamDecoderState.setDepthLimit(depthLimit + 1);
        decoderState.setDepthLimit(depthLimit + 1);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertTrue(streamDecoder.readObject(stream, streamDecoderState) instanceof UnknownDescribedType);
        } else {
            assertTrue(decoder.readObject(buffer.copy(true), decoderState) instanceof UnknownDescribedType);
        }
    }

    private UnknownDescribedType createNode(int limit, int depth) {
        final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0xAA00468C00000003L);

        if (++depth > limit) {
            return new UnknownDescribedType(DESCRIPTOR_CODE, null);
        } else {
            return new UnknownDescribedType(DESCRIPTOR_CODE, List.of(createNode(limit, depth)));
        }
    }

    @Test
    public void testDecodeFailsWhenArrayOfValuesSizeIsToLargeArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfValuesSizeIsToLarge(EncodingCodes.ARRAY8, false);
    }

    @Test
    public void testDecodeFailsWhenArrayOfValuesSizeIsToLargeArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfValuesSizeIsToLarge(EncodingCodes.ARRAY32, false);
    }

    @Test
    public void testDecodeFailsWhenArrayOfValuesSizeIsToLargeArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfValuesSizeIsToLarge(EncodingCodes.ARRAY8, true);
    }

    @Test
    public void testDecodeFailsWhenArrayOfValuesSizeIsToLargeArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfValuesSizeIsToLarge(EncodingCodes.ARRAY32, true);
    }

    private void doTestDecodeFailsWhenArrayOfValuesSizeIsToLarge(byte arrayType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        // First show that we can do this if the data is correct
        if (arrayType == EncodingCodes.ARRAY32) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(12);  // Size
            buffer.writeInt(2);  // Count
        } else {
            buffer.writeByte(EncodingCodes.ARRAY8);
            buffer.writeByte((byte) 9);  // Size
            buffer.writeByte((byte) 2);  // Count
        }
        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 1);  // Size
        buffer.writeByte((byte) 0);  // Count
        buffer.writeByte((byte) 1);  // Size
        buffer.writeByte((byte) 0);  // Count

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder.readValue(stream, streamDecoderState) instanceof UnknownDescribedType[]);
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder.readValue(buffer, decoderState) instanceof UnknownDescribedType[]);
        }

        // Now check that if we set the array size to big it will not decode
        ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().allocate();

        if (arrayType == EncodingCodes.ARRAY32) {
            buffer2.writeByte(EncodingCodes.ARRAY32);
            buffer2.writeInt(13);  // Size
            buffer2.writeInt(2);   // Count
        } else {
            buffer2.writeByte(EncodingCodes.ARRAY8);
            buffer2.writeByte((byte) 10);  // Size
            buffer2.writeByte((byte) 2);  // Count
        }
        buffer2.writeByte((byte) 0); // Described Type Indicator
        buffer2.writeByte(EncodingCodes.SMALLULONG);
        buffer2.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer2.writeByte(EncodingCodes.LIST8);
        buffer2.writeByte((byte) 1);  // Size
        buffer2.writeByte((byte) 0);  // Count
        buffer2.writeByte((byte) 1);  // Size
        buffer2.writeByte((byte) 0);  // Count

        if (fromStream) {
            streamDecoderState.setMaxArraySize(1);
            InputStream stream = new ProtonBufferInputStream(buffer2);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer2, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(buffer2, decoderState));
        }
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithList0EncodingsArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithList0Encodings(EncodingCodes.ARRAY8, false);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithList0EncodingsArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithList0Encodings(EncodingCodes.ARRAY32, false);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithList0EncodingsArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithList0Encodings(EncodingCodes.ARRAY8, true);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithList0EncodingsArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithList0Encodings(EncodingCodes.ARRAY32, true);
    }

    private void doTestDecodeFailsWhenArrayOfTypeWithList0Encodings(byte arrayType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        // First show that we can do this if the data is correct
        if (arrayType == EncodingCodes.ARRAY32) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(8);  // Size
            buffer.writeInt(2);  // Count
        } else {
            buffer.writeByte(EncodingCodes.ARRAY8);
            buffer.writeByte((byte) 5);  // Size
            buffer.writeByte((byte) 2);  // Count
        }
        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST0);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(buffer, decoderState));
        }

        buffer.setReadOffset(0);  // Reset and try with limits lifted

        decoderState.setMaxZeroWidthArrayElements(2);
        streamDecoderState.setMaxZeroWidthArrayElements(2);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder.readValue(stream, streamDecoderState) instanceof UnknownDescribedType[]);
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder.readValue(buffer, decoderState) instanceof UnknownDescribedType[]);
        }
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCountArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCount(EncodingCodes.ARRAY8, false);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCountArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCount(EncodingCodes.ARRAY32, false);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCountArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCount(EncodingCodes.ARRAY8, true);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCountArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCount(EncodingCodes.ARRAY32, true);
    }

    private void doTestDecodeFailsWhenArrayOfTypeWithListEncodingsHasOverLargeCount(byte arrayType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        streamDecoderState.setMaxArraySize(16);

        // First show that we can do this if the data is correct
        if (arrayType == EncodingCodes.ARRAY32) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(10);  // Size
            buffer.writeInt(17);  // Count
        } else {
            buffer.writeByte(EncodingCodes.ARRAY8);
            buffer.writeByte((byte) 7);  // Size
            buffer.writeByte((byte) 17);  // Count
        }
        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 0);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(buffer, decoderState));
        }
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithULong0EncodingsArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, false, EncodingCodes.ULONG0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithULong0EncodingsArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, false, EncodingCodes.ULONG0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithULong0EncodingsArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, true, EncodingCodes.ULONG0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithULong0EncodingsArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, true, EncodingCodes.ULONG0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithUInt0EncodingsArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, false, EncodingCodes.UINT0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithUInt0EncodingsArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, false, EncodingCodes.UINT0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithUInt0EncodingsArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, true, EncodingCodes.UINT0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithUInt0EncodingsArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, true, EncodingCodes.UINT0);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithNullEncodingsArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, false, EncodingCodes.NULL);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithNullEncodingsArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, false, EncodingCodes.NULL);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithNullEncodingsArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, true, EncodingCodes.NULL);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithNullEncodingsArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, true, EncodingCodes.NULL);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithTrueEncodingsArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, false, EncodingCodes.BOOLEAN_TRUE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithTrueEncodingsArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, false, EncodingCodes.BOOLEAN_TRUE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithTrueEncodingsArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, true, EncodingCodes.BOOLEAN_TRUE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithTrueEncodingsArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, true, EncodingCodes.BOOLEAN_TRUE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithFalseEncodingsArray8() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, false, EncodingCodes.BOOLEAN_FALSE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithFalseEncodingsArray32() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, false, EncodingCodes.BOOLEAN_FALSE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithFalseEncodingsArray8FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY8, true, EncodingCodes.BOOLEAN_FALSE);
    }

    @Test
    public void testDecodeFailsWhenArrayOfTypeWithFalseEncodingsArray32FS() throws IOException {
        doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(EncodingCodes.ARRAY32, true, EncodingCodes.BOOLEAN_FALSE);
    }

    private void doTestDecodeFailsWhenArrayOfTypeWithUZeroSizedEncodings(byte arrayType, boolean fromStream, byte code) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        // First show that we can do this if the data is correct
        if (arrayType == EncodingCodes.ARRAY32) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(8);  // Size
            buffer.writeInt(2);  // Count
        } else {
            buffer.writeByte(EncodingCodes.ARRAY8);
            buffer.writeByte((byte) 5);  // Size
            buffer.writeByte((byte) 2);  // Count
        }
        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(code);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertThrows(DecodeException.class, () -> typeDecoder.readValue(buffer, decoderState));
        }

        buffer.setReadOffset(0);  // Reset and try with limits lifted

        decoderState.setMaxZeroWidthArrayElements(2);
        streamDecoderState.setMaxZeroWidthArrayElements(2);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder.readValue(stream, streamDecoderState) instanceof UnknownDescribedType[]);
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Object.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder.readValue(buffer, decoderState) instanceof UnknownDescribedType[]);
        }
    }

    @Test
    public void testCannotDecodeDescribedTypeWithNonPrimitiveValue() throws Exception {
        doTestCannotDecodeDescribedTypeWithNonPrimitiveValue(false);
    }

    @Test
    public void testCannotDecodeDescribedTypeWithNonPrimitiveValueFS() throws Exception {
        doTestCannotDecodeDescribedTypeWithNonPrimitiveValue(true);
    }

    private void doTestCannotDecodeDescribedTypeWithNonPrimitiveValue(boolean fromStream) throws IOException {
        final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0xAA00468C00000003L);
        final UnknownDescribedType toEncode =
            new UnknownDescribedType(DESCRIPTOR_CODE, new UnknownDescribedType(DESCRIPTOR_CODE, null));
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, toEncode);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertThrows(DecodeException.class, () -> streamDecoder.readObject(stream, streamDecoderState));
        } else {
            assertThrows(DecodeException.class, () -> decoder.readObject(buffer.copy(true), decoderState));
        }
    }

    @Test
    public void testDecodeExceptionThrownForInvalidDescribedTypeDescriptor() throws IOException {
        doTestDecodeExceptionThrownForInvliadDescribefTypeDescriptor(false);
    }

    @Test
    public void testDecodeExceptionThrownForInvalidDescribedTypeDescriptorFS() throws IOException {
        doTestDecodeExceptionThrownForInvliadDescribefTypeDescriptor(true);
    }

    private void doTestDecodeExceptionThrownForInvliadDescribefTypeDescriptor(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST0);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertThrows(DecodeException.class, () -> streamDecoder.readObject(stream, streamDecoderState));
        } else {
            assertThrows(DecodeException.class, () -> decoder.readObject(buffer.copy(true), decoderState));
        }

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertThrows(DecodeException.class, () -> streamDecoder.readNextTypeDecoder(stream, streamDecoderState));
        } else {
            assertThrows(DecodeException.class, () -> decoder.readNextTypeDecoder(buffer.copy(true), decoderState));
        }
    }

    @Test
    public void testDecodeExceptionThrownForInvalidNullDescribedTypeDescriptor() throws IOException {
        doTestDecodeExceptionThrownForInvalidNullDescribedTypeDescriptor(false);
    }

    @Test
    public void testDecodeExceptionThrownForInvalidNulDescribedTypeDescriptorFS() throws IOException {
        doTestDecodeExceptionThrownForInvalidNullDescribedTypeDescriptor(true);
    }

    private void doTestDecodeExceptionThrownForInvalidNullDescribedTypeDescriptor(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(NoLocalType.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST0);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertThrows(DecodeException.class, () -> streamDecoder.readObject(stream, streamDecoderState));
        } else {
            assertThrows(DecodeException.class, () -> decoder.readObject(buffer.copy(true), decoderState));
        }

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer.copy(true));
            assertThrows(DecodeException.class, () -> streamDecoder.readNextTypeDecoder(stream, streamDecoderState));
        } else {
            assertThrows(DecodeException.class, () -> decoder.readNextTypeDecoder(buffer.copy(true), decoderState));
        }
    }
}
