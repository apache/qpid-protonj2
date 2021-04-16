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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.junit.jupiter.api.Test;

/**
 * Test the BooleanTypeDecoder for correctness
 */
public class BooleanTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsBoolean() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsBoolean(false);
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsBooleanFS() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsBoolean(true);
    }

    private void testDecoderThrowsWhenAskedToReadWrongTypeAsBoolean(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        if (fromStream) {
            try {
                streamDecoder.readBoolean(stream, streamDecoderState);
                fail("Should not allow read of integer type as boolean");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readBoolean(stream, streamDecoderState, false);
                fail("Should not allow read of integer type as boolean");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readBoolean(buffer, decoderState);
                fail("Should not allow read of integer type as boolean");
            } catch (DecodeException e) {}

            try {
                decoder.readBoolean(buffer, decoderState, false);
                fail("Should not allow read of integer type as boolean");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testDecodeBooleanEncodedBytes() throws Exception {
        testDecodeBooleanEncodedBytes(false);
    }

    @Test
    public void testDecodeBooleanEncodedBytesFS() throws Exception {
        testDecodeBooleanEncodedBytes(true);
    }

    private void testDecodeBooleanEncodedBytes(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.BOOLEAN_TRUE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.BOOLEAN_FALSE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(1);

        if (fromStream) {
            boolean result1 = streamDecoder.readBoolean(stream, streamDecoderState);
            boolean result2 = streamDecoder.readBoolean(stream, streamDecoderState);
            boolean result3 = streamDecoder.readBoolean(stream, streamDecoderState);
            boolean result4 = streamDecoder.readBoolean(stream, streamDecoderState);

            assertTrue(result1);
            assertFalse(result2);
            assertFalse(result3);
            assertTrue(result4);
        } else {
            boolean result1 = decoder.readBoolean(buffer, decoderState);
            boolean result2 = decoder.readBoolean(buffer, decoderState);
            boolean result3 = decoder.readBoolean(buffer, decoderState);
            boolean result4 = decoder.readBoolean(buffer, decoderState);

            assertTrue(result1);
            assertFalse(result2);
            assertFalse(result3);
            assertTrue(result4);
        }
    }

    @Test
    public void testPeekNextTypeDecoder() throws Exception {
        testPeekNextTypeDecoder(false);
    }

    @Test
    public void testPeekNextTypeDecoderFS() throws Exception {
        testPeekNextTypeDecoder(true);
    }

    private void testPeekNextTypeDecoder(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.BOOLEAN_TRUE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.BOOLEAN_FALSE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(1);

        if (fromStream) {
            assertEquals(Boolean.class, streamDecoder.peekNextTypeDecoder(stream, streamDecoderState).getTypeClass());
            assertTrue(streamDecoder.readBoolean(stream, streamDecoderState));
            assertEquals(Boolean.class, streamDecoder.peekNextTypeDecoder(stream, streamDecoderState).getTypeClass());
            assertFalse(streamDecoder.readBoolean(stream, streamDecoderState));
            assertEquals(Boolean.class, streamDecoder.peekNextTypeDecoder(stream, streamDecoderState).getTypeClass());
            assertFalse(streamDecoder.readBoolean(stream, streamDecoderState));
            assertEquals(Boolean.class, streamDecoder.peekNextTypeDecoder(stream, streamDecoderState).getTypeClass());
            assertTrue(streamDecoder.readBoolean(stream, streamDecoderState));
        } else {
            assertEquals(Boolean.class, decoder.peekNextTypeDecoder(buffer, decoderState).getTypeClass());
            assertTrue(decoder.readBoolean(buffer, decoderState));
            assertEquals(Boolean.class, decoder.peekNextTypeDecoder(buffer, decoderState).getTypeClass());
            assertFalse(decoder.readBoolean(buffer, decoderState));
            assertEquals(Boolean.class, decoder.peekNextTypeDecoder(buffer, decoderState).getTypeClass());
            assertFalse(decoder.readBoolean(buffer, decoderState));
            assertEquals(Boolean.class, decoder.peekNextTypeDecoder(buffer, decoderState).getTypeClass());
            assertTrue(decoder.readBoolean(buffer, decoderState));
        }
    }

    @Test
    public void testDecodeBooleanEncodedBytesAsPrimtives() throws Exception {
        testDecodeBooleanEncodedBytesAsPrimtives(false);
    }

    @Test
    public void testDecodeBooleanEncodedBytesAsPrimtivesFS() throws Exception {
        testDecodeBooleanEncodedBytesAsPrimtives(true);
    }

    private void testDecodeBooleanEncodedBytesAsPrimtives(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.BOOLEAN_TRUE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(0);
        buffer.writeByte(EncodingCodes.BOOLEAN_FALSE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(1);

        if (fromStream) {
            boolean result1 = streamDecoder.readBoolean(stream, streamDecoderState, false);
            boolean result2 = streamDecoder.readBoolean(stream, streamDecoderState, true);
            boolean result3 = streamDecoder.readBoolean(stream, streamDecoderState, true);
            boolean result4 = streamDecoder.readBoolean(stream, streamDecoderState, false);

            assertTrue(result1);
            assertFalse(result2);
            assertFalse(result3);
            assertTrue(result4);
        } else {
            boolean result1 = decoder.readBoolean(buffer, decoderState, false);
            boolean result2 = decoder.readBoolean(buffer, decoderState, true);
            boolean result3 = decoder.readBoolean(buffer, decoderState, true);
            boolean result4 = decoder.readBoolean(buffer, decoderState, false);

            assertTrue(result1);
            assertFalse(result2);
            assertFalse(result3);
            assertTrue(result4);
        }
    }

    @Test
    public void testDecodeBooleanTrue() throws Exception {
        testDecodeBooleanTrue(false);
    }

    @Test
    public void testDecodeBooleanTrueFS() throws Exception {
        testDecodeBooleanTrue(true);
    }

    private void testDecodeBooleanTrue(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeBoolean(buffer, encoderState, true);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }
        assertTrue(result instanceof Boolean);
        assertTrue(((Boolean) result).booleanValue());

        encoder.writeBoolean(buffer, encoderState, true);

        final Boolean booleanResult;
        if (fromStream) {
            booleanResult = streamDecoder.readBoolean(stream, streamDecoderState);
        } else {
            booleanResult = decoder.readBoolean(buffer, decoderState);
        }
        assertTrue(booleanResult.booleanValue());
        assertEquals(Boolean.TRUE, booleanResult);
    }

    @Test
    public void testDecodeBooleanFalse() throws Exception {
        testDecodeBooleanFalse(false);
    }

    @Test
    public void testDecodeBooleanFalseFS() throws Exception {
        testDecodeBooleanFalse(true);
    }

    private void testDecodeBooleanFalse(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeBoolean(buffer, encoderState, false);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof Boolean);
        assertFalse(((Boolean) result).booleanValue());
    }

    @Test
    public void testDecodeBooleanFromNullEncoding() throws Exception {
        testDecodeBooleanFromNullEncoding(false);
    }

    @Test
    public void testDecodeBooleanFromNullEncodingFS() throws Exception {
        testDecodeBooleanFromNullEncoding(true);
    }

    private void testDecodeBooleanFromNullEncoding(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeBoolean(buffer, encoderState, true);
        encoder.writeNull(buffer, encoderState);

        final boolean result;
        if (fromStream) {
            result = streamDecoder.readBoolean(stream, streamDecoderState);
        } else {
            result = decoder.readBoolean(buffer, decoderState);
        }
        assertTrue(result);
        assertNull(decoder.readBoolean(buffer, decoderState));
    }

    @Test
    public void testDecodeBooleanAsPrimitiveWithDefault() throws Exception {
        testDecodeBooleanAsPrimitiveWithDefault(false);
    }

    @Test
    public void testDecodeBooleanAsPrimitiveWithDefaultFS() throws Exception {
        testDecodeBooleanAsPrimitiveWithDefault(true);
    }

    private void testDecodeBooleanAsPrimitiveWithDefault(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeBoolean(buffer, encoderState, true);
        encoder.writeNull(buffer, encoderState);

        if (fromStream) {
            boolean result = streamDecoder.readBoolean(stream, streamDecoderState, false);
            assertTrue(result);
            result = streamDecoder.readBoolean(stream, streamDecoderState, false);
            assertFalse(result);
        } else {
            boolean result = decoder.readBoolean(buffer, decoderState, false);
            assertTrue(result);
            result = decoder.readBoolean(buffer, decoderState, false);
            assertFalse(result);
        }
    }

    @Test
    public void testDecodeBooleanFailsForNonBooleanType() throws Exception {
        testDecodeBooleanFailsForNonBooleanType(false);
    }

    @Test
    public void testDecodeBooleanFailsForNonBooleanTypeFS() throws Exception {
        testDecodeBooleanFailsForNonBooleanType(true);
    }

    private void testDecodeBooleanFailsForNonBooleanType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeLong(buffer, encoderState, 1l);

        if (fromStream) {
            try {
                streamDecoder.readBoolean(stream, streamDecoderState);
                fail("Should not read long as boolean value.");
            } catch (DecodeException ioex) {}
        } else {
            try {
                decoder.readBoolean(buffer, decoderState);
                fail("Should not read long as boolean value.");
            } catch (DecodeException ioex) {}
        }
    }

    @Test
    public void testDecodeSmallSeriesOfBooleans() throws IOException {
        doTestDecodeBooleanSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfBooleans() throws IOException {
        doTestDecodeBooleanSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfBooleansFS() throws IOException {
        doTestDecodeBooleanSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfBooleansFS() throws IOException {
        doTestDecodeBooleanSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeBooleanSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < size; ++i) {
            encoder.writeBoolean(buffer, encoderState, i % 2 == 0);
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof Boolean);

            Boolean boolValue = (Boolean) result;
            assertEquals(i % 2 == 0, boolValue.booleanValue());
        }
    }

    @Test
    public void testArrayOfBooleanObjects() throws IOException {
        testArrayOfBooleanObjects(false);
    }

    @Test
    public void testArrayOfBooleanObjectsFS() throws IOException {
        testArrayOfBooleanObjects(true);
    }

    private void testArrayOfBooleanObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int size = 10;

        Boolean[] source = new Boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
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

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfBooleanObjects() throws IOException {
        testZeroSizedArrayOfBooleanObjects(false);
    }

    @Test
    public void testZeroSizedArrayOfBooleanObjectsFS() throws IOException {
        testZeroSizedArrayOfBooleanObjects(true);
    }

    private void testZeroSizedArrayOfBooleanObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Boolean[] source = new Boolean[0];

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

        boolean[] array = (boolean[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeSmallBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(SMALL_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeLargeBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(LARGE_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeSmallBooleanArrayFS() throws IOException {
        doTestDecodeBooleanArrayType(SMALL_ARRAY_SIZE, true);
    }

    @Test
    public void testDecodeLargeBooleanArrayFS() throws IOException {
        doTestDecodeBooleanArrayType(LARGE_ARRAY_SIZE, true);
    }

    private void doTestDecodeBooleanArrayType(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
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

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testArrayOfPrimitiveBooleanObjects() throws IOException {
        testArrayOfPrimitiveBooleanObjects(false);
    }

    @Test
    public void testArrayOfPrimitiveBooleanObjectsFS() throws IOException {
        testArrayOfPrimitiveBooleanObjects(true);
    }

    private void testArrayOfPrimitiveBooleanObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int size = 10;

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
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

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfPrimitiveBooleanObjects() throws IOException {
        testZeroSizedArrayOfPrimitiveBooleanObjects(false);
    }

    @Test
    public void testZeroSizedArrayOfPrimitiveBooleanObjectsFS() throws IOException {
        testZeroSizedArrayOfPrimitiveBooleanObjects(true);
    }

    private void testZeroSizedArrayOfPrimitiveBooleanObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        boolean[] source = new boolean[0];

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

        boolean[] array = (boolean[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testArrayOfArraysOfPrimitiveBooleanObjects() throws IOException {
        testArrayOfArraysOfPrimitiveBooleanObjects(false);
    }

    @Test
    public void testArrayOfArraysOfPrimitiveBooleanObjectsFS() throws IOException {
        testArrayOfArraysOfPrimitiveBooleanObjects(true);
    }

    private void testArrayOfArraysOfPrimitiveBooleanObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int size = 10;

        boolean[][] source = new boolean[2][size];
        for (int i = 0; i < size; ++i) {
            source[0][i] = i % 2 == 0;
            source[1][i] = i % 2 == 0;
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

        Object[] resultArray = (Object[]) result;

        assertNotNull(resultArray);
        assertEquals(2, resultArray.length);

        assertTrue(resultArray[0].getClass().isArray());
        assertTrue(resultArray[1].getClass().isArray());

        for (int i = 0; i < resultArray.length; ++i) {
            boolean[] nested = (boolean[]) resultArray[i];
            assertEquals(source[i].length, nested.length);
            assertArrayEquals(source[i], nested);
        }
    }

    @Test
    public void testReadAllBooleanTypeEncodings() throws IOException {
        testReadAllBooleanTypeEncodings(false);
    }

    @Test
    public void testReadAllBooleanTypeEncodingsFS() throws IOException {
        testReadAllBooleanTypeEncodings(true);
    }

    private void testReadAllBooleanTypeEncodings(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.BOOLEAN_TRUE);
        buffer.writeByte(EncodingCodes.BOOLEAN_FALSE);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(1);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        buffer.writeByte(0);

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertTrue((Boolean) typeDecoder.readValue(stream, streamDecoderState));
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertFalse((Boolean) typeDecoder.readValue(stream, streamDecoderState));
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertTrue((Boolean) typeDecoder.readValue(stream, streamDecoderState));
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertFalse((Boolean) typeDecoder.readValue(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertTrue((Boolean) typeDecoder.readValue(buffer, decoderState));
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertFalse((Boolean) typeDecoder.readValue(buffer, decoderState));
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertTrue((Boolean) typeDecoder.readValue(buffer, decoderState));
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Boolean.class, typeDecoder.getTypeClass());
            assertFalse((Boolean) typeDecoder.readValue(buffer, decoderState));
        }
    }

    @Test
    public void testSkipValueFullBooleanTypeEncodings() throws IOException {
        testSkipValueFullBooleanTypeEncodings(false);
    }

    @Test
    public void testSkipValueFullBooleanTypeEncodingsFS() throws IOException {
        testSkipValueFullBooleanTypeEncodings(true);
    }

    private void testSkipValueFullBooleanTypeEncodings(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < 10; ++i) {
            buffer.writeByte(EncodingCodes.BOOLEAN);
            buffer.writeByte(1);
            buffer.writeByte(EncodingCodes.BOOLEAN);
            buffer.writeByte(0);
        }

        encoder.writeObject(buffer, encoderState, false);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN & 0xFF);
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN & 0xFF);
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
        assertTrue(result instanceof Boolean);

        Boolean value = (Boolean) result;
        assertEquals(false, value);
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
            encoder.writeBoolean(buffer, encoderState, Boolean.TRUE);
            encoder.writeBoolean(buffer, encoderState, false);
        }

        encoder.writeObject(buffer, encoderState, false);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN_TRUE & 0xFF);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN_FALSE & 0xFF);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN_TRUE & 0xFF);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.BOOLEAN_FALSE & 0xFF);
                assertEquals(Boolean.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Boolean);

        Boolean value = (Boolean) result;
        assertEquals(false, value);
    }
}
