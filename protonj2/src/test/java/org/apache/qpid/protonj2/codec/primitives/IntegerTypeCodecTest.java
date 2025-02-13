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
import org.apache.qpid.protonj2.codec.decoders.primitives.Integer32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Integer8TypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.IntegerTypeEncoder;
import org.junit.jupiter.api.Test;

public class IntegerTypeCodecTest extends CodecTestSupport {

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
                streamDecoder.readInteger(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readInteger(stream, streamDecoderState, (short) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readInteger(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readInteger(buffer, decoderState, (short) 0);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(42);
        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(44);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) -2);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(42, decoder.readInteger(buffer, decoderState).intValue());
        assertEquals(44, decoder.readInteger(buffer, decoderState, 42));
        assertEquals(43, decoder.readInteger(buffer, decoderState, 42));
        assertEquals(-2, decoder.readInteger(buffer, decoderState, 42));
        assertNull(decoder.readInteger(buffer, decoderState));
        assertEquals(42, decoder.readInteger(buffer, decoderState, 42));
    }

    @Test
    public void testReadUByteFromEncodingCodeFromStream() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(42);
        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(44);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) 43);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) -43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertEquals(42, streamDecoder.readInteger(stream, streamDecoderState).intValue());
        assertEquals(44, streamDecoder.readInteger(stream, streamDecoderState, 42));
        assertEquals(43, streamDecoder.readInteger(stream, streamDecoderState, 42));
        assertEquals(-43, streamDecoder.readInteger(stream, streamDecoderState, 42));
        assertNull(streamDecoder.readInteger(stream, streamDecoderState));
        assertEquals(42, streamDecoder.readInteger(stream, streamDecoderState, 42));
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.INT, (byte) new Integer32TypeDecoder().getTypeCode());
        assertEquals(EncodingCodes.SMALLINT, (byte) new Integer8TypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Integer.class, new IntegerTypeEncoder().getTypeClass());
        assertEquals(Integer.class, new Integer8TypeDecoder().getTypeClass());
        assertEquals(Integer.class, new Integer32TypeDecoder().getTypeClass());
    }

    @Test
    public void testReadIntegerFromEncodingCodeInt() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(42);

        assertEquals(42, decoder.readInteger(buffer, decoderState).intValue());
    }

    @Test
    public void testReadIntegerFromEncodingCodeSmallInt() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) 42);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) -1);

        assertEquals(42, decoder.readInteger(buffer, decoderState).intValue());
        assertEquals(-1, decoder.readInteger(buffer, decoderState).intValue());
    }

    @Test
    public void testReadIntegerFromEncodingCodeIntFromStream() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(42);

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertEquals(42, streamDecoder.readInteger(stream, streamDecoderState).intValue());
    }

    @Test
    public void testReadIntegerFromEncodingCodeSmallIntFromStream() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) 42);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) -1);

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertEquals(42, streamDecoder.readInteger(stream, streamDecoderState).intValue());
        assertEquals(-1, streamDecoder.readInteger(stream, streamDecoderState).intValue());
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
            encoder.writeInteger(buffer, encoderState, Integer.MAX_VALUE);
            encoder.writeInteger(buffer, encoderState, 16);
        }

        int expected = 42;

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
                assertEquals(Integer.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
                typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Integer.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Integer.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
                typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Integer.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Integer);

        Integer value = (Integer) result;
        assertEquals(expected, value.intValue());
    }

    @Test
    public void testArrayOfObjects() throws IOException {
        doTestArrayOfObjects(false);
    }

    @Test
    public void testArrayOfObjectsFromStream() throws IOException {
        doTestArrayOfObjects(true);
    }

    protected void doTestArrayOfObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int size = 10;

        Integer[] source = new Integer[size];
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

        Integer[] source = new Integer[0];

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
        assertEquals(source.length, array.length);
    }

    @Test
    public void testReadIntegerArrayInt32() throws IOException {
        doTestReadIntegerArray(EncodingCodes.INT, false);
    }

    @Test
    public void testReadIntegerArrayInt32FromStream() throws IOException {
        doTestReadIntegerArray(EncodingCodes.INT, true);
    }

    @Test
    public void testReadIntegerArrayInt8() throws IOException {
        doTestReadIntegerArray(EncodingCodes.SMALLINT, false);
    }

    @Test
    public void testReadIntegerArrayInt8FromStream() throws IOException {
        doTestReadIntegerArray(EncodingCodes.SMALLINT, true);
    }

    public void doTestReadIntegerArray(byte encoding, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        if (encoding == EncodingCodes.INT) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(21);  // Size
            buffer.writeInt(3);   // Count
            buffer.writeByte(EncodingCodes.INT);
            buffer.writeInt(1);   // [0]
            buffer.writeInt(2);   // [1]
            buffer.writeInt(-1);  // [2]
        } else if (encoding == EncodingCodes.SMALLINT) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(12);  // Size
            buffer.writeInt(3);   // Count
            buffer.writeByte(EncodingCodes.SMALLINT);
            buffer.writeByte((byte) 1);   // [0]
            buffer.writeByte((byte) 2);   // [1]
            buffer.writeByte((byte) -1);  // [2]
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

        int[] array = (int[]) result;

        assertEquals(3, array.length);
        assertEquals(1, array[0]);
        assertEquals(2, array[1]);
        assertEquals(-1, array[2]);
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

        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte((byte) 0);
        buffer.writeByte(EncodingCodes.INT);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(1, typeDecoder.readSize(stream, streamDecoderState));
            typeDecoder.readValue(stream, streamDecoderState);
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(4, typeDecoder.readSize(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(1, typeDecoder.readSize(buffer, decoderState));
            typeDecoder.readValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(4, typeDecoder.readSize(buffer, decoderState));
        }
    }
}
