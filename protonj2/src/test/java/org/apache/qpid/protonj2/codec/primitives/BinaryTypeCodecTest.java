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

import java.io.EOFException;
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
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.BinaryTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test the Binary codec for correctness
 */
public class BinaryTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(false);
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisTypeFromStream() throws Exception {
        testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(true);
    }

    private void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        if (fromStream) {
            try {
                streamDecoder.readBinary(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readBinaryAsBuffer(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readBinary(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readBinaryAsBuffer(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        buffer.writeByte(EncodingCodes.NULL);
        assertNull(decoder.readBinary(buffer, decoderState));
    }

    @Test
    public void testReadFromNullEncodingCodeFromStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        buffer.writeByte(EncodingCodes.NULL);
        assertNull(streamDecoder.readBinary(stream, streamDecoderState));
    }

    @Test
    public void testEncodeDecodeEmptyArrayBinary() throws Exception {
        testEncodeDecodeEmptyArrayBinary(false);
    }

    @Test
    public void testEncodeDecodeEmptyArrayBinaryFromStream() throws Exception {
        testEncodeDecodeEmptyArrayBinary(true);
    }

    private void testEncodeDecodeEmptyArrayBinary(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        Binary input = new Binary(new byte[0]);

        encoder.writeBinary(buffer, encoderState, input);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(0, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
    }

    @Test
    public void testEncodeDecodeBinary() throws Exception {
        testEncodeDecodeBinary(false);
    }

    @Test
    public void testEncodeDecodeBinaryFromStream() throws Exception {
        testEncodeDecodeBinary(true);
    }

    private void testEncodeDecodeBinary(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        Binary input = new Binary(new byte[] {0, 1, 2, 3, 4});

        encoder.writeBinary(buffer, encoderState, input);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(5, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
        assertEquals(input, output);
        assertArrayEquals(input.getArray(), output.getArray());
    }

    @Test
    public void testEncodeDecodeBinaryUsingRawBytesWithSmallArray() throws Exception {
        testEncodeDecodeBinaryUsingRawBytesWithSmallArray(false);
    }

    @Test
    public void testEncodeDecodeBinaryUsingRawBytesWithSmallArrayFromStream() throws Exception {
        testEncodeDecodeBinaryUsingRawBytesWithSmallArray(true);
    }

    private void testEncodeDecodeBinaryUsingRawBytesWithSmallArray(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        byte[] input = new byte[16];
        filler.nextBytes(input);

        encoder.writeBinary(buffer, encoderState, input);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(input.length, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
        assertArrayEquals(input, output.getArray());
    }

    @Test
    public void testEncodeDecodeBinaryUsingRawBytesWithLargeArray() throws Exception {
        testEncodeDecodeBinaryUsingRawBytesWithLargeArray(false);
    }

    @Test
    public void testEncodeDecodeBinaryUsingRawBytesWithLargeArrayFromStream() throws Exception {
        testEncodeDecodeBinaryUsingRawBytesWithLargeArray(true);
    }

    private void testEncodeDecodeBinaryUsingRawBytesWithLargeArray(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        byte[] input = new byte[512];
        filler.nextBytes(input);

        encoder.writeBinary(buffer, encoderState, input);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(input.length, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
        assertArrayEquals(input, output.getArray());
    }

    @Test
    public void testDecodeFailsEarlyOnInvliadBinaryLengthVBin8() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(255);

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not be able to read binary with length greater than readable bytes");
        } catch (IllegalArgumentException iae) {}

        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testDecodeFailsEarlyOnInvliadBinaryLengthVBin32() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(Integer.MAX_VALUE);

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not be able to read binary with length greater than readable bytes");
        } catch (IllegalArgumentException iae) {}

        assertEquals(5, buffer.getReadIndex());
    }

    @Test
    public void testDecodeAsBufferFailsEarlyOnInvliadBinaryLengthVBin32() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(Integer.MAX_VALUE);

        try {
            decoder.readBinaryAsBuffer(buffer, decoderState);
            fail("Should not be able to read binary with length greater than readable bytes");
        } catch (IllegalArgumentException iae) {}

        assertEquals(5, buffer.getReadIndex());
    }

    @Test
    public void testDecodeOfBinaryTagFailsEarlyOnInvliadBinaryLengthVBin32() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(Integer.MAX_VALUE);
        buffer.writeInt(Integer.MAX_VALUE);

        try {
            decoder.readDeliveryTag(buffer, decoderState);
            fail("Should not be able to read binary with length greater than readable bytes");
        } catch (DecodeException dex) {}
    }

    @Test
    public void testSkipFailsEarlyOnInvliadBinaryLengthVBin8() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(255);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
        assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.VBIN8 & 0xFF);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip binary with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testSkipFailsEarlyOnInvliadBinaryLengthVBin8FromStream() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(255);

        StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
        assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
        assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.VBIN8 & 0xFF);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(stream, streamDecoderState);
        } catch (IllegalArgumentException ex) {
            fail("Should be able to skip binary with length greater than readable bytes");
        }

        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testSkipFailsEarlyOnInvliadBinaryLengthVBin32() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(Integer.MAX_VALUE);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
        assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.VBIN32 & 0xFF);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip binary with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(5, buffer.getReadIndex());
    }

    @Test
    public void testSkipFailsEarlyOnInvliadBinaryLengthVBin32FromStream() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(Integer.MAX_VALUE);

        StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
        assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
        assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.VBIN32 & 0xFF);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(stream, streamDecoderState);
        } catch (IllegalArgumentException ex) {
            fail("Should be able to skip binary with length greater than readable bytes");
        }

        assertEquals(5, buffer.getReadIndex());
    }

    @Test
    public void testReadEncodedSizeFromVBin8Encoding() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(255);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(Binary.class, typeDecoder.getTypeClass());
        BinaryTypeDecoder binaryDecoder = (BinaryTypeDecoder) typeDecoder;
        assertEquals(255, binaryDecoder.readSize(buffer));

        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testReadEncodedSizeFromVBin8EncodingUsingStream() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(255);

        StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
        assertEquals(Binary.class, typeDecoder.getTypeClass());
        BinaryTypeDecoder binaryDecoder = (BinaryTypeDecoder) typeDecoder;
        assertEquals(255, binaryDecoder.readSize(stream));

        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testZeroSizedArrayOfBinaryObjects() throws IOException {
        testZeroSizedArrayOfBinaryObjects(false);
    }

    @Test
    public void testZeroSizedArrayOfBinaryObjectsFromStream() throws IOException {
        testZeroSizedArrayOfBinaryObjects(true);
    }

    private void testZeroSizedArrayOfBinaryObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Binary[] source = new Binary[0];

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

        Binary[] array = (Binary[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testArrayOfBinaryObjects() throws IOException {
        testArrayOfBinaryObjects(false);
    }

    @Test
    public void testArrayOfBinaryObjectsFromStream() throws IOException {
        testArrayOfBinaryObjects(true);
    }

    private void testArrayOfBinaryObjects(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        Binary[] source = new Binary[5];
        for (int i = 0; i < source.length; ++i) {
            byte[] data = new byte[16 * i];
            filler.nextBytes(data);

            source[i] = new Binary(data);
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

        Binary[] array = (Binary[]) result;
        assertEquals(source.length, array.length);

        for (int i = 0; i < source.length; ++i) {
            Binary decoded = ((Binary[]) result)[i];
            assertArrayEquals(source[i].getArray(), decoded.getArray());
        }
    }

    @Test
    public void testStreamSkipOfEncodingHandlesIOException() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        byte[] input = new byte[512];
        filler.nextBytes(input);

        encoder.writeBinary(buffer, encoderState, input);

        StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        stream = Mockito.spy(stream);

        Mockito.when(stream.skip(Mockito.anyLong())).thenThrow(EOFException.class);

        try {
            typeDecoder.skipValue(stream, streamDecoderState);
            fail("Expected an exception on skip of encoded list failure.");
        } catch (DecodeException dex) {}
    }
}
