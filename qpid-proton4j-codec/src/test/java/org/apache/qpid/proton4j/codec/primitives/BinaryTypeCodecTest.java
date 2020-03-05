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
package org.apache.qpid.proton4j.codec.primitives;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.junit.Test;

/**
 * Test the Binary codec for correctness
 */
public class BinaryTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readBinary(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}

        try {
            decoder.readBinaryAsBuffer(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readBinary(buffer, decoderState));
    }

    @Test
    public void testEncodeDecodeEmptyArrayBinary() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Binary input = new Binary(new byte[0]);

        encoder.writeBinary(buffer, encoderState, input);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(0, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
    }

    @Test
    public void testEncodeDecodeBinary() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Binary input = new Binary(new byte[] {0, 1, 2, 3, 4});

        encoder.writeBinary(buffer, encoderState, input);

        Object result = decoder.readObject(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        byte[] input = new byte[16];
        filler.nextBytes(input);

        encoder.writeBinary(buffer, encoderState, input);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(input.length, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
        assertArrayEquals(input, output.getArray());
    }

    @Test
    public void testEncodeDecodeBinaryUsingRawBytesWithLargeArray() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        byte[] input = new byte[512];
        filler.nextBytes(input);

        encoder.writeBinary(buffer, encoderState, input);

        Object result = decoder.readObject(buffer, decoderState);
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
    public void testSkipFailsEarlyOnInvliadBinaryLengthVBin8() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(255);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip binary with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testSkipFailsEarlyOnInvliadBinaryLengthVBin32() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(Integer.MAX_VALUE);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(Binary.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip binary with length greater than readable bytes");
        } catch (IllegalArgumentException ex) {}

        assertEquals(5, buffer.getReadIndex());
    }

    @Test
    public void testZeroSizedArrayOfBinaryObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Binary[] source = new Binary[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        Binary[] array = (Binary[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testArrayOfBinaryObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Random filler = new Random();
        filler.setSeed(System.nanoTime());

        Binary[] source = new Binary[5];
        for (int i = 0; i < source.length; ++i) {
            byte[] data = new byte[16 * i];
            filler.nextBytes(data);

            source[i] = new Binary(data);
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
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
}
