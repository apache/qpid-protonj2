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
package org.apache.qpid.protonj2.codec.decoders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeEOFException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.types.UnknownDescribedType;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.Test;

public class ProtonDecoderTest extends CodecTestSupport {

    @Test
    public void testReadNullFromReadObjectForNullEncodng() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readObject(buffer, decoderState));
        assertNull(decoder.readObject(buffer, decoderState, UUID.class));
    }

    @Test
    public void testTryReadFromEmptyBuffer() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should fail on read of object from empty buffer");
        } catch (DecodeEOFException dex) {}
    }

    @Test
    public void testErrorOnReadOfUnknownEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(255);

        assertNull(decoder.peekNextTypeDecoder(buffer, decoderState));

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should throw if no type decoder exists for given type");
        } catch (DecodeException ioe) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        try {
            decoder.readObject(buffer, decoderState, String.class);
            fail("Should not allow for conversion to String type");
        } catch (ClassCastException cce) {
        }
    }

    @Test
    public void testReadMultipleFromNullEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readMultiple(buffer, decoderState, UUID.class));
    }

    @Test
    public void testReadMultipleFromSingleEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        UUID[] result = decoder.readMultiple(buffer, decoderState, UUID.class);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals(value, result[0]);
    }

    @Test
    public void testReadMultipleRequestsWrongTypeForArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        try {
            decoder.readMultiple(buffer, decoderState, String.class);
            fail("Should not be able to convert to wrong resulting array type");
        } catch (ClassCastException cce) {}
    }

    @Test
    public void testReadMultipleRequestsWrongTypeForArrayEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID[] value = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };

        encoder.writeArray(buffer, encoderState, value);

        try {
            decoder.readMultiple(buffer, decoderState, String.class);
            fail("Should not be able to convert to wrong resulting array type");
        } catch (ClassCastException cce) {}
    }

    @Test
    public void testDecodeUnknownDescribedTypeWithNegativeLongDescriptor() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(UnsignedLong.MAX_VALUE.longValue());
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnknownDescribedType);

        UnknownDescribedType type = (UnknownDescribedType) result;
        assertTrue(type.getDescribed() instanceof UUID);
        assertEquals(value, type.getDescribed());
    }

    @Test
    public void testDecodeUnknownDescribedTypeWithMaxLongDescriptor() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(Long.MAX_VALUE);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnknownDescribedType);

        UnknownDescribedType type = (UnknownDescribedType) result;
        assertTrue(type.getDescribed() instanceof UUID);
        assertEquals(value, type.getDescribed());
    }

    @Test
    public void testDecodeUnknownDescribedTypeWithUnknownDescriptorCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(255);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnknownDescribedType);

        UnknownDescribedType type = (UnknownDescribedType) result;
        assertTrue(type.getDescribed() instanceof UUID);
        assertEquals(value, type.getDescribed());
    }

    @Test
    public void testReadUnsignedIntegerTypes() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT0);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte(127);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(0);
        buffer.writeByte(0);
        buffer.writeByte(0);
        buffer.writeByte(255);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(0, decoder.readUnsignedInteger(buffer, decoderState, 32));
        assertEquals(127, decoder.readUnsignedInteger(buffer, decoderState, 32));
        assertEquals(255, decoder.readUnsignedInteger(buffer, decoderState, 32));
        assertEquals(32, decoder.readUnsignedInteger(buffer, decoderState, 32));
    }

    @Test
    public void testReadStringWithCustomStringDecoder() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(16);
        buffer.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });

        ((ProtonDecoderState) decoderState).setStringDecoder(new UTF8Decoder() {

            @Override
            public String decodeUTF8(ProtonBuffer buffer, int utf8length) {
               return "string-decoder";
            }
        });

        assertNotNull(((ProtonDecoderState) decoderState).getStringDecoder());

        String result = decoder.readString(buffer, decoderState);

        assertEquals("string-decoder", result);
        assertFalse(buffer.isReadable());
    }

    @Test
    public void testStringReadFromCustomDecoderThrowsDecodeExceptionOnError() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(16);
        buffer.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });

        ((ProtonDecoderState) decoderState).setStringDecoder(new UTF8Decoder() {

            @Override
            public String decodeUTF8(ProtonBuffer buffer, int utf8length) {
                throw new IndexOutOfBoundsException();
            }
        });

        assertNotNull(((ProtonDecoderState) decoderState).getStringDecoder());
        assertThrows(DecodeException.class, () -> decoder.readString(buffer, decoderState));
    }
}
