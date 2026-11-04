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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeEOFException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnknownDescribedType;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.Test;

public class ProtonDecoderTest extends CodecTestSupport {

    @Test
    public void testGetCachedDecoderStateReturnsCachedState() {
        DecoderState first = decoder.getCachedDecoderState();

        assertSame(first, decoder.getCachedDecoderState());
    }

    @Test
    public void testDescriptedTypeDecoderRegistrationChecksForNullDescriptors() {
        assertThrows(IllegalArgumentException.class,
            () -> decoder.registerDescribedTypeDecoder(new TestDescribedTypeDecoder(null, null)));
        assertThrows(IllegalArgumentException.class,
            () -> decoder.registerDescribedTypeDecoder(new TestDescribedTypeDecoder(null, new UnsignedLong(0))));
        assertThrows(IllegalArgumentException.class,
            () -> decoder.registerDescribedTypeDecoder(new TestDescribedTypeDecoder(Symbol.getSymbol("test"), null)));
    }

    @Test
    public void testReadNullFromReadObjectForNullEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readObject(buffer, decoderState));
        assertNull(decoder.readObject(buffer, decoderState, UUID.class));
    }

    @Test
    public void testTryReadFromEmptyBuffer() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should fail on read of object from empty buffer");
        } catch (DecodeEOFException dex) {}
    }

    @Test
    public void testErrorOnReadOfUnknownEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 255);

        assertNull(decoder.peekNextTypeDecoder(buffer, decoderState));

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should throw if no type decoder exists for given type");
        } catch (DecodeException ioe) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertThrows(DecodeException.class, () -> decoder.readObject(buffer, decoderState, String.class));
    }

    @Test
    public void testReadMultipleFromNullEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readMultiple(buffer, decoderState, UUID.class));
    }

    @Test
    public void testReadMultipleFromSingleEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertThrows(DecodeException.class, () -> decoder.readMultiple(buffer, decoderState, String.class));
    }

    @Test
    public void testReadMultipleRequestsWrongTypeForArrayEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID[] value = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };

        encoder.writeArray(buffer, encoderState, value);

        assertThrows(DecodeException.class, () -> decoder.readMultiple(buffer, decoderState, String.class));
    }

    @Test
    public void testDecodeUnknownDescribedTypeWithNegativeLongDescriptor() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte((byte) 255);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof UnknownDescribedType);

        UnknownDescribedType type = (UnknownDescribedType) result;
        assertTrue(type.getDescribed() instanceof UUID);
        assertEquals(value, type.getDescribed());
        assertNotNull(type.toString());
    }

    @Test
    public void testReadUnsignedIntegerTypes() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UINT0);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte((byte) 127);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) 255);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(0, decoder.readUnsignedInteger(buffer, decoderState, 32));
        assertEquals(127, decoder.readUnsignedInteger(buffer, decoderState, 32));
        assertEquals(255, decoder.readUnsignedInteger(buffer, decoderState, 32));
        assertEquals(32, decoder.readUnsignedInteger(buffer, decoderState, 32));
    }

    @Test
    public void testReadUnsignedIntegerTypesAsObject() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UINT0);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte((byte) 127);
        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) 255);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(UnsignedInteger.valueOf(0), decoder.readUnsignedInteger(buffer, decoderState, UnsignedInteger.valueOf(32)));
        assertEquals(UnsignedInteger.valueOf(127), decoder.readUnsignedInteger(buffer, decoderState, UnsignedInteger.valueOf(32)));
        assertEquals(UnsignedInteger.valueOf(255), decoder.readUnsignedInteger(buffer, decoderState, UnsignedInteger.valueOf(32)));
        assertEquals(UnsignedInteger.valueOf(32), decoder.readUnsignedInteger(buffer, decoderState, UnsignedInteger.valueOf(32)));
    }

    @Test
    public void testReadStringWithCustomStringDecoder() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

    @Test
    public void testDecodeUnknownDescribedTypeFailsWhenInSASLMode() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        ProtonDecoder decoder = ProtonDecoderFactory.createSasl();
        final ProtonDecoderState state = decoder.newDecoderState();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte((byte) 255);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertThrows(DecodeException.class, () -> decoder.readObject(buffer, state));
    }

    @Test
    public void testDecodeUnknownDescribedTypeWithRestrictedDescriptorFailsWhenInSASLMode() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        ProtonDecoder decoder = ProtonDecoderFactory.createSasl();
        final ProtonDecoderState state = decoder.newDecoderState();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLUINT);
        buffer.writeByte((byte) 255);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertThrows(DecodeException.class, () -> decoder.readObject(buffer, state));
    }

    @Test
    public void testLargeSymbolDescriptorsAreNotPutInUnknownTypeCache() throws IOException {
        final ProtonDecoder decoder = ProtonDecoderFactory.create();
        final ProtonDecoderState state = decoder.newDecoderState();
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final int descriptorLength = ProtonDecoder.UNKNOWN_DESCRIBED_TYPE_DESCRIPTOR_SIZE_LIMIT + 1;
        final UUID value = UUID.randomUUID();

        for (int i = 0; i < ProtonDecoder.UNKNOWN_DESCRIBED_TYPES_CACHE_LIMIT; ++i) {
            buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
            buffer.writeByte(EncodingCodes.SYM8);
            buffer.writeByte((byte) descriptorLength);
            for (int j = 0; j < descriptorLength; ++j) {
                buffer.writeByte((byte) random.nextInt(127));
            }
            buffer.writeByte(EncodingCodes.UUID);
            buffer.writeLong(value.getMostSignificantBits());
            buffer.writeLong(value.getLeastSignificantBits());
        }

        Set<TypeDecoder<?>> typeDecoders = new HashSet<>();

        for (int i = 0; i < ProtonDecoder.UNKNOWN_DESCRIBED_TYPES_CACHE_LIMIT; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, state);
            assertTrue(typeDecoder instanceof UnknownDescribedTypeDecoder);
            assertTrue(typeDecoders.add(typeDecoder));
            UnknownDescribedType result = (UnknownDescribedType) typeDecoder.readValue(buffer, state);
            assertTrue(result.getDescriptor() instanceof Symbol);
            assertTrue(result.getDescribed() instanceof UUID);
        }

        assertEquals(ProtonDecoder.UNKNOWN_DESCRIBED_TYPES_CACHE_LIMIT, typeDecoders.size());
    }

    @Test
    public void testReadObjectArray8FailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter() throws IOException {
        testReadObjectFailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter(EncodingCodes.ARRAY8);
    }

    @Test
    public void testReadObjectArray32FailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter() throws IOException {
        testReadObjectFailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter(EncodingCodes.ARRAY32);
    }

    private void testReadObjectFailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter(byte arrayType) throws IOException {
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        if (EncodingCodes.ARRAY32 == arrayType) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(8); // Size
            buffer.writeInt(3);  // Count
        } else {
            buffer.writeByte(EncodingCodes.ARRAY8);
            buffer.writeByte((byte) 5);
            buffer.writeByte((byte) 3);
        }

        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 2);
        buffer.writeByte((byte) 3);

        assertThrows(DecodeException.class, () -> decoder.readObject(buffer, decoderState, Symbol.class));

        assertTrue(buffer.isReadable()); // Should not have read array contents
    }

    @Test
    public void testReadMultipleArray8FailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter() throws IOException {
        testReadMultipleFailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter(EncodingCodes.ARRAY8);
    }

    @Test
    public void testReadMultipleArray32FailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter() throws IOException {
        testReadMultipleFailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter(EncodingCodes.ARRAY32);
    }

    private void testReadMultipleFailsBeforeDecodingContentsIfEncodingDoesNotMatchFilter(byte arrayType) throws IOException {
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        if (EncodingCodes.ARRAY32 == arrayType) {
            buffer.writeByte(EncodingCodes.ARRAY32);
            buffer.writeInt(8); // Size
            buffer.writeInt(3);  // Count
        } else {
            buffer.writeByte(EncodingCodes.ARRAY8);
            buffer.writeByte((byte) 5);
            buffer.writeByte((byte) 3);
        }

        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 2);
        buffer.writeByte((byte) 3);

        assertThrows(DecodeException.class, () -> decoder.readMultiple(buffer, decoderState, Symbol.class));

        assertTrue(buffer.isReadable()); // Should not have read array contents
    }

    @Test
    public void testReadObjectForObjectDoesNotDecodeIfFilterDoesNotMatch() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertThrows(DecodeException.class, () -> decoder.readObject(buffer, decoderState, String.class));

        assertTrue(buffer.isReadable()); // Should not have read UUID contents
    }

    @Test
    public void testReadMultipeForObjectDoesNotDecodeIfFilterDoesNotMatch() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        assertThrows(DecodeException.class, () -> decoder.readMultiple(buffer, decoderState, String.class));

        assertTrue(buffer.isReadable()); // Should not have read UUID contents
    }
}
