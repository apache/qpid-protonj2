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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeEOFException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.types.UnknownDescribedType;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ProtonStreamDecoderTest extends CodecTestSupport {

    @Test
    public void testGetCachedDecoderStateReturnsCachedState() {
        StreamDecoderState first = streamDecoder.getCachedDecoderState();

        assertSame(first, streamDecoder.getCachedDecoderState());
    }

    @Test
    public void testReadNullFromReadObjectForNullEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertNull(streamDecoder.readObject(stream, streamDecoderState));
        assertNull(streamDecoder.readObject(stream, streamDecoderState, UUID.class));
    }

    @Test
    public void testTryReadFromEmptyStream() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        try {
            streamDecoder.readObject(stream, streamDecoderState);
            fail("Should fail on read of object from empty stream");
        } catch (DecodeEOFException dex) {}
    }

    @Test
    public void testErrorOnReadOfUnknownEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 255);

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertNull(streamDecoder.peekNextTypeDecoder(stream, streamDecoderState));

        try {
            streamDecoder.readObject(stream, streamDecoderState);
            fail("Should throw if no type streamDecoder exists for given type");
        } catch (DecodeException ioe) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        InputStream stream = new ProtonBufferInputStream(buffer);

        try {
            streamDecoder.readObject(stream, streamDecoderState, String.class);
            fail("Should not allow for conversion to String type");
        } catch (ClassCastException cce) {
        }
    }

    @Test
    public void testReadMultipleFromNullEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        buffer.writeByte(EncodingCodes.NULL);

        InputStream stream = new ProtonBufferInputStream(buffer);
        assertNull(streamDecoder.readMultiple(stream, streamDecoderState, UUID.class));
    }

    @Test
    public void testReadMultipleFromSingleEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        InputStream stream = new ProtonBufferInputStream(buffer);

        UUID[] result = streamDecoder.readMultiple(stream, streamDecoderState, UUID.class);

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

        InputStream stream = new ProtonBufferInputStream(buffer);

        try {
            streamDecoder.readMultiple(stream, streamDecoderState, String.class);
            fail("Should not be able to convert to wrong resulting array type");
        } catch (ClassCastException cce) {}
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

        InputStream stream = new ProtonBufferInputStream(buffer);

        final Object result = streamDecoder.readObject(stream, streamDecoderState);

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

        InputStream stream = new ProtonBufferInputStream(buffer);

        final Object result = streamDecoder.readObject(stream, streamDecoderState);

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

        InputStream stream = new ProtonBufferInputStream(buffer);

        final Object result = streamDecoder.readObject(stream, streamDecoderState);

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

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertEquals(0, streamDecoder.readUnsignedInteger(stream, streamDecoderState, 32));
        assertEquals(127, streamDecoder.readUnsignedInteger(stream, streamDecoderState, 32));
        assertEquals(255, streamDecoder.readUnsignedInteger(stream, streamDecoderState, 32));
        assertEquals(32, streamDecoder.readUnsignedInteger(stream, streamDecoderState, 32));
    }

    @Test
    public void testReadMultipleRequestsWrongTypeForArrayEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final UUID[] value = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };

        encoder.writeArray(buffer, encoderState, value);

        InputStream stream = new ProtonBufferInputStream(buffer);

        try {
            streamDecoder.readMultiple(stream, streamDecoderState, String.class);
            fail("Should not be able to convert to wrong resulting array type");
        } catch (ClassCastException cce) {}
    }

    @Test
    public void testReadStringWithCustomStringDecoder() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(16);
        buffer.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });

        ((ProtonStreamDecoderState) streamDecoderState).setStringDecoder(new UTF8StreamDecoder() {

            @Override
            public String decodeUTF8(InputStream stream) {
                return "string-decoder";
            }
        });

        assertNotNull(((ProtonStreamDecoderState) streamDecoderState).getStringDecoder());

        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);

        String result = streamDecoder.readString(stream, streamDecoderState);

        assertEquals("string-decoder", result);
    }

    @Test
    public void testStringReadFromCustomDecoderThrowsDecodeExceptionOnError() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(16);
        buffer.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });

        ((ProtonStreamDecoderState) streamDecoderState).setStringDecoder(new UTF8StreamDecoder() {

            @Override
            public String decodeUTF8(InputStream stream) {
                throw new IndexOutOfBoundsException();
            }
        });

        InputStream stream = new ProtonBufferInputStream(buffer);

        assertNotNull(((ProtonStreamDecoderState) streamDecoderState).getStringDecoder());
        assertThrows(DecodeException.class, () -> streamDecoder.readString(stream, streamDecoderState));
    }

    @Test
    public void testCannotPeekFromStreamThatCannotMark() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));

        Mockito.when(stream.markSupported()).thenReturn(false);

        try {
            streamDecoder.peekNextTypeDecoder(stream, streamDecoderState);
            fail("Should fail on read of object from empty stream");
        } catch (UnsupportedOperationException uopex) {}
    }

    @Test
    public void testDecodeErrorFromPeekWhenStreamResetFails() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, "test");

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));
        Mockito.doThrow(new IOException()).when(stream).reset();

        try {
            streamDecoder.peekNextTypeDecoder(stream, streamDecoderState);
            fail("Should fail on read of object from empty stream");
        } catch (DecodeException dex) {}
    }

    @Test
    public void testStreamDecoderCanStillReadWhenMarkNotSupported() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, "test");
        encoder.writeObject(buffer, encoderState, Accepted.getInstance());

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));
        Mockito.when(stream.markSupported()).thenReturn(false);

        final Object string = streamDecoder.readObject(stream, streamDecoderState);
        final Object accepted = streamDecoder.readObject(stream, streamDecoderState);

        assertEquals("test", string);
        assertSame(Accepted.getInstance(), accepted);
    }

    @Test
    public void testReadDescriptorOfTypeSymbol32MarkNotSupported() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SYM32);
        buffer.writeInt(Accepted.DESCRIPTOR_SYMBOL.getLength());
        Accepted.DESCRIPTOR_SYMBOL.writeTo(buffer);
        buffer.writeByte(EncodingCodes.LIST0);

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));

        Mockito.when(stream.markSupported()).thenReturn(false);

        final Object accepted = streamDecoder.readObject(stream, streamDecoderState);

        assertSame(Accepted.getInstance(), accepted);
    }

    @Test
    public void testReadDescriptorOfTypeSymbol8MarkNotSupported() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SYM8);
        buffer.writeByte((byte) Accepted.DESCRIPTOR_SYMBOL.getLength());
        Accepted.DESCRIPTOR_SYMBOL.writeTo(buffer);
        buffer.writeByte(EncodingCodes.LIST0);

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));
        Mockito.when(stream.markSupported()).thenReturn(false);

        final Object accepted = streamDecoder.readObject(stream, streamDecoderState);

        assertSame(Accepted.getInstance(), accepted);
    }

    @Test
    public void testReadDescriptorOfTypeUnsignedLongMarkNotSupported() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.ULONG);
        buffer.writeLong(Accepted.DESCRIPTOR_CODE.longValue());
        buffer.writeByte(EncodingCodes.LIST0);

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));

        Mockito.when(stream.markSupported()).thenReturn(false);

        final Object accepted = streamDecoder.readObject(stream, streamDecoderState);

        assertSame(Accepted.getInstance(), accepted);
    }

    @Test
    public void testReadDescriptorOfTypeSmallUnsignedLongMarkNotSupported() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST0);

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));

        Mockito.when(stream.markSupported()).thenReturn(false);

        final Object accepted = streamDecoder.readObject(stream, streamDecoderState);

        assertSame(Accepted.getInstance(), accepted);
    }

    @Test
    public void testDecodeErrorWhenMarkNotSupportedAndUnknownEncodingCodeFoundInDescribedType() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(Accepted.DESCRIPTOR_CODE.longValue());
        buffer.writeByte(EncodingCodes.LIST0);

        InputStream stream = Mockito.spy(new ProtonBufferInputStream(buffer));
        Mockito.when(stream.markSupported()).thenReturn(false);

        try {
            streamDecoder.readObject(stream, streamDecoderState);
            fail("Should fail on read of object with bad descriptor type");
        } catch (DecodeException dex) {}
    }
}
