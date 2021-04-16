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
import java.util.Arrays;
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
import org.apache.qpid.protonj2.types.Symbol;
import org.junit.jupiter.api.Test;

public class SymbolTypeCodecTest extends CodecTestSupport {

    private final String SMALL_SYMBOL_VALUIE = "Small String";
    private final String LARGE_SYMBOL_VALUIE = "Large String: " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog.";

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

        if (fromStream) {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeByte(EncodingCodes.UINT);

            try {
                streamDecoder.readSymbol(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                streamDecoder.readSymbol(stream, streamDecoderState, "");
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeByte(EncodingCodes.UINT);

            try {
                decoder.readSymbol(buffer, decoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}

            try {
                decoder.readSymbol(buffer, decoderState, "");
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        }
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        testReadFromNullEncodingCode(false);
    }

    @Test
    public void testReadFromNullEncodingCodeFS() throws IOException {
        testReadFromNullEncodingCode(true);
    }

    private void testReadFromNullEncodingCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertNull(streamDecoder.readSymbol(stream, streamDecoderState));
            assertEquals("", streamDecoder.readSymbol(stream, streamDecoderState, ""));
        } else {
            assertNull(decoder.readSymbol(buffer, decoderState));
            assertEquals("", decoder.readSymbol(buffer, decoderState, ""));
        }
    }

    @Test
    public void testEncodeSmallSymbol() throws IOException {
        doTestEncodeDecode(Symbol.valueOf(SMALL_SYMBOL_VALUIE), false);
    }

    @Test
    public void testEncodeLargeSymbol() throws IOException {
        doTestEncodeDecode(Symbol.valueOf(LARGE_SYMBOL_VALUIE), false);
    }

    @Test
    public void testEncodeEmptySymbol() throws IOException {
        doTestEncodeDecode(Symbol.valueOf(""), false);
    }

    @Test
    public void testEncodeNullSymbol() throws IOException {
        doTestEncodeDecode(null, false);
    }

    @Test
    public void testEncodeSmallSymbolFS() throws IOException {
        doTestEncodeDecode(Symbol.valueOf(SMALL_SYMBOL_VALUIE), true);
    }

    @Test
    public void testEncodeLargeSymbolFS() throws IOException {
        doTestEncodeDecode(Symbol.valueOf(LARGE_SYMBOL_VALUIE), true);
    }

    @Test
    public void testEncodeEmptySymbolFS() throws IOException {
        doTestEncodeDecode(Symbol.valueOf(""), true);
    }

    @Test
    public void testEncodeNullSymbolFS() throws IOException {
        doTestEncodeDecode(null, true);
    }

    private void doTestEncodeDecode(Symbol value, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeSymbol(buffer, encoderState, value);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readSymbol(stream, streamDecoderState);
        } else {
            result = decoder.readSymbol(buffer, decoderState);
        }

        if (value != null) {
            assertNotNull(result);
            assertTrue(result instanceof Symbol);
        } else {
            assertNull(result);
        }

        assertEquals(value, result);
    }

    @Test
    public void testDecodeSmallSeriesOfSymbols() throws IOException {
        doTestDecodeSymbolSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfSymbols() throws IOException {
        doTestDecodeSymbolSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfSymbolsFS() throws IOException {
        doTestDecodeSymbolSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfSymbolsFS() throws IOException {
        doTestDecodeSymbolSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeSymbolSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < size; ++i) {
            encoder.writeSymbol(buffer, encoderState, Symbol.valueOf(LARGE_SYMBOL_VALUIE));
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof Symbol);
            assertEquals(LARGE_SYMBOL_VALUIE, result.toString());
        }
    }

    @Test
    public void testDecodeSmallSymbolArray() throws IOException {
        doTestDecodeSymbolArrayType(SMALL_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeLargeSymbolArray() throws IOException {
        doTestDecodeSymbolArrayType(LARGE_ARRAY_SIZE, false);
    }

    @Test
    public void testDecodeSmallSymbolArrayFS() throws IOException {
        doTestDecodeSymbolArrayType(SMALL_ARRAY_SIZE, true);
    }

    @Test
    public void testDecodeLargeSymbolArrayFS() throws IOException {
        doTestDecodeSymbolArrayType(LARGE_ARRAY_SIZE, true);
    }

    private void doTestDecodeSymbolArrayType(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Symbol[] source = new Symbol[size];
        for (int i = 0; i < size; ++i) {
            source[i] = Symbol.valueOf("test->" + i);
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

        Symbol[] array = (Symbol[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testEmptyShortSymbolEncode() throws IOException {
        doTestEmptySymbolEncodeAsGivenType(EncodingCodes.SYM8, false);
    }

    @Test
    public void testEmptyLargeSymbolEncode() throws IOException {
        doTestEmptySymbolEncodeAsGivenType(EncodingCodes.SYM32, false);
    }

    @Test
    public void testEmptyShortSymbolEncodeFS() throws IOException {
        doTestEmptySymbolEncodeAsGivenType(EncodingCodes.SYM8, true);
    }

    @Test
    public void testEmptyLargeSymbolEncodeFS() throws IOException {
        doTestEmptySymbolEncodeAsGivenType(EncodingCodes.SYM32, true);
    }

    public void doTestEmptySymbolEncodeAsGivenType(byte encodingCode, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(encodingCode);
        buffer.writeInt(0);

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.peekNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Symbol.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), encodingCode & 0xFF);
        } else {
            TypeDecoder<?> typeDecoder = decoder.peekNextTypeDecoder(buffer, decoderState);
            assertEquals(Symbol.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), encodingCode & 0xFF);
        }

        final Object result;
        if (fromStream) {
            result = streamDecoder.readSymbol(stream, streamDecoderState);
        } else {
            result = decoder.readSymbol(buffer, decoderState);
        }

        assertNotNull(result);
        assertEquals("", result.toString());
    }

    @Test
    public void testEmptyShortSymbolEncodeAsString() throws IOException {
        doTestEmptySymbolEncodeAsGivenTypeReadAsString(EncodingCodes.SYM8, false);
    }

    @Test
    public void testEmptyLargeSymbolEncodeAsString() throws IOException {
        doTestEmptySymbolEncodeAsGivenTypeReadAsString(EncodingCodes.SYM32, false);
    }

    @Test
    public void testEmptyShortSymbolEncodeAsStringFS() throws IOException {
        doTestEmptySymbolEncodeAsGivenTypeReadAsString(EncodingCodes.SYM8, true);
    }

    @Test
    public void testEmptyLargeSymbolEncodeAsStringFS() throws IOException {
        doTestEmptySymbolEncodeAsGivenTypeReadAsString(EncodingCodes.SYM32, true);
    }

    public void doTestEmptySymbolEncodeAsGivenTypeReadAsString(byte encodingCode, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(encodingCode);
        buffer.writeInt(0);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readSymbol(stream, streamDecoderState, "");
        } else {
            result = decoder.readSymbol(buffer, decoderState, "");
        }

        assertNotNull(result);
        assertEquals("", result);
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedSym32() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.SYM32);
        buffer.writeInt(Integer.MAX_VALUE);

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedSym8() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.SYM8);
        buffer.writeByte(Byte.MAX_VALUE);

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray50() throws Throwable {
        doEncodeDecodeSmallSymbolArrayTestImpl(50, false);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray100() throws Throwable {
        doEncodeDecodeSmallSymbolArrayTestImpl(100, false);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray384() throws Throwable {
        doEncodeDecodeSmallSymbolArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray50FS() throws Throwable {
        doEncodeDecodeSmallSymbolArrayTestImpl(50, true);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray100FS() throws Throwable {
        doEncodeDecodeSmallSymbolArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray384FS() throws Throwable {
        doEncodeDecodeSmallSymbolArrayTestImpl(384, true);
    }

    private void doEncodeDecodeSmallSymbolArrayTestImpl(int count, boolean fromStream) throws Throwable {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);
        Symbol[] source = createPayloadArraySmallSymbols(count);

        try {
            assertEquals(count, source.length, "Unexpected source array length");

            int encodingWidth = 4;
            int arrayPayloadSize = encodingWidth + 1 + (count * 5); // variable width for element count + byte type descriptor + (number of elements * size[=length+content-char])
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ProtonBuffer expectedEncodingWrapper = ProtonByteBufferAllocator.DEFAULT.wrap(expectedEncoding);
            expectedEncodingWrapper.setWriteIndex(0);

            // Write the array encoding code, array size, and element count
            expectedEncodingWrapper.writeByte((byte) 0xF0); // 'array32' type descriptor code
            expectedEncodingWrapper.writeInt(arrayPayloadSize);
            expectedEncodingWrapper.writeInt(count);

            // Write the type descriptor
            expectedEncodingWrapper.writeByte((byte) 0xb3); // 'sym32' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                Symbol symbol = source[i];
                assertEquals(1, symbol.getLength(), "Unexpected length");

                expectedEncodingWrapper.writeInt(1); // Length
                expectedEncodingWrapper.writeByte(symbol.toString().charAt(0)); // Content
            }

            assertFalse(expectedEncodingWrapper.isWritable(), "Should have filled expected encoding array");

            // Now verify against the actual encoding of the array
            assertEquals(0, buffer.getReadIndex(), "Unexpected buffer position");
            encoder.writeArray(buffer, encoderState, source);
            assertEquals(expectedEncodedArraySize, buffer.getReadableBytes(), "Unexpected encoded payload length");

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.markReadIndex();
            buffer.readBytes(actualEncoding);
            assertFalse(buffer.isReadable(), "Should have drained the encoder buffer contents");

            assertArrayEquals(expectedEncoding, actualEncoding, "Unexpected actual array encoding");

            // Now verify against the decoding
            buffer.resetReadIndex();

            final Object decoded;
            if (fromStream) {
                decoded = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoded = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertEquals(Symbol.class, decoded.getClass().getComponentType());

            assertArrayEquals(source, (Symbol[]) decoded, "Unexpected decoding");
        } catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    // Creates 1 char Symbols with chars of 0-9, for encoding as sym8
    private static Symbol[] createPayloadArraySmallSymbols(int length) {
        Random rand = new Random(System.currentTimeMillis());

        Symbol[] payload = new Symbol[length];
        for (int i = 0; i < length; i++) {
            payload[i] = Symbol.valueOf(String.valueOf(rand.nextInt(9)));
        }

        return payload;
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < 10; ++i) {
            encoder.writeSymbol(buffer, encoderState, Symbol.valueOf("skipMe"));
        }

        Symbol expected = Symbol.valueOf("expected-symbol-value");

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Symbol.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Symbol.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Symbol);

        Symbol value = (Symbol) result;
        assertEquals(expected, value);
    }
}
