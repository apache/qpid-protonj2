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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Character.UnicodeBlock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StringTypeCodecTest extends CodecTestSupport {

    private static final List<String> TEST_DATA = generateTestData();

    private final String SMALL_STRING_VALUE = "Small String";
    private final String LARGE_STRING_VALUE = "Large String: " +
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.UINT);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);

            try {
                streamDecoder.readString(stream, streamDecoderState);
                fail("Should not allow read of integer type as this type");
            } catch (DecodeException e) {}
        } else {
            try {
                decoder.readString(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            assertNull(streamDecoder.readString(stream, streamDecoderState));
        } else {
            assertNull(decoder.readString(buffer, decoderState));
        }
    }

    @Test
    public void testEncodeSmallString() throws IOException {
        doTestEncodeDecode(SMALL_STRING_VALUE, false);
    }

    @Test
    public void testEncodeLargeString() throws IOException {
        doTestEncodeDecode(LARGE_STRING_VALUE, false);
    }

    @Test
    public void testEncodeEmptyString() throws IOException {
        doTestEncodeDecode("", false);
    }

    @Test
    public void testEncodeNullString() throws IOException {
        doTestEncodeDecode(null, false);
    }

    @Test
    public void testEncodeSmallStringFS() throws IOException {
        doTestEncodeDecode(SMALL_STRING_VALUE, true);
    }

    @Test
    public void testEncodeLargeStringFS() throws IOException {
        doTestEncodeDecode(LARGE_STRING_VALUE, true);
    }

    @Test
    public void testEncodeEmptyStringFS() throws IOException {
        doTestEncodeDecode("", true);
    }

    @Test
    public void testEncodeNullStringFS() throws IOException {
        doTestEncodeDecode(null, true);
    }

    private void doTestEncodeDecode(String value, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, value);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        if (value != null) {
            assertNotNull(result);
            assertTrue(result instanceof String);
        } else {
            assertNull(result);
        }

        assertEquals(value, result);
    }

    @Test
    public void testDecodeSmallSeriesOfStrings() throws IOException {
        doTestDecodeStringSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfStrings() throws IOException {
        doTestDecodeStringSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfStringsFS() throws IOException {
        doTestDecodeStringSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfStringsFS() throws IOException {
        doTestDecodeStringSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeStringSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeString(buffer, encoderState, LARGE_STRING_VALUE);
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
            assertTrue(result instanceof String);
            assertEquals(LARGE_STRING_VALUE, result);
        }
    }

    @Test
    public void testDecodeStringOfZeroLengthWithLargeEncoding() throws IOException {
        doTestDecodeStringOfZeroLengthWithGivenEncoding(EncodingCodes.STR32, false);
    }

    @Test
    public void testDecodeStringOfZeroLengthWithSmallEncoding() throws IOException {
        doTestDecodeStringOfZeroLengthWithGivenEncoding(EncodingCodes.STR8, false);
    }

    @Test
    public void testDecodeStringOfZeroLengthWithLargeEncodingFS() throws IOException {
        doTestDecodeStringOfZeroLengthWithGivenEncoding(EncodingCodes.STR32, true);
    }

    @Test
    public void testDecodeStringOfZeroLengthWithSmallEncodingFS() throws IOException {
        doTestDecodeStringOfZeroLengthWithGivenEncoding(EncodingCodes.STR8, true);
    }

    private void doTestDecodeStringOfZeroLengthWithGivenEncoding(byte encodingCode, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        // Manually encode the type we want.
        if (encodingCode == EncodingCodes.STR32) {
            buffer.writeByte(EncodingCodes.STR32);
            buffer.writeInt(0);
        } else {
            buffer.writeByte(EncodingCodes.STR8);
            buffer.writeByte((byte) 0);
        }

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.peekNextTypeDecoder(stream, streamDecoderState);
            assertEquals(String.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), encodingCode & 0xFF);
        } else {
            TypeDecoder<?> typeDecoder = decoder.peekNextTypeDecoder(buffer, decoderState);
            assertEquals(String.class, typeDecoder.getTypeClass());
            assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
            assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), encodingCode & 0xFF);
        }

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertEquals("", result);
    }

    @Test
    public void testEncodeAndDecodeComplexStrings() throws IOException {
        testEncodeAndDecodeComplexStrings(false);
    }

    @Test
    public void testEncodeAndDecodeComplexStringsFS() throws IOException {
        testEncodeAndDecodeComplexStrings(true);
    }

    private void testEncodeAndDecodeComplexStrings(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (final String input : TEST_DATA) {
            encoder.writeString(buffer, encoderState, input);

            final Object result;
            if (fromStream) {
                final InputStream stream = new ProtonBufferInputStream(buffer.split());
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            buffer.clear();

            assertEquals(input, result);
        }
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedStr32() throws IOException {
        testEncodedSizeExceedsRemainingDetectedStr32(false);
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedStr32FS() throws IOException {
        testEncodedSizeExceedsRemainingDetectedStr32(true);
    }

    private void testEncodedSizeExceedsRemainingDetectedStr32(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(8);
        buffer.writeInt(Integer.MAX_VALUE);

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
    public void testEncodedSizeExceedsRemainingDetectedStr8() throws IOException {
        testEncodedSizeExceedsRemainingDetectedStr8(false);
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedStr8FS() throws IOException {
        testEncodedSizeExceedsRemainingDetectedStr8(true);
    }

    private void testEncodedSizeExceedsRemainingDetectedStr8(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte(EncodingCodes.STR8);
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

    //----- Test support for string encodings --------------------------------//

    private static List<String> generateTestData() {
        return new LinkedList<String>() {
            private static final long serialVersionUID = 7331717267070233454L;

            {
                // non-surrogate pair blocks
                addAll(getAllStringsFromUnicodeBlocks(UnicodeBlock.BASIC_LATIN,
                                                      UnicodeBlock.LATIN_1_SUPPLEMENT,
                                                      UnicodeBlock.GREEK,
                                                      UnicodeBlock.LETTERLIKE_SYMBOLS));
                // blocks with surrogate pairs
                addAll(getAllStringsFromUnicodeBlocks(UnicodeBlock.LINEAR_B_SYLLABARY,
                                                      UnicodeBlock.MISCELLANEOUS_SYMBOLS_AND_PICTOGRAPHS,
                                                      UnicodeBlock.MUSICAL_SYMBOLS,
                                                      UnicodeBlock.EMOTICONS,
                                                      UnicodeBlock.PLAYING_CARDS,
                                                      UnicodeBlock.BOX_DRAWING,
                                                      UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS,
                                                      UnicodeBlock.PRIVATE_USE_AREA,
                                                      UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_A,
                                                      UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_B));

                // some additional combinations of characters that could cause problems to the encoder
                String[] boxDrawing = getAllStringsFromUnicodeBlocks(UnicodeBlock.BOX_DRAWING).toArray(new String[0]);
                String[] halfFullWidthForms = getAllStringsFromUnicodeBlocks(UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS).toArray(new String[0]);
                for (int i = 0; i < halfFullWidthForms.length; i++) {
                    add(halfFullWidthForms[i] + boxDrawing[i % boxDrawing.length]);
                }
            }
        };
    }

    /**
     * Loop over all the chars in given {@link UnicodeBlock}s and return a {@link Set <String>}
     * containing all the possible values as their {@link String} values.
     *
     * @param blocks
     *        the {@link UnicodeBlock}s to loop over
     *
     * @return a {@link Set <String>} containing all the possible values as {@link String} values
     */
    private static Set<String> getAllStringsFromUnicodeBlocks(final UnicodeBlock... blocks) {
        final Set<UnicodeBlock> blockSet = new HashSet<>(Arrays.asList(blocks));
        final Set<String> strings = new HashSet<>();

        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (blockSet.contains(UnicodeBlock.of(codePoint))) {
                final int charCount = Character.charCount(codePoint);
                final StringBuilder sb = new StringBuilder(charCount);
                if (charCount == 1) {
                    sb.append(String.valueOf((char) codePoint));
                } else if (charCount == 2) {
                    sb.append(Character.highSurrogate(codePoint));
                    sb.append(Character.lowSurrogate(codePoint));
                } else {
                    throw new IllegalArgumentException("Character.charCount of " + charCount + " not supported.");
                }
                strings.add(sb.toString());
            }
        }
        return strings;
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeString(buffer, encoderState, "skipMe");
        }

        String expected = "expected-string-value";

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
                assertEquals(String.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.STR8 & 0xFF);
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(String.class, typeDecoder.getTypeClass());
                assertTrue(typeDecoder instanceof PrimitiveTypeDecoder);
                assertEquals(((PrimitiveTypeDecoder<?>) typeDecoder).getTypeCode(), EncodingCodes.STR8 & 0xFF);
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
        assertTrue(result instanceof String);

        String value = (String) result;
        assertEquals(expected, value);
    }

    @Test
    public void testDecodeNonStringWhenStringExpectedReportsUsefulError() {
        testDecodeNonStringWhenStringExpectedReportsUsefulError(false);
    }

    @Test
    public void testDecodeNonStringWhenStringExpectedReportsUsefulErrorFS() {
        testDecodeNonStringWhenStringExpectedReportsUsefulError(true);
    }

    private void testDecodeNonStringWhenStringExpectedReportsUsefulError(boolean fromStream) {
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        final UUID encoded = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(encoded.getMostSignificantBits());
        buffer.writeLong(encoded.getLeastSignificantBits());

        TypeDecoder<?> nextType = decoder.peekNextTypeDecoder(buffer, decoderState);
        assertEquals(UUID.class, nextType.getTypeClass());

        final int oldPos = buffer.getReadOffset();

        if (fromStream) {
            final InputStream stream = new ProtonBufferInputStream(buffer);

            try {
                streamDecoder.readString(stream, streamDecoderState);
            } catch (DecodeException ex) {
                // Should indicate the type that it found in the error
                assertTrue(ex.getMessage().contains(EncodingCodes.toString(EncodingCodes.UUID)));
            }
        } else {
            try {
                decoder.readString(buffer, decoderState);
            } catch (DecodeException ex) {
                // Should indicate the type that it found in the error
                assertTrue(ex.getMessage().contains(EncodingCodes.toString(EncodingCodes.UUID)));
            }

            buffer.setReadOffset(oldPos);
            UUID actual = decoder.readUUID(buffer, decoderState);
            assertEquals(encoded, actual);
        }
    }

    @Test
    public void testDecodeUnknownTypeWhenStringExpectedReportsUsefulError() {
        testDecodeUnknownTypeWhenStringExpectedReportsUsefulError(false);
    }

    @Test
    public void testDecodeUnknownTypeWhenStringExpectedReportsUsefulErrorFS() {
        testDecodeUnknownTypeWhenStringExpectedReportsUsefulError(true);
    }

    private void testDecodeUnknownTypeWhenStringExpectedReportsUsefulError(boolean fromStream) {
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0x01);

        if (fromStream) {
            final InputStream stream = new ProtonBufferInputStream(buffer);

            try {
                streamDecoder.readString(stream, streamDecoderState);
            } catch (DecodeException ex) {
                // Should indicate the type that it found in the error
                assertTrue(ex.getMessage().contains("Unknown-Type:0x01"));
            }
        } else {
            try {
                decoder.readString(buffer, decoderState);
            } catch (DecodeException ex) {
                // Should indicate the type that it found in the error
                assertTrue(ex.getMessage().contains("Unknown-Type:0x01"));
            }
        }
    }

    @Test
    public void testStreamSkipOfStringEncodingHandlesIOException() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, "test-string-value");

        InputStream stream = new ProtonBufferInputStream(buffer);
        StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
        assertEquals(String.class, typeDecoder.getTypeClass());

        stream = Mockito.spy(stream);

        Mockito.when(stream.skip(Mockito.anyLong())).thenThrow(EOFException.class);

        try {
            typeDecoder.skipValue(stream, streamDecoderState);
            fail("Expected an exception on skip of encoded string failure.");
        } catch (DecodeException dex) {}
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

        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte((byte) 8);
        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(16);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(8, typeDecoder.readSize(stream, streamDecoderState));
            typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(16, typeDecoder.readSize(stream, streamDecoderState));
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(8, typeDecoder.readSize(buffer, decoderState));
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(16, typeDecoder.readSize(buffer, decoderState));
        }
    }
}
