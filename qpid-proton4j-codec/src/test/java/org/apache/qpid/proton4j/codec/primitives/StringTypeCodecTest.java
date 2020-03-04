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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.Character.UnicodeBlock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.junit.Test;

public class StringTypeCodecTest extends CodecTestSupport {

    private static final List<String> TEST_DATA = generateTestData();

    private final String SMALL_STRING_VALUIE = "Small String";
    private final String LARGE_STRING_VALUIE = "Large String: " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog.";

    @Test
    public void testLookupTypeDecoderForType() throws Exception {
        TypeDecoder<?> result = decoder.getTypeDecoder("");

        assertNotNull(result);
        assertEquals(String.class, result.getTypeClass());
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readString(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (IOException e) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readString(buffer, decoderState));
    }

    @Test
    public void testEncodeSmallString() throws IOException {
        doTestEncodeDecode(SMALL_STRING_VALUIE);
    }

    @Test
    public void testEncodeLargeString() throws IOException {
        doTestEncodeDecode(LARGE_STRING_VALUIE);
    }

    @Test
    public void testEncodeEmptyString() throws IOException {
        doTestEncodeDecode("");
    }

    @Test
    public void testEncodeNullString() throws IOException {
        doTestEncodeDecode(null);
    }

    private void doTestEncodeDecode(String value) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

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
        doTestDecodeStringSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfStrings() throws IOException {
        doTestDecodeStringSeries(LARGE_SIZE);
    }

    private void doTestDecodeStringSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeString(buffer, encoderState, LARGE_STRING_VALUIE);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof String);
            assertEquals(LARGE_STRING_VALUIE, result);
        }
    }

    @Test
    public void testDecodeStringOfZeroLengthWithLargeEncoding() throws IOException {
        doTestDecodeStringOfZeroLengthWithGivenEncoding(EncodingCodes.STR32);
    }

    @Test
    public void testDecodeStringOfZeroLengthWithSmallEncoding() throws IOException {
        doTestDecodeStringOfZeroLengthWithGivenEncoding(EncodingCodes.STR8);
    }

    private void doTestDecodeStringOfZeroLengthWithGivenEncoding(byte encodingCode) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        // Manually encode the type we want.
        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(0);

        String result = decoder.readString(buffer, decoderState);
        assertNotNull(result);
        assertEquals("", result);
    }

    @Test
    public void testEncodeAndDecodeComplexStrings() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (final String input : TEST_DATA) {
            encoder.writeString(buffer, encoderState, input);
            String result = decoder.readString(buffer, decoderState);

            buffer.clear();

            assertEquals(input, result);
        }
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedStr32() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(8);
        buffer.writeInt(Integer.MAX_VALUE);

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testEncodedSizeExceedsRemainingDetectedStr8() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(4);
        buffer.writeByte(Byte.MAX_VALUE);

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeString(buffer, encoderState, "skipMe");
        }

        String expected = "expected-string-value";

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(String.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof String);

        String value = (String) result;
        assertEquals(expected, value);
    }
}
