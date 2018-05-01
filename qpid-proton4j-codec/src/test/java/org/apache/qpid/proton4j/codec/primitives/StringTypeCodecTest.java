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

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.junit.Test;

public class StringTypeCodecTest extends CodecTestSupport {

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
}
