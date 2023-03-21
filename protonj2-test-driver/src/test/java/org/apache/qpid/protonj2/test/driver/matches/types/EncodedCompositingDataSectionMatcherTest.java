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
package org.apache.qpid.protonj2.test.driver.matches.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedCompositingDataSectionMatcher;
import org.junit.jupiter.api.Test;

class EncodedCompositingDataSectionMatcherTest {

    @Test
    public void testIncomingReadWithoutDataSectionFailsValidation() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        ByteBuffer incomingBytes = ByteBuffer.allocate(EXPECTED_SIZE);

        incomingBytes.put(PAYLOAD).flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertFalse(matcher.matches(incomingBytes));
    }

    @Test
    public void testValidatePartiallyTransmittedDataSectionShouldSucceed() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK = new byte[EXPECTED_SIZE / 2];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK);

        ByteBuffer partial1 = ByteBuffer.allocate(EXPECTED_SIZE);

        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(EXPECTED_SIZE);
        partial1.put(CHUNK);
        partial1.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
    }

    @Test
    public void testIncorrectPartiallyTransmittedDataSectionShouldNotSucceed() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK = new byte[EXPECTED_SIZE / 2];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED + 1);
        bytesGenerator.nextBytes(CHUNK);

        ByteBuffer partial1 = ByteBuffer.allocate(EXPECTED_SIZE);

        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(EXPECTED_SIZE);
        partial1.put(CHUNK);
        partial1.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertFalse(matcher.matches(partial1));
    }

    @Test
    public void testValidateSplitFameTransmittedDataSectionShouldSucceed() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK1 = new byte[EXPECTED_SIZE / 2];
        final byte[] CHUNK2 = new byte[EXPECTED_SIZE / 2];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK1);
        bytesGenerator.nextBytes(CHUNK2);

        final ByteBuffer partial1 = ByteBuffer.allocate(EXPECTED_SIZE);
        final ByteBuffer partial2 = ByteBuffer.allocate(EXPECTED_SIZE);

        // First half arrives with preamble
        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(EXPECTED_SIZE);
        partial1.put(CHUNK1);
        partial1.flip();

        // Second half arrives without preamble as expected
        partial2.put(CHUNK2);
        partial2.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertThat(partial2, matcher);
    }

    @Test
    public void testValidateMultiFameTransmittedDataSectionShouldSucceed() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK1 = new byte[EXPECTED_SIZE / 4];
        final byte[] CHUNK2 = new byte[EXPECTED_SIZE / 4];
        final byte[] CHUNK3 = new byte[EXPECTED_SIZE / 4];
        final byte[] CHUNK4 = new byte[EXPECTED_SIZE / 4];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK1);
        bytesGenerator.nextBytes(CHUNK2);
        bytesGenerator.nextBytes(CHUNK3);
        bytesGenerator.nextBytes(CHUNK4);

        final ByteBuffer partial1 = ByteBuffer.allocate(EXPECTED_SIZE);
        final ByteBuffer partial2 = ByteBuffer.allocate(EXPECTED_SIZE);
        final ByteBuffer partial3 = ByteBuffer.allocate(EXPECTED_SIZE);
        final ByteBuffer partial4 = ByteBuffer.allocate(EXPECTED_SIZE);

        // First chunk arrives with preamble
        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(EXPECTED_SIZE);
        partial1.put(CHUNK1);
        partial1.flip();

        // Second chunk arrives without preamble as expected
        partial2.put(CHUNK2);
        partial2.flip();
        // Third chunk arrives without preamble as expected
        partial3.put(CHUNK3);
        partial3.flip();
        // Fourth chunk arrives without preamble as expected
        partial4.put(CHUNK4);
        partial4.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertThat(partial2, matcher);
        assertThat(partial3, matcher);
        assertThat(partial4, matcher);

        final ByteBuffer partial5 = ByteBuffer.allocate(4);

        partial5.put(new byte[] { 3, 3, 3, 3});
        partial5.flip();

        // Anything else that arrives that is handed to this matcher should fail
        assertFalse(matcher.matches(partial5));
    }

    @Test
    public void testUnevenTransferOfBytesSplitStillPassesValidation() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] CHUNK1 = new byte[] { 0, 1, 2 };
        final byte[] CHUNK2 = new byte[] { 3, 4, 5, 6, 7, 8, 9 };

        final ByteBuffer partial1 = ByteBuffer.allocate(16);
        final ByteBuffer partial2 = ByteBuffer.allocate(16);

        // First half arrives with preamble
        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(PAYLOAD.length);
        partial1.put(CHUNK1);
        partial1.flip();

        // Second half arrives without preamble as expected
        partial2.put(CHUNK2);
        partial2.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertThat(partial2, matcher);
    }

    @Test
    public void testIncorrectSplitFameTransmittedDataSectionShouldNotSucceed() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK1 = new byte[EXPECTED_SIZE / 2];
        final byte[] CHUNK2 = new byte[EXPECTED_SIZE / 2];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK1);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK2);

        final ByteBuffer partial1 = ByteBuffer.allocate(256);
        final ByteBuffer partial2 = ByteBuffer.allocate(128);

        // First half arrives with preamble
        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(EXPECTED_SIZE);
        partial1.put(CHUNK1);
        partial1.flip();

        // Second half arrives without preamble as expected
        partial2.put(CHUNK2);
        partial2.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertFalse(matcher.matches(partial2));
    }

    @Test
    public void testTrailingBytesCausesFailureInSameReadOperation() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK = new byte[EXPECTED_SIZE];
        final byte[] EXTRA = new byte[] { 1, 0, 1, 0, 1, 0, 1 };

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK);

        final ByteBuffer inboundBytes = ByteBuffer.allocate(512);

        inboundBytes.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        inboundBytes.put(EncodingCodes.SMALLULONG);
        inboundBytes.put(Data.DESCRIPTOR_CODE.byteValue());
        inboundBytes.put(EncodingCodes.VBIN32);
        inboundBytes.putInt(EXPECTED_SIZE);
        inboundBytes.put(CHUNK);
        inboundBytes.put(EXTRA);
        inboundBytes.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertFalse(matcher.matches(inboundBytes));
    }

    @Test
    public void testMultipleDataSectionsThatSupplyTheExpectedBytes() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK1 = new byte[EXPECTED_SIZE / 2];
        final byte[] CHUNK2 = new byte[EXPECTED_SIZE / 2];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK1);
        bytesGenerator.nextBytes(CHUNK2);

        // First half arrives with preamble
        final ByteBuffer partial1 = ByteBuffer.allocate(256);
        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(CHUNK1.length);
        partial1.put(CHUNK1);
        partial1.flip();

        // Second half arrives without preamble as expected
        final ByteBuffer partial2 = ByteBuffer.allocate(256);
        partial2.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial2.put(EncodingCodes.SMALLULONG);
        partial2.put(Data.DESCRIPTOR_CODE.byteValue());
        partial2.put(EncodingCodes.VBIN32);
        partial2.putInt(CHUNK2.length);
        partial2.put(CHUNK2);
        partial2.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertThat(partial2, matcher);
    }

    @Test
    public void testMultipleDataSectionsThatSupplyUnexpectedBytes() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];
        final byte[] CHUNK1 = new byte[EXPECTED_SIZE / 2];
        final byte[] CHUNK2 = new byte[EXPECTED_SIZE / 2];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        bytesGenerator.setSeed(SEED);
        bytesGenerator.nextBytes(CHUNK1);
        bytesGenerator.nextBytes(CHUNK2);

        // First half arrives with preamble
        final ByteBuffer partial1 = ByteBuffer.allocate(256);
        partial1.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.put(EncodingCodes.SMALLULONG);
        partial1.put(Data.DESCRIPTOR_CODE.byteValue());
        partial1.put(EncodingCodes.VBIN32);
        partial1.putInt(CHUNK1.length);
        partial1.put(CHUNK1);
        partial1.flip();

        // Second half arrives without preamble as expected
        final ByteBuffer partial2 = ByteBuffer.allocate(128 + 9);
        partial2.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial2.put(EncodingCodes.SMALLULONG);
        partial2.put(Data.DESCRIPTOR_CODE.byteValue());
        partial2.put(EncodingCodes.VBIN32);
        partial2.putInt(CHUNK2.length + 1);
        partial2.put(CHUNK2);
        partial2.put((byte) 64);
        partial2.flip();

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertFalse(matcher.matches(partial2));
    }
}
