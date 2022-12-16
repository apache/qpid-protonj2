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

import java.util.Random;

import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedCompositingDataSectionMatcher;
import org.junit.jupiter.api.Test;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;

class EncodedCompositingDataSectionMatcherTest {

    @Test
    public void testIncomingReadWithoutDataSectionFailsValidation() {
        final long SEED = System.nanoTime();
        final int EXPECTED_SIZE = 256;
        final byte[] PAYLOAD = new byte[EXPECTED_SIZE];

        Random bytesGenerator = new Random(SEED);
        bytesGenerator.nextBytes(PAYLOAD);
        Buffer incomingBytes = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);

        incomingBytes.writeBytes(PAYLOAD);

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

        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);

        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(EXPECTED_SIZE);
        partial1.writeBytes(CHUNK);

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

        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);

        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(EXPECTED_SIZE);
        partial1.writeBytes(CHUNK);

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

        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);
        Buffer partial2 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);

        // First half arrives with preamble
        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(EXPECTED_SIZE);
        partial1.writeBytes(CHUNK1);

        // Second half arrives without preamble as expected
        partial2.writeBytes(CHUNK2);

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

        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);
        Buffer partial2 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);
        Buffer partial3 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);
        Buffer partial4 = BufferAllocator.onHeapUnpooled().allocate(EXPECTED_SIZE);

        // First chunk arrives with preamble
        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(EXPECTED_SIZE);
        partial1.writeBytes(CHUNK1);

        // Second chunk arrives without preamble as expected
        partial2.writeBytes(CHUNK2);
        // Third chunk arrives without preamble as expected
        partial3.writeBytes(CHUNK3);
        // Fourth chunk arrives without preamble as expected
        partial4.writeBytes(CHUNK4);

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertThat(partial2, matcher);
        assertThat(partial3, matcher);
        assertThat(partial4, matcher);

        // Anything else that arrives that is handed to this matcher should fail
        try (Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(4).writeBytes(new byte[] { 3, 3, 3, 3})) {
            assertFalse(matcher.matches(buffer));
        }
    }

    @Test
    public void testUnevenTransferOfBytesSplitStillPassesValidation() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] CHUNK1 = new byte[] { 0, 1, 2 };
        final byte[] CHUNK2 = new byte[] { 3, 4, 5, 6, 7, 8, 9 };

        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(16);
        Buffer partial2 = BufferAllocator.onHeapUnpooled().allocate(16);

        // First half arrives with preamble
        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(PAYLOAD.length);
        partial1.writeBytes(CHUNK1);

        // Second half arrives without preamble as expected
        partial2.writeBytes(CHUNK2);

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

        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(16);
        Buffer partial2 = BufferAllocator.onHeapUnpooled().allocate(16);

        // First half arrives with preamble
        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(EXPECTED_SIZE);
        partial1.writeBytes(CHUNK1);

        // Second half arrives without preamble as expected
        partial2.writeBytes(CHUNK2);

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

        Buffer inboundBytes = BufferAllocator.onHeapUnpooled().allocate(16);

        inboundBytes.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        inboundBytes.writeByte(EncodingCodes.SMALLULONG);
        inboundBytes.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        inboundBytes.writeByte(EncodingCodes.VBIN32);
        inboundBytes.writeInt(EXPECTED_SIZE);
        inboundBytes.writeBytes(CHUNK);
        inboundBytes.writeBytes(EXTRA);

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
        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(16);
        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(CHUNK1.length);
        partial1.writeBytes(CHUNK1);

        // Second half arrives without preamble as expected
        Buffer partial2 = BufferAllocator.onHeapUnpooled().allocate(16);
        partial2.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial2.writeByte(EncodingCodes.SMALLULONG);
        partial2.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial2.writeByte(EncodingCodes.VBIN32);
        partial2.writeInt(CHUNK2.length);
        partial2.writeBytes(CHUNK2);

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
        Buffer partial1 = BufferAllocator.onHeapUnpooled().allocate(16);
        partial1.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial1.writeByte(EncodingCodes.SMALLULONG);
        partial1.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial1.writeByte(EncodingCodes.VBIN32);
        partial1.writeInt(CHUNK1.length);
        partial1.writeBytes(CHUNK1);

        // Second half arrives without preamble as expected
        Buffer partial2 = BufferAllocator.onHeapUnpooled().allocate(16);
        partial2.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        partial2.writeByte(EncodingCodes.SMALLULONG);
        partial2.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        partial2.writeByte(EncodingCodes.VBIN32);
        partial2.writeInt(CHUNK2.length + 1);
        partial2.writeBytes(CHUNK2);
        partial2.writeByte((byte) 64);

        EncodedCompositingDataSectionMatcher matcher =
            new EncodedCompositingDataSectionMatcher(PAYLOAD);

        assertThat(partial1, matcher);
        assertFalse(matcher.matches(partial2));
    }
}
