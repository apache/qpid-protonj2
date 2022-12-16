/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.protonj2.test.driver.matchers.types;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.qpid.protonj2.test.driver.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;

/**
 * Data Section matcher that can be used with multiple expectTransfer calls to match a larger
 * given payload block to the contents of one or more incoming Data sections split across multiple
 * transfer frames and or multiple Data Sections within those transfer frames.
 */
public class EncodedCompositingDataSectionMatcher extends TypeSafeMatcher<Buffer> {

    private static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");
    private static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);

    private final int expectedValueSize;
    private final Buffer expectedValue;

    private boolean expectTrailingBytes;
    private String decodingErrorDescription;

    // State data used during validation of the composite data values
    private boolean unexpectedTrailingBytes;
    private boolean expectDataSectionPreamble = true;
    private int expectedCurrentDataSectionBytes = -1;
    private int expectedRemainingBytes;

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedCompositingDataSectionMatcher(byte[] expectedValue) {
        this(BufferAllocator.onHeapUnpooled().copyOf(expectedValue).makeReadOnly());
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedCompositingDataSectionMatcher(Binary expectedValue) {
        this(BufferAllocator.onHeapUnpooled().copyOf(expectedValue.asByteBuffer()).makeReadOnly());
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedCompositingDataSectionMatcher(Buffer expectedValue) {
        Objects.requireNonNull(expectedValue, "The expected value cannot be null for this matcher");

        this.expectedValue = expectedValue;
        this.expectedRemainingBytes = this.expectedValue.readableBytes();
        this.expectedValueSize = this.expectedRemainingBytes;
    }

    public boolean isTrailingBytesExpected() {
        return expectTrailingBytes;
    }

    public EncodedCompositingDataSectionMatcher setExpectTrailingBytes(boolean expectTrailingBytes) {
        this.expectTrailingBytes = expectTrailingBytes;
        return this;
    }

    protected Object getExpectedValue() {
        return expectedValue;
    }

    @Override
    protected boolean matchesSafely(Buffer receivedBinary) {
        if (expectDataSectionPreamble) {
            Object descriptor = readDescribedTypeEncoding(receivedBinary);

            if (!(DESCRIPTOR_CODE.equals(descriptor) || DESCRIPTOR_SYMBOL.equals(descriptor))) {
                return false;
            }

            // Should be a Binary AMQP type with a length value and possibly some bytes
            byte encodingCode = receivedBinary.readByte();

            if (encodingCode == EncodingCodes.VBIN8) {
                expectedCurrentDataSectionBytes = receivedBinary.readByte() & 0xFF;
            } else if (encodingCode == EncodingCodes.VBIN32) {
                expectedCurrentDataSectionBytes = receivedBinary.readInt();
            } else {
                decodingErrorDescription = "Expected to read a Binary Type but read encoding code: " + encodingCode;
                return false;
            }

            if (expectedCurrentDataSectionBytes > expectedRemainingBytes) {
                decodingErrorDescription = "Expected encoded Binary to indicate size of: " + expectedRemainingBytes + ", " +
                                           "or less but read an encoded size of: " + expectedCurrentDataSectionBytes;
                return false;
            }

            expectDataSectionPreamble = false;  // We got the current preamble
        }

        if (expectedRemainingBytes != 0) {
            final int currentChunkSize = Math.min(expectedCurrentDataSectionBytes, receivedBinary.readableBytes());
            try (Buffer expectedValueChunk = expectedValue.copy(expectedValue.readerOffset(), currentChunkSize, true);
                 Buffer currentChunk = receivedBinary.copy(receivedBinary.readerOffset(), currentChunkSize, true)) {

                receivedBinary.skipReadableBytes(currentChunkSize);
                expectedValue.skipReadableBytes(currentChunkSize);

                if (!expectedValueChunk.equals(currentChunk)) {
                    return false;
                }

                expectedRemainingBytes -= currentChunkSize;
                expectedCurrentDataSectionBytes -= currentChunkSize;
            }

            if (expectedRemainingBytes != 0 && expectedCurrentDataSectionBytes == 0) {
                expectDataSectionPreamble = true;
                expectedCurrentDataSectionBytes = -1;
            }
        }

        if (expectedRemainingBytes == 0 && receivedBinary.readableBytes() > 0 && !isTrailingBytesExpected()) {
            unexpectedTrailingBytes = true;
            return false;
        } else {
            return true;
        }
    }

    private static final int DESCRIBED_TYPE_INDICATOR = 0;

    private Object readDescribedTypeEncoding(Buffer data) {
        byte encodingCode = data.readByte();

        if (encodingCode == DESCRIBED_TYPE_INDICATOR) {
            encodingCode = data.readByte();
            switch (encodingCode) {
                case EncodingCodes.ULONG0:
                    return UnsignedLong.ZERO;
                case EncodingCodes.SMALLULONG:
                    return UnsignedLong.valueOf(data.readByte() & 0xff);
                case EncodingCodes.ULONG:
                    return UnsignedLong.valueOf(data.readLong());
                case EncodingCodes.SYM8:
                    return readSymbol8(data);
                case EncodingCodes.SYM32:
                    return readSymbol32(data);
                default:
                    decodingErrorDescription = "Expected Unsigned Long or Symbol type but found encoding: " +  encodingCode;
            }
        } else {
            decodingErrorDescription = "Expected to read a Described Type but read encoding code: " + encodingCode;
        }

        return null;
    }

    private Symbol readSymbol32(Buffer buffer) {
        int length = buffer.readInt();

        if (length == 0) {
            return Symbol.valueOf("");
        } else {
            final ByteBuffer symbolBuffer = ByteBuffer.allocate(length);
            buffer.readBytes(symbolBuffer);

            return Symbol.getSymbol(symbolBuffer, false);
        }
    }

    private Symbol readSymbol8(Buffer buffer) {
        int length = buffer.readByte() & 0xFF;

        if (length == 0) {
            return Symbol.valueOf("");
        } else {
            final ByteBuffer symbolBuffer = ByteBuffer.allocate(length);
            buffer.readBytes(symbolBuffer);

            return Symbol.getSymbol(symbolBuffer, false);
        }
    }

    @Override
    protected void describeMismatchSafely(Buffer item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual encoded form: ").appendValue(item);

        if (decodingErrorDescription != null) {
            mismatchDescription.appendText("\nExpected descriptor: ")
                               .appendValue(DESCRIPTOR_SYMBOL)
                               .appendText(" / ")
                               .appendValue(DESCRIPTOR_CODE);
            mismatchDescription.appendText("\nError that failed the validation: ").appendValue(decodingErrorDescription);
        }

        if (unexpectedTrailingBytes) {
            mismatchDescription.appendText("\nUnexpected trailing bytes in provided bytes after decoding!");
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a complete Binary encoding of a Data section that wraps")
                   .appendText(" an collection of bytes of eventual size {").appendValue(expectedValueSize)
                   .appendText("}").appendText(" containing: ").appendValue(getExpectedValue());
    }
}