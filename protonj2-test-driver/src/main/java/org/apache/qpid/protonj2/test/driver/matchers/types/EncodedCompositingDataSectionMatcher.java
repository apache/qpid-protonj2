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
import java.util.Arrays;
import java.util.Objects;

import org.apache.qpid.protonj2.test.driver.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Data Section matcher that can be used with multiple expectTransfer calls to match a larger
 * given payload block to the contents of one or more incoming Data sections split across multiple
 * transfer frames and or multiple Data Sections within those transfer frames.
 */
public class EncodedCompositingDataSectionMatcher extends TypeSafeMatcher<ByteBuffer> {

    private static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");
    private static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);

    private final int expectedValueSize;
    private final ByteBuffer expectedValue;

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
        this(ByteBuffer.wrap(Arrays.copyOf(expectedValue, expectedValue.length)).asReadOnlyBuffer());
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedCompositingDataSectionMatcher(Binary expectedValue) {
        this(ByteBuffer.wrap(expectedValue.arrayCopy()).asReadOnlyBuffer());
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedCompositingDataSectionMatcher(ByteBuffer expectedValue) {
        Objects.requireNonNull(expectedValue, "The expected value cannot be null for this matcher");

        this.expectedValue = expectedValue.asReadOnlyBuffer();
        this.expectedRemainingBytes = this.expectedValue.remaining();
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
    protected boolean matchesSafely(ByteBuffer receivedBinary) {
        if (expectDataSectionPreamble) {
            Object descriptor = readDescribedTypeEncoding(receivedBinary);

            if (!(DESCRIPTOR_CODE.equals(descriptor) || DESCRIPTOR_SYMBOL.equals(descriptor))) {
                return false;
            }

            // Should be a Binary AMQP type with a length value and possibly some bytes
            byte encodingCode = receivedBinary.get();

            if (encodingCode == EncodingCodes.VBIN8) {
                expectedCurrentDataSectionBytes = receivedBinary.get() & 0xFF;
            } else if (encodingCode == EncodingCodes.VBIN32) {
                expectedCurrentDataSectionBytes = receivedBinary.getInt();
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
            final int currentChunkSize = Math.min(expectedCurrentDataSectionBytes, receivedBinary.remaining());

            final ByteBuffer expectedValueChunk = expectedValue.slice().limit(currentChunkSize);
            final ByteBuffer currentChunk = receivedBinary.slice().limit(currentChunkSize);

            receivedBinary.position(receivedBinary.position() + currentChunkSize);
            expectedValue.position(expectedValue.position() + currentChunkSize);

            if (!expectedValueChunk.equals(currentChunk)) {
                return false;
            }

            expectedRemainingBytes -= currentChunkSize;
            expectedCurrentDataSectionBytes -= currentChunkSize;

            if (expectedRemainingBytes != 0 && expectedCurrentDataSectionBytes == 0) {
                expectDataSectionPreamble = true;
                expectedCurrentDataSectionBytes = -1;
            }
        }

        if (expectedRemainingBytes == 0 && receivedBinary.remaining() > 0 && !isTrailingBytesExpected()) {
            unexpectedTrailingBytes = true;
            return false;
        } else {
            return true;
        }
    }

    private static final int DESCRIBED_TYPE_INDICATOR = 0;

    private Object readDescribedTypeEncoding(ByteBuffer data) {
        byte encodingCode = data.get();

        if (encodingCode == DESCRIBED_TYPE_INDICATOR) {
            encodingCode = data.get();
            switch (encodingCode) {
                case EncodingCodes.ULONG0:
                    return UnsignedLong.ZERO;
                case EncodingCodes.SMALLULONG:
                    return UnsignedLong.valueOf(data.get() & 0xff);
                case EncodingCodes.ULONG:
                    return UnsignedLong.valueOf(data.getLong());
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

    private static Symbol readSymbol32(ByteBuffer buffer) {
        return readSymbol(buffer.getInt(), buffer);
    }

    private static Symbol readSymbol8(ByteBuffer buffer) {
        return readSymbol(buffer.get() & 0xFF, buffer);
    }

    private static Symbol readSymbol(int length, ByteBuffer buffer) {
        if (length == 0) {
            return Symbol.valueOf("");
        } else {
            final byte[] symbolBytes = new byte[length];

            buffer.get(symbolBytes);

            final ByteBuffer symbolBuffer = ByteBuffer.wrap(symbolBytes).asReadOnlyBuffer();

            return Symbol.getSymbol(symbolBuffer, false);
        }
    }

    @Override
    protected void describeMismatchSafely(ByteBuffer item, Description mismatchDescription) {
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