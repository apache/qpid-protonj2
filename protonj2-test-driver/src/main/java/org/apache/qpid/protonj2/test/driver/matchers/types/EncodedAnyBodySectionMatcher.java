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

package org.apache.qpid.protonj2.test.driver.matchers.types;

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.test.driver.codec.Codec;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Description;

/**
 * Encoded type matcher that validates that the encoded AMQP type is a valid body
 * section in the standard AMQP message format set of allowed sections, Data, AmqpValue
 * or AmqpSequence.
 */
public class EncodedAnyBodySectionMatcher extends EncodedBodySectionMatcher {

    private static final Symbol AMQP_VALUE_DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:amqp-value:*");
    private static final UnsignedLong AMQP_VALUE_DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000077L);
    private static final Symbol AMQP_SEQUENCE_DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:amqp-sequence:list");
    private static final UnsignedLong AMQP_SEQUENCE_DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000076L);
    private static final Symbol DATA_DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");
    private static final UnsignedLong DATA_DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);

    private boolean allowTrailingBytes;
    private DescribedType decodedDescribedType;
    private boolean unexpectedTrailingBytes;

    public EncodedAnyBodySectionMatcher() {
        this(false);
    }

    public EncodedAnyBodySectionMatcher(boolean allowTrailingBytes) {
        this.allowTrailingBytes = allowTrailingBytes;
    }

    @Override
    public void setAllowTrailingBytes(boolean allowTrailingBytes) {
        this.allowTrailingBytes = allowTrailingBytes;
    }

    @Override
    public boolean isAllowTrailngBytes() {
        return allowTrailingBytes;
    }

    @Override
    protected boolean matchesSafely(ByteBuffer receivedBinary) {
        int length = receivedBinary.remaining();
        Codec data = Codec.Factory.create();
        long decoded = data.decode(receivedBinary);
        decodedDescribedType = data.getDescribedType();
        Object descriptor = decodedDescribedType.getDescriptor();

        if (!AMQP_VALUE_DESCRIPTOR_CODE.equals(descriptor) &&
            !AMQP_VALUE_DESCRIPTOR_SYMBOL.equals(descriptor) &&
            !AMQP_SEQUENCE_DESCRIPTOR_CODE.equals(descriptor) &&
            !AMQP_SEQUENCE_DESCRIPTOR_SYMBOL.equals(descriptor) &&
            !DATA_DESCRIPTOR_CODE.equals(descriptor) &&
            !DATA_DESCRIPTOR_SYMBOL.equals(descriptor)) {

            return false;
        }

        // We are strict and require the encoding to have a value.
        if (decodedDescribedType.getDescribed() == null) {
            return false;
        }

        if (decoded < length && !allowTrailingBytes) {
            unexpectedTrailingBytes = true;
            return false;
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a body section of any AMQP Value, Sequence or Data section.");
    }

    @Override
    protected void describeMismatchSafely(ByteBuffer item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual encoded form: ").appendValue(item);

        if (decodedDescribedType != null) {
            mismatchDescription.appendText("\nExpected descriptor of AMQP Value, Sequence or Data section: ");
            mismatchDescription.appendText("\nActual described type: ").appendValue(decodedDescribedType);
        }

        if (unexpectedTrailingBytes) {
            mismatchDescription.appendText("\nUnexpected trailing bytes in provided bytes after decoding!");
        }
    }
}
