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

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.Codec;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

public abstract class EncodedAmqpTypeMatcher extends TypeSafeMatcher<ByteBuffer> {

    private final Symbol descriptorSymbol;
    private final UnsignedLong descriptorCode;
    private final Object expectedValue;
    private boolean allowTrailingBytes;
    private DescribedType decodedDescribedType;
    private boolean unexpectedTrailingBytes;

    public EncodedAmqpTypeMatcher(Symbol symbol, UnsignedLong code, Object expectedValue) {
        this(symbol, code, expectedValue, false);
    }

    public EncodedAmqpTypeMatcher(Symbol symbol, UnsignedLong code, Object expectedValue, boolean allowTrailingBytes) {
        this.descriptorSymbol = symbol;
        this.descriptorCode = code;
        this.expectedValue = expectedValue;
        this.allowTrailingBytes = allowTrailingBytes;
    }

    public void setAllowTrailingBytes(boolean allowTrailingBytes) {
        this.allowTrailingBytes = allowTrailingBytes;
    }

    public boolean isAllowTrailngBytes() {
        return allowTrailingBytes;
    }

    protected Object getExpectedValue() {
        return expectedValue;
    }

    @Override
    protected boolean matchesSafely(ByteBuffer receivedBinary) {
        int length = receivedBinary.remaining();
        Codec data = Codec.Factory.create();
        long decoded = data.decode(receivedBinary);
        decodedDescribedType = data.getDescribedType();
        Object descriptor = decodedDescribedType.getDescriptor();

        if (!(descriptorCode.equals(descriptor) || descriptorSymbol.equals(descriptor))) {
            return false;
        }

        if (expectedValue == null && decodedDescribedType.getDescribed() != null) {
            return false;
        } else if (expectedValue != null) {
            if (expectedValue instanceof Matcher) {
                Matcher<?> matcher = (Matcher<?>) expectedValue;
                if (!matcher.matches(decodedDescribedType.getDescribed())) {
                    return false;
                }
            } else if (expectedValue instanceof Map<?, ?>) {
                final Map<?, ?> expectedMap = (Map<?, ?>) expectedValue;

                if (!(decodedDescribedType.getDescribed() instanceof Map)) {
                    return false;
                }

                final Map<?, ?> receivedMap = (Map<?, ?>) decodedDescribedType.getDescribed();

                final Matcher<?> everyItemMatcher = everyItem(is(in(expectedMap.entrySet())));
                final Matcher<?> containsInAnyOrder = arrayContainingInAnyOrder(expectedMap.entrySet().toArray());

                if (receivedMap.size() != expectedMap.size()) {
                    return false;
                }

                if (!everyItemMatcher.matches(receivedMap.entrySet())) {
                    return false;
                }

                if (!containsInAnyOrder.matches(receivedMap.entrySet().toArray())) {
                    return false;
                }
            } else {
                final Matcher<?> expectedValueMatcher = Matchers.is(expectedValue);
                if (!expectedValueMatcher.matches(decodedDescribedType.getDescribed())) {
                    return false;
                }
            }
        }

        if (decoded < length && !allowTrailingBytes) {
            unexpectedTrailingBytes = true;
            return false;
        }

        return true;
    }

    @Override
    protected void describeMismatchSafely(ByteBuffer item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual encoded form: ").appendValue(item);

        if (decodedDescribedType != null) {
            mismatchDescription.appendText("\nExpected descriptor: ")
                               .appendValue(descriptorSymbol)
                               .appendText(" / ")
                               .appendValue(descriptorCode);
            mismatchDescription.appendText("\nActual described type: ").appendValue(decodedDescribedType);
        }

        if (unexpectedTrailingBytes) {
            mismatchDescription.appendText("\nUnexpected trailing bytes in provided bytes after decoding!");
        }
    }

    /**
     * Provide a description of this matcher.
     */
    @Override
    public abstract void describeTo(Description description);

}