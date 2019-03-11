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
package org.apache.qpid.proton4j.amqp.driver.matchers.types;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public abstract class EncodedAmqpTypeMatcher extends TypeSafeMatcher<Binary> {

    private final Symbol descriptorSymbol;
    private final UnsignedLong descriptorCode;
    private final Object expectedValue;
    private boolean permitTrailingBytes;
    private DescribedType decodedDescribedType;
    private boolean unexpectedTrailingBytes;

    public EncodedAmqpTypeMatcher(Symbol symbol, UnsignedLong code, Object expectedValue) {
        this(symbol, code, expectedValue, false);
    }

    public EncodedAmqpTypeMatcher(Symbol symbol, UnsignedLong code, Object expectedValue, boolean permitTrailingBytes) {
        this.descriptorSymbol = symbol;
        this.descriptorCode = code;
        this.expectedValue = expectedValue;
        this.permitTrailingBytes = permitTrailingBytes;
    }

    protected Object getExpectedValue() {
        return expectedValue;
    }

    @Override
    protected boolean matchesSafely(Binary receivedBinary) {
        int length = receivedBinary.getLength();
        Data data = null; // TODO Data.Factory.create();
        long decoded = 0l; // TODO data.decode(receivedBinary.asByteBuffer());
        decodedDescribedType = null; // TODO data.getDescribedType();
        Object descriptor = decodedDescribedType.getDescriptor();

        if (!(descriptorCode.equals(descriptor) || descriptorSymbol.equals(descriptor))) {
            return false;
        }

        if (expectedValue == null && decodedDescribedType.getDescribed() != null) {
            return false;
        } else if (expectedValue != null && !expectedValue.equals(decodedDescribedType.getDescribed())) {
            return false;
        }

        if (decoded < length && !permitTrailingBytes) {
            unexpectedTrailingBytes = true;
            return false;
        }

        return true;
    }

    @Override
    protected void describeMismatchSafely(Binary item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual encoded form: ").appendValue(item);

        if (decodedDescribedType != null) {
            mismatchDescription.appendText("\nExpected descriptor: ").appendValue(descriptorSymbol).appendText(" / ").appendValue(descriptorCode);

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