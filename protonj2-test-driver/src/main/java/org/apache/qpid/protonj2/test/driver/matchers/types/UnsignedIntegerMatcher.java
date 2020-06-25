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

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for values that must decode to an AMQP UnsignedInteger
 */
public class UnsignedIntegerMatcher extends TypeSafeMatcher<UnsignedInteger> {

    private final UnsignedInteger expectedValue;

    public UnsignedIntegerMatcher(int expectedValue) {
        this.expectedValue = UnsignedInteger.valueOf(expectedValue);
    }

    public UnsignedIntegerMatcher(long expectedValue) {
        this.expectedValue = UnsignedInteger.valueOf(expectedValue);
    }

    public UnsignedIntegerMatcher(UnsignedInteger expectedValue) {
        this.expectedValue = expectedValue;
    }

    protected UnsignedInteger getExpectedValue() {
        return expectedValue;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected UnsignedInteger:{")
                   .appendValue(expectedValue)
                   .appendText("}");
    }

    @Override
    protected void describeMismatchSafely(UnsignedInteger item, Description mismatchDescription) {
        mismatchDescription.appendText("Actual value received:{")
                           .appendValue(item)
                           .appendText("}");
    }

    @Override
    protected boolean matchesSafely(UnsignedInteger item) {
        if (expectedValue == null) {
            return item == null;
        } else {
            return expectedValue.equals(item);
        }
    }
}
