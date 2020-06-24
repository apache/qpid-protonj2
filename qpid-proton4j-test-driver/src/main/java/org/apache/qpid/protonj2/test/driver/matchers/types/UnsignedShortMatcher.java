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

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for values that must decode to an AMQP UnsignedInteger
 */
public class UnsignedShortMatcher extends TypeSafeMatcher<UnsignedShort> {

    private final UnsignedShort expectedValue;

    public UnsignedShortMatcher(short expectedValue) {
        this.expectedValue = UnsignedShort.valueOf(expectedValue);
    }

    public UnsignedShortMatcher(int expectedValue) {
        this.expectedValue = UnsignedShort.valueOf(expectedValue);
    }

    public UnsignedShortMatcher(UnsignedShort expectedValue) {
        this.expectedValue = expectedValue;
    }

    protected UnsignedShort getExpectedValue() {
        return expectedValue;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected UnsignedShort:{")
                   .appendValue(expectedValue)
                   .appendText("}");
    }

    @Override
    protected void describeMismatchSafely(UnsignedShort item, Description mismatchDescription) {
        mismatchDescription.appendText("Actual value received:{")
                           .appendValue(item)
                           .appendText("}");
    }

    @Override
    protected boolean matchesSafely(UnsignedShort item) {
        if (expectedValue == null) {
            return item == null;
        } else {
            return expectedValue.equals(item);
        }
    }
}
