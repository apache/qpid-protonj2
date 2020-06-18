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
package org.apache.qpid.proton4j.test.driver.matchers;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher used to validate that received described types match expectations.
 */
public abstract class ListDescribedTypeMatcher extends TypeSafeMatcher<ListDescribedType> {

    private String mismatchTextAddition;

    private final int numFields;

    private final UnsignedLong descriptorCode;
    private final Symbol descriptorSymbol;

    protected final Map<Enum<?>, Matcher<?>> fieldMatchers = new LinkedHashMap<>();

    public ListDescribedTypeMatcher(int numFields, UnsignedLong code, Symbol symbol) {
        this.descriptorCode = code;
        this.descriptorSymbol = symbol;
        this.numFields = numFields;
    }

    public ListDescribedTypeMatcher addFieldMatcher(Enum<?> field, Matcher<?> matcher) {
        if (field.ordinal() > numFields) {
            throw new IllegalArgumentException("Field enum supplied exceeds number of fields in type");
        }

        fieldMatchers.put(field, matcher);
        return this;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(getDescribedTypeClass().getSimpleName() + " which matches: ").appendValue(fieldMatchers);
    }

    @Override
    protected boolean matchesSafely(ListDescribedType received) {
        try {
            Object descriptor = received.getDescriptor();
            if (!descriptorCode.equals(descriptor) && !descriptorSymbol.equals(descriptor)) {
                mismatchTextAddition = "Descriptor mismatch";
                return false;
            }

            for (Map.Entry<Enum<?>, Matcher<?>> entry : fieldMatchers.entrySet()) {
                @SuppressWarnings("unchecked")
                Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
                assertThat("Field " + entry.getKey() + " value should match",
                    received.getFieldValue(entry.getKey().ordinal()), matcher);
            }
        } catch (AssertionError ae) {
            mismatchTextAddition = "AssertionFailure: " + ae.getMessage();
            return false;
        }

        return true;
    }

    @Override
    protected void describeMismatchSafely(ListDescribedType item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual form: ").appendValue(item);

        mismatchDescription.appendText("\nExpected descriptor: ")
                           .appendValue(descriptorSymbol)
                           .appendText(" / ")
                           .appendValue(descriptorCode);

        if (mismatchTextAddition != null) {
            mismatchDescription.appendText("\nAdditional info: ").appendValue(mismatchTextAddition);
        }
    }

    protected abstract Class<?> getDescribedTypeClass();

}
