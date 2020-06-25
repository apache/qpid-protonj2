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
package org.apache.qpid.protonj2.test.driver.matchers.messaging;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.protonj2.test.driver.codec.messaging.Received;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class ReceivedMatcher extends ListDescribedTypeMatcher {

    public ReceivedMatcher() {
        super(Received.Field.values().length, Received.DESCRIPTOR_CODE, Received.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Received.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public ReceivedMatcher withSectionNumber(int sectionNo) {
        return withSectionNumber(equalTo(UnsignedInteger.valueOf(sectionNo)));
    }

    public ReceivedMatcher withSectionNumber(long sectionNo) {
        return withSectionNumber(equalTo(UnsignedInteger.valueOf(sectionNo)));
    }

    public ReceivedMatcher withSectionNumber(UnsignedInteger sectionNo) {
        return withSectionNumber(equalTo(sectionNo));
    }

    public ReceivedMatcher withSectionOffset(int sectionOffset) {
        return withSectionOffset(equalTo(UnsignedLong.valueOf(sectionOffset)));
    }

    public ReceivedMatcher withSectionOffset(long sectionOffset) {
        return withSectionOffset(equalTo(UnsignedLong.valueOf(sectionOffset)));
    }

    public ReceivedMatcher withSectionOffset(UnsignedLong sectionOffset) {
        return withSectionOffset(equalTo(sectionOffset));
    }

    //----- Matcher based with methods for more complex validation

    public ReceivedMatcher withSectionNumber(Matcher<?> m) {
        addFieldMatcher(Received.Field.SECTION_NUMBER, m);
        return this;
    }

    public ReceivedMatcher withSectionOffset(Matcher<?> m) {
        addFieldMatcher(Received.Field.SECTION_OFFSET, m);
        return this;
    }
}
