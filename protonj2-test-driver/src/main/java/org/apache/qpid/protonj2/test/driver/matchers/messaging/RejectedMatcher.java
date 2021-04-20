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

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.messaging.Rejected;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.ErrorConditionMatcher;
import org.hamcrest.Matcher;

public class RejectedMatcher extends ListDescribedTypeMatcher {

    public RejectedMatcher() {
        super(Rejected.Field.values().length, Rejected.DESCRIPTOR_CODE, Rejected.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Rejected.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public RejectedMatcher withError(ErrorCondition error) {
        return withError(equalTo(error));
    }

    public RejectedMatcher withError(String condition) {
        return withError(new ErrorConditionMatcher().withCondition(condition));
    }

    public RejectedMatcher withError(Symbol condition) {
        return withError(new ErrorConditionMatcher().withCondition(condition));
    }

    public RejectedMatcher withError(String condition, String description) {
        return withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description));
    }

    public RejectedMatcher withError(String condition, String description, Map<String, Object> info) {
        return withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info));
    }

    public RejectedMatcher withError(Symbol condition, String description, Map<Symbol, Object> info) {
        return withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfoMap(info));
    }

    //----- Matcher based with methods for more complex validation

    public RejectedMatcher withError(Matcher<?> m) {
        addFieldMatcher(Rejected.Field.ERROR, m);
        return this;
    }
}
