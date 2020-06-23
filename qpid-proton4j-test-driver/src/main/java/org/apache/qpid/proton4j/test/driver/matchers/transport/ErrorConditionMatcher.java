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
package org.apache.qpid.proton4j.test.driver.matchers.transport;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class ErrorConditionMatcher extends ListDescribedTypeMatcher {

    public ErrorConditionMatcher() {
        super(ErrorCondition.Field.values().length, ErrorCondition.DESCRIPTOR_CODE, ErrorCondition.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return ErrorCondition.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public ErrorConditionMatcher withCondition(String condition) {
        return withCondition(equalTo(Symbol.valueOf(condition)));
    }

    public ErrorConditionMatcher withCondition(Symbol condition) {
        return withCondition(equalTo(condition));
    }

    public ErrorConditionMatcher withDescription(String description) {
        return withDescription(equalTo(description));
    }

    public ErrorConditionMatcher withInfoMap(Map<Symbol, Object> info) {
        return withInfo(equalTo(info));
    }

    public ErrorConditionMatcher withInfo(Map<String, Object> info) {
        return withInfo(equalTo(TypeMapper.toSymbolKeyedMap(info)));
    }

    //----- Matcher based with methods for more complex validation

    public ErrorConditionMatcher withCondition(Matcher<?> m) {
        addFieldMatcher(ErrorCondition.Field.CONDITION, m);
        return this;
    }

    public ErrorConditionMatcher withDescription(Matcher<?> m) {
        addFieldMatcher(ErrorCondition.Field.DESCRIPTION, m);
        return this;
    }

    public ErrorConditionMatcher withInfo(Matcher<?> m) {
        addFieldMatcher(ErrorCondition.Field.INFO, m);
        return this;
    }
}
