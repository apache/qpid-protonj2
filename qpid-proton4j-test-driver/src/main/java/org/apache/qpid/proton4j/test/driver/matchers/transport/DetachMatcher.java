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
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.Detach;
import org.apache.qpid.proton4j.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class DetachMatcher extends ListDescribedTypeMatcher {

    public DetachMatcher() {
        super(Detach.Field.values().length, Detach.DESCRIPTOR_CODE, Detach.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Detach.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public DetachMatcher withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public DetachMatcher withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public DetachMatcher withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public DetachMatcher withClosed(boolean closed) {
        return withClosed(equalTo(closed));
    }

    public DetachMatcher withError(ErrorCondition error) {
        return withError(equalTo(error));
    }

    public DetachMatcher withError(String condition) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition))));
    }

    public DetachMatcher withError(String condition, String description) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description)));
    }

    public DetachMatcher withError(String condition, String description, Map<String, Object> info) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
    }

    public DetachMatcher withError(Symbol condition, String description) {
        return withError(equalTo(new ErrorCondition(condition, description)));
    }

    public DetachMatcher withError(Symbol condition, String description, Map<Symbol, Object> info) {
        return withError(equalTo(new ErrorCondition(condition, description, info)));
    }

    //----- Matcher based with methods for more complex validation

    public DetachMatcher withHandle(Matcher<?> m) {
        addFieldMatcher(Detach.Field.HANDLE, m);
        return this;
    }

    public DetachMatcher withClosed(Matcher<?> m) {
        addFieldMatcher(Detach.Field.CLOSED, m);
        return this;
    }

    public DetachMatcher withError(Matcher<?> m) {
        addFieldMatcher(Detach.Field.ERROR, m);
        return this;
    }
}
