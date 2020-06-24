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
package org.apache.qpid.protonj2.test.driver.matchers.transport;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class EndMatcher extends ListDescribedTypeMatcher {

    public EndMatcher() {
        super(End.Field.values().length, End.DESCRIPTOR_CODE, End.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return End.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public EndMatcher withError(ErrorCondition error) {
        return withError(equalTo(error));
    }

    public EndMatcher withError(String condition, String description) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description)));
    }

    public EndMatcher withError(String condition, String description, Map<String, Object> info) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
    }

    public EndMatcher withError(Symbol condition, String description) {
        return withError(equalTo(new ErrorCondition(condition, description)));
    }

    public EndMatcher withError(Symbol condition, String description, Map<Symbol, Object> info) {
        return withError(equalTo(new ErrorCondition(condition, description, info)));
    }

    //----- Matcher based with methods for more complex validation

    public EndMatcher withError(Matcher<?> m) {
        addFieldMatcher(End.Field.ERROR, m);
        return this;
    }
}
