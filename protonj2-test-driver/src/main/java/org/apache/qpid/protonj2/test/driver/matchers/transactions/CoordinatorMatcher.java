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
package org.apache.qpid.protonj2.test.driver.matchers.transactions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class CoordinatorMatcher extends ListDescribedTypeMatcher {

    public CoordinatorMatcher() {
        super(Coordinator.Field.values().length, Coordinator.DESCRIPTOR_CODE, Coordinator.DESCRIPTOR_SYMBOL);
    }

    public CoordinatorMatcher(Coordinator coordinator) {
        super(Coordinator.Field.values().length, Coordinator.DESCRIPTOR_CODE, Coordinator.DESCRIPTOR_SYMBOL);

        addCoordinatorMatchers(coordinator);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Coordinator.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public CoordinatorMatcher withCapabilities(Symbol... capabilities) {
        return withCapabilities(equalTo(capabilities));
    }

    public CoordinatorMatcher withCapabilities(String... capabilities) {
        return withCapabilities(equalTo(TypeMapper.toSymbolArray(capabilities)));
    }

    //----- Matcher based with methods for more complex validation

    public CoordinatorMatcher withCapabilities(Matcher<?> m) {
        addFieldMatcher(Coordinator.Field.CAPABILITIES, m);
        return this;
    }

    //----- Populate the matcher from a given Source object

    private void addCoordinatorMatchers(Coordinator coordinator) {
        if (coordinator.getCapabilities() != null) {
            addFieldMatcher(Coordinator.Field.CAPABILITIES, equalTo(coordinator.getCapabilities()));
        } else {
            addFieldMatcher(Coordinator.Field.CAPABILITIES, nullValue());
        }
    }
}
