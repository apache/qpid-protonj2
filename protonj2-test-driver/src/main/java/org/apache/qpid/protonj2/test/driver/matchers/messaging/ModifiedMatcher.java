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

import org.apache.qpid.protonj2.test.driver.codec.messaging.Modified;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class ModifiedMatcher extends ListDescribedTypeMatcher {

    public ModifiedMatcher() {
        super(Modified.Field.values().length, Modified.DESCRIPTOR_CODE, Modified.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Modified.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public ModifiedMatcher withDeliveryFailed(boolean deliveryFailed) {
        return withDeliveryFailed(equalTo(deliveryFailed));
    }

    public ModifiedMatcher withDeliveryFailed(Boolean deliveryFailed) {
        return withDeliveryFailed(equalTo(deliveryFailed));
    }

    public ModifiedMatcher withUndeliverableHere(boolean undeliverableHere) {
        return withUndeliverableHere(equalTo(undeliverableHere));
    }

    public ModifiedMatcher withUndeliverableHere(Boolean undeliverableHere) {
        return withUndeliverableHere(equalTo(undeliverableHere));
    }

    public ModifiedMatcher withMessageAnnotationsMap(Map<Symbol, Object> sectionNo) {
        return withMessageAnnotations(equalTo(sectionNo));
    }

    public ModifiedMatcher withMessageAnnotations(Map<String, Object> sectionNo) {
        return withMessageAnnotations(equalTo(TypeMapper.toSymbolKeyedMap(sectionNo)));
    }

    //----- Matcher based with methods for more complex validation

    public ModifiedMatcher withDeliveryFailed(Matcher<?> m) {
        addFieldMatcher(Modified.Field.DELIVERY_FAILED, m);
        return this;
    }

    public ModifiedMatcher withUndeliverableHere(Matcher<?> m) {
        addFieldMatcher(Modified.Field.UNDELIVERABLE_HERE, m);
        return this;
    }

    public ModifiedMatcher withMessageAnnotations(Matcher<?> m) {
        addFieldMatcher(Modified.Field.MESSAGE_ANNOTATIONS, m);
        return this;
    }
}
