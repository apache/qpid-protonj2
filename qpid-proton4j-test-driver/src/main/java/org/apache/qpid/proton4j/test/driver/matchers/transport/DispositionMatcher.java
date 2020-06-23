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

import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.proton4j.test.driver.codec.transport.Disposition;
import org.apache.qpid.proton4j.test.driver.codec.transport.Role;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class DispositionMatcher extends ListDescribedTypeMatcher {

    public DispositionMatcher() {
        super(Disposition.Field.values().length, Disposition.DESCRIPTOR_CODE, Disposition.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Disposition.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public DispositionMatcher withRole(boolean role) {
        return withRole(equalTo(role));
    }

    public DispositionMatcher withRole(Boolean role) {
        return withRole(equalTo(role));
    }

    public DispositionMatcher withRole(Role role) {
        return withRole(equalTo(role.getValue()));
    }

    public DispositionMatcher withFirst(int first) {
        return withFirst(equalTo(UnsignedInteger.valueOf(first)));
    }

    public DispositionMatcher withFirst(long first) {
        return withFirst(equalTo(UnsignedInteger.valueOf(first)));
    }

    public DispositionMatcher withFirst(UnsignedInteger first) {
        return withFirst(equalTo(first));
    }

    public DispositionMatcher withLast(int last) {
        return withLast(equalTo(UnsignedInteger.valueOf(last)));
    }

    public DispositionMatcher withLast(long last) {
        return withLast(equalTo(UnsignedInteger.valueOf(last)));
    }

    public DispositionMatcher withLast(UnsignedInteger last) {
        return withLast(equalTo(last));
    }

    public DispositionMatcher withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    public DispositionMatcher withState(DeliveryState state) {
        return withState(equalTo(state));
    }

    public DispositionMatcher withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    //----- Matcher based with methods for more complex validation

    public DispositionMatcher withRole(Matcher<?> m) {
        addFieldMatcher(Disposition.Field.ROLE, m);
        return this;
    }

    public DispositionMatcher withFirst(Matcher<?> m) {
        addFieldMatcher(Disposition.Field.FIRST, m);
        return this;
    }

    public DispositionMatcher withLast(Matcher<?> m) {
        addFieldMatcher(Disposition.Field.LAST, m);
        return this;
    }

    public DispositionMatcher withSettled(Matcher<?> m) {
        addFieldMatcher(Disposition.Field.SETTLED, m);
        return this;
    }

    public DispositionMatcher withState(Matcher<?> m) {
        addFieldMatcher(Disposition.Field.STATE, m);
        return this;
    }

    public DispositionMatcher withBatchable(Matcher<?> m) {
        addFieldMatcher(Disposition.Field.BATCHABLE, m);
        return this;
    }
}
