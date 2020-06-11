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
package org.apache.qpid.proton4j.amqp.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Disposition;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.transport.DispositionMatcher;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.Role;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Disposition performative
 */
public class DispositionExpectation extends AbstractExpectation<Disposition> {

    private final DispositionMatcher matcher = new DispositionMatcher();

    public DispositionExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public DispositionExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    //----- Type specific with methods that perform simple equals checks

    public DispositionExpectation withRole(Role role) {
        withRole(equalTo(role.getValue()));
        return this;
    }

    public DispositionExpectation withFirst(int first) {
        return withFirst(equalTo(UnsignedInteger.valueOf(first)));
    }

    public DispositionExpectation withFirst(long first) {
        return withFirst(equalTo(UnsignedInteger.valueOf(first)));
    }

    public DispositionExpectation withFirst(UnsignedInteger first) {
        return withFirst(equalTo(first));
    }

    public DispositionExpectation withLast(int last) {
        return withLast(equalTo(UnsignedInteger.valueOf(last)));
    }

    public DispositionExpectation withLast(long last) {
        return withLast(equalTo(UnsignedInteger.valueOf(last)));
    }

    public DispositionExpectation withLast(UnsignedInteger last) {
        return withLast(equalTo(last));
    }

    public DispositionExpectation withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    public DispositionExpectation withState(DeliveryState state) {
        // TODO - Might want a more generic DescribedTypeMatcher for these.
        return withState(equalTo(TypeMapper.mapFromProtonType(state)));
    }

    public DispositionExpectation withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    //----- Matcher based with methods for more complex validation

    public DispositionExpectation withRole(Matcher<?> m) {
        matcher.addFieldMatcher(Disposition.Field.ROLE, m);
        return this;
    }

    public DispositionExpectation withFirst(Matcher<?> m) {
        matcher.addFieldMatcher(Disposition.Field.FIRST, m);
        return this;
    }

    public DispositionExpectation withLast(Matcher<?> m) {
        matcher.addFieldMatcher(Disposition.Field.LAST, m);
        return this;
    }

    public DispositionExpectation withSettled(Matcher<?> m) {
        matcher.addFieldMatcher(Disposition.Field.SETTLED, m);
        return this;
    }

    public DispositionExpectation withState(Matcher<?> m) {
        matcher.addFieldMatcher(Disposition.Field.STATE, m);
        return this;
    }

    public DispositionExpectation withBatchable(Matcher<?> m) {
        matcher.addFieldMatcher(Disposition.Field.BATCHABLE, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Object getFieldValue(Disposition disposition, Enum<?> performativeField) {
        return disposition.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Disposition.Field.values()[fieldIndex];
    }

    @Override
    protected Class<Disposition> getExpectedTypeClass() {
        return Disposition.class;
    }
}
