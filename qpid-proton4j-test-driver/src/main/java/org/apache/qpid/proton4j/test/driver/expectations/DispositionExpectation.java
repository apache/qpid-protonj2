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
package org.apache.qpid.proton4j.test.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Accepted;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Modified;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Rejected;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Released;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.proton4j.test.driver.codec.transport.Disposition;
import org.apache.qpid.proton4j.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.test.driver.codec.transport.Role;
import org.apache.qpid.proton4j.test.driver.matchers.transport.DispositionMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Disposition performative
 */
public class DispositionExpectation extends AbstractExpectation<Disposition> {

    private final DispositionMatcher matcher = new DispositionMatcher();
    private final DeliveryStateBuilder stateBuilder = new DeliveryStateBuilder();

    public DispositionExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public DispositionExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    //----- Type specific with methods that perform simple equals checks

    public DispositionExpectation withRole(boolean role) {
        withRole(equalTo(role));
        return this;
    }

    public DispositionExpectation withRole(Boolean role) {
        withRole(equalTo(role));
        return this;
    }

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
        return withState(equalTo(state));
    }

    public DeliveryStateBuilder withState() {
        return stateBuilder;
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
    protected Class<Disposition> getExpectedTypeClass() {
        return Disposition.class;
    }

    public final class DeliveryStateBuilder {

        public DispositionExpectation accepted() {
            withState(Accepted.getInstance());
            return DispositionExpectation.this;
        }

        public DispositionExpectation released() {
            withState(Released.getInstance());
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected() {
            withState(new Rejected());
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(String condition, String description) {
            withState(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified() {
            withState(new Modified());
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified(boolean failed) {
            withState(new Modified());
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified(boolean failed, boolean undeliverableHere) {
            withState(new Modified());
            return DispositionExpectation.this;
        }

        public DispositionExpectation transactional(byte[] txnid) {
            withState(Accepted.getInstance());
            return DispositionExpectation.this;
        }
    }
}
