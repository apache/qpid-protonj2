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
package org.apache.qpid.protonj2.test.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Discharge;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.DischargeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.hamcrest.Matcher;

/**
 * Expectation used to script incoming transaction declarations.
 */
public class DischargeExpectation extends TransferExpectation {

    private DischargeMatcher discharge = new DischargeMatcher();
    private EncodedAmqpValueMatcher matcher = new EncodedAmqpValueMatcher(discharge);

    public DischargeExpectation(AMQPTestDriver driver) {
        super(driver);

        withPayload(matcher);
    }

    @Override
    public DischargeExpectation optional() {
        return (DischargeExpectation) super.optional();
    }

    @Override
    public DischargeExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DischargeExpectation withFail(boolean fail) {
        discharge.withFail(fail);
        return this;
    }

    public DischargeExpectation withTxnId(byte[] txnId) {
        discharge.withTxnId(new Binary(txnId));
        return this;
    }

    public DischargeExpectation withTxnId(Binary txnId) {
        discharge.withTxnId(txnId);
        return this;
    }

    public DischargeExpectation withDischarge(Discharge discharge) {
        withPayload(new EncodedAmqpValueMatcher(discharge));
        return this;
    }

    public DischargeExpectation withNullDischarge() {
        withPayload(new EncodedAmqpValueMatcher(null));
        return this;
    }

    //----- Type specific with methods that perform simple equals checks

    @Override
    public DischargeExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    @Override
    public DischargeExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    @Override
    public DischargeExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    @Override
    public DischargeExpectation withDeliveryId(int deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    @Override
    public DischargeExpectation withDeliveryId(long deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    @Override
    public DischargeExpectation withDeliveryId(UnsignedInteger deliveryId) {
        return withDeliveryId(equalTo(deliveryId));
    }

    @Override
    public DischargeExpectation withDeliveryTag(byte[] tag) {
        return withDeliveryTag(new Binary(tag));
    }

    @Override
    public DischargeExpectation withDeliveryTag(Binary deliveryTag) {
        return withDeliveryTag(equalTo(deliveryTag));
    }

    @Override
    public DischargeExpectation withMessageFormat(int messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    @Override
    public DischargeExpectation withMessageFormat(long messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    @Override
    public DischargeExpectation withMessageFormat(UnsignedInteger messageFormat) {
        return withMessageFormat(equalTo(messageFormat));
    }

    @Override
    public DischargeExpectation withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    @Override
    public DischargeExpectation withMore(boolean more) {
        return withMore(equalTo(more));
    }

    @Override
    public DischargeExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode.getValue()));
    }

    @Override
    public DischargeExpectation withState(DeliveryState state) {
        return withState(equalTo(state));
    }

    @Override
    public DischargeExpectation withNullState() {
        return withState(nullValue());
    }

    @Override
    public DischargeExpectation withResume(boolean resume) {
        return withResume(equalTo(resume));
    }

    @Override
    public DischargeExpectation withAborted(boolean aborted) {
        return withAborted(equalTo(aborted));
    }

    @Override
    public DischargeExpectation withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    //----- Matcher based with methods for more complex validation

    @Override
    public DischargeExpectation withHandle(Matcher<?> m) {
        super.withHandle(m);
        return this;
    }

    @Override
    public DischargeExpectation withDeliveryId(Matcher<?> m) {
        super.withDeliveryId(m);
        return this;
    }

    @Override
    public DischargeExpectation withDeliveryTag(Matcher<?> m) {
        super.withDeliveryTag(m);
        return this;
    }

    @Override
    public DischargeExpectation withMessageFormat(Matcher<?> m) {
        super.withMessageFormat(m);
        return this;
    }

    @Override
    public DischargeExpectation withSettled(Matcher<?> m) {
        super.withSettled(m);
        return this;
    }

    @Override
    public DischargeExpectation withMore(Matcher<?> m) {
        super.withMore(m);
        return this;
    }

    @Override
    public DischargeExpectation withRcvSettleMode(Matcher<?> m) {
        super.withRcvSettleMode(m);
        return this;
    }

    @Override
    public DischargeExpectation withState(Matcher<?> m) {
        super.withState(m);
        return this;
    }

    @Override
    public DischargeExpectation withResume(Matcher<?> m) {
        super.withResume(m);
        return this;
    }

    @Override
    public DischargeExpectation withAborted(Matcher<?> m) {
        super.withAborted(m);
        return this;
    }

    @Override
    public DischargeExpectation withBatchable(Matcher<?> m) {
        super.withBatchable(m);
        return this;
    }
}
