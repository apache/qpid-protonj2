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

import java.util.Random;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.actions.DispositionInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Declare;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Declared;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.hamcrest.Matcher;

/**
 * Expectation used to script incoming transaction declarations.
 */
public class DeclareExpectation extends TransferExpectation {

    private final EncodedAmqpValueMatcher defaultPayloadMatcher = new EncodedAmqpValueMatcher(new Declare());

    public DeclareExpectation(AMQPTestDriver driver) {
        super(driver);

        withPayload(defaultPayloadMatcher);
    }

    @Override
    public DispositionInjectAction accept() {
        final byte[] txnId = new byte[4];

        Random rand = new Random();
        rand.setSeed(System.nanoTime());
        rand.nextBytes(txnId);

        return accept(txnId);
    }

    /**
     * Indicates a successful transaction declaration by returning a {@link Declared}
     * disposition with the given transaction Id.
     *
     * @param txnId
     * 		byte array containing the transaction id that has been declared.
     *
     * @return this {@link DispositionInjectAction} instance.
     */
    public DispositionInjectAction accept(byte[] txnId) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        if (txnId != null) {
            response.withState(new Declared().setTxnId(new Binary(txnId)));
        } else {
            response.withState(new Declared());
        }

        driver.addScriptedElement(response);
        return response;
    }

    /**
     * Indicates a successful transaction declaration by returning a {@link Declared}
     * disposition with the given transaction Id.
     *
     * @param txnId
     * 		byte array containing the transaction id that has been declared.
     *
     * @return this {@link DispositionInjectAction} instance.
     */
    public DispositionInjectAction declared(byte[] txnId) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        if (txnId != null) {
            response.withState(new Declared().setTxnId(new Binary(txnId)));
        } else {
            response.withState(new Declared());
        }

        driver.addScriptedElement(response);
        return response;
    }

    //----- Type specific with methods that perform simple equals checks

    @Override
    public DeclareExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DeclareExpectation withDeclare(Declare declare) {
        withPayload(new EncodedAmqpValueMatcher(declare));
        return this;
    }

    public DeclareExpectation withNullDeclare() {
        withPayload(new EncodedAmqpValueMatcher(null));
        return this;
    }

    //----- Type specific with methods that perform simple equals checks

    @Override
    public DeclareExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    @Override
    public DeclareExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    @Override
    public DeclareExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    @Override
    public DeclareExpectation withDeliveryId(int deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    @Override
    public DeclareExpectation withDeliveryId(long deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    @Override
    public DeclareExpectation withDeliveryId(UnsignedInteger deliveryId) {
        return withDeliveryId(equalTo(deliveryId));
    }

    @Override
    public DeclareExpectation withDeliveryTag(byte[] tag) {
        return withDeliveryTag(new Binary(tag));
    }

    @Override
    public DeclareExpectation withDeliveryTag(Binary deliveryTag) {
        return withDeliveryTag(equalTo(deliveryTag));
    }

    @Override
    public DeclareExpectation withMessageFormat(int messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    @Override
    public DeclareExpectation withMessageFormat(long messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    @Override
    public DeclareExpectation withMessageFormat(UnsignedInteger messageFormat) {
        return withMessageFormat(equalTo(messageFormat));
    }

    @Override
    public DeclareExpectation withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    @Override
    public DeclareExpectation withMore(boolean more) {
        return withMore(equalTo(more));
    }

    @Override
    public DeclareExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode.getValue()));
    }

    @Override
    public DeclareExpectation withState(DeliveryState state) {
        return withState(equalTo(state));
    }

    @Override
    public DeclareExpectation withNullState() {
        return withState(nullValue());
    }

    @Override
    public DeclareExpectation withResume(boolean resume) {
        return withResume(equalTo(resume));
    }

    @Override
    public DeclareExpectation withAborted(boolean aborted) {
        return withAborted(equalTo(aborted));
    }

    @Override
    public DeclareExpectation withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    //----- Matcher based with methods for more complex validation

    @Override
    public DeclareExpectation withHandle(Matcher<?> m) {
        super.withHandle(m);
        return this;
    }

    @Override
    public DeclareExpectation withDeliveryId(Matcher<?> m) {
        super.withDeliveryId(m);
        return this;
    }

    @Override
    public DeclareExpectation withDeliveryTag(Matcher<?> m) {
        super.withDeliveryTag(m);
        return this;
    }

    @Override
    public DeclareExpectation withMessageFormat(Matcher<?> m) {
        super.withMessageFormat(m);
        return this;
    }

    @Override
    public DeclareExpectation withSettled(Matcher<?> m) {
        super.withSettled(m);
        return this;
    }

    @Override
    public DeclareExpectation withMore(Matcher<?> m) {
        super.withMore(m);
        return this;
    }

    @Override
    public DeclareExpectation withRcvSettleMode(Matcher<?> m) {
        super.withRcvSettleMode(m);
        return this;
    }

    @Override
    public DeclareExpectation withState(Matcher<?> m) {
        super.withState(m);
        return this;
    }

    @Override
    public DeclareExpectation withResume(Matcher<?> m) {
        super.withResume(m);
        return this;
    }

    @Override
    public DeclareExpectation withAborted(Matcher<?> m) {
        super.withAborted(m);
        return this;
    }

    @Override
    public DeclareExpectation withBatchable(Matcher<?> m) {
        super.withBatchable(m);
        return this;
    }
}
