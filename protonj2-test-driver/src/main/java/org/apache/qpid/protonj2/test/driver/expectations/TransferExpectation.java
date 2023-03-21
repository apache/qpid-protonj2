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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.LinkTracker;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DispositionInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Modified;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Rejected;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Released;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.TransactionalStateMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

/**
 * Scripted expectation for the AMQP Transfer performative
 */
public class TransferExpectation extends AbstractExpectation<Transfer> {

    private final TransferMatcher matcher = new TransferMatcher();
    private final DeliveryStateBuilder stateBuilder = new DeliveryStateBuilder();

    private Matcher<ByteBuffer> payloadMatcher = Matchers.any(ByteBuffer.class);

    protected DispositionInjectAction response;

    public TransferExpectation(AMQPTestDriver driver) {
        super(driver);

        // Default mandatory field validation.
        withHandle(notNullValue());
    }

    public DispositionInjectAction respond() {
        response = new DispositionInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction accept() {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(Accepted.getInstance());

        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction release() {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(Released.getInstance());

        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction reject() {
        return reject(null);
    }

    public DispositionInjectAction reject(String condition, String description) {
        return reject(new ErrorCondition(Symbol.valueOf(condition), description));
    }

    public DispositionInjectAction reject(Symbol condition, String description) {
        return reject(new ErrorCondition(condition, description));
    }

    public DispositionInjectAction reject(ErrorCondition error) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(new Rejected().setError(error));

        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction modify(boolean failed) {
        return modify(failed, false);
    }

    public DispositionInjectAction modify(boolean failed, boolean undeliverable) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverable));

        driver.addScriptedElement(response);
        return response;
    }

    @Override
    public TransferExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    @Override
    public void handleTransfer(int frameSize, Transfer transfer, ByteBuffer payload, int channel, AMQPTestDriver driver) {
        super.handleTransfer(frameSize, transfer, payload, channel, driver);

        final UnsignedShort remoteChannel = UnsignedShort.valueOf(channel);
        final SessionTracker session = driver.sessions().getSessionFromRemoteChannel(remoteChannel);

        if (session == null) {
            throw new AssertionError(String.format(
                "Received Transfer on channel [%s] that has no matching Session for that remote channel. ", remoteChannel));
        }

        final LinkTracker link = session.handleTransfer(transfer, payload);

        if (response != null) {
            // Input was validated now populate response with auto values where not configured
            // to say otherwise by the test.
            if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
                response.onChannel(link.getSession().getLocalChannel());
            }

            // Populate the fields of the response with defaults if non set by the test script
            if (response.getPerformative().getFirst() == null) {
                response.withFirst(transfer.getDeliveryId());
            }

            if (response.getPerformative().getRole() == null) {
                response.withRole(link.getRole());
            }

            // Remaining response fields should be set by the test script as they can't be inferred.
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public TransferExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public TransferExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public TransferExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public TransferExpectation withDeliveryId(int deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    public TransferExpectation withDeliveryId(long deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    public TransferExpectation withDeliveryId(UnsignedInteger deliveryId) {
        return withDeliveryId(equalTo(deliveryId));
    }

    public TransferExpectation withDeliveryTag(byte[] tag) {
        return withDeliveryTag(new Binary(tag));
    }

    public TransferExpectation withDeliveryTag(Binary deliveryTag) {
        return withDeliveryTag(equalTo(deliveryTag));
    }

    public TransferExpectation withNonNullDeliveryTag() {
        return withDeliveryTag(notNullValue());
    }

    public TransferExpectation withNullDeliveryTag() {
        return withDeliveryTag(nullValue());
    }

    public TransferExpectation withMessageFormat(int messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    public TransferExpectation withMessageFormat(long messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    public TransferExpectation withMessageFormat(UnsignedInteger messageFormat) {
        return withMessageFormat(equalTo(messageFormat));
    }

    public TransferExpectation withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    public TransferExpectation withMore(boolean more) {
        return withMore(equalTo(more));
    }

    public TransferExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode.getValue()));
    }

    public TransferExpectation withState(DeliveryState state) {
        return withState(equalTo(state));
    }

    public DeliveryStateBuilder withState() {
        return stateBuilder;
    }

    public TransferExpectation withNullState() {
        return withState(nullValue());
    }

    public TransferExpectation withResume(boolean resume) {
        return withResume(equalTo(resume));
    }

    public TransferExpectation withAborted(boolean aborted) {
        return withAborted(equalTo(aborted));
    }

    public TransferExpectation withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    public TransferExpectation withNonNullPayload() {
        this.payloadMatcher = notNullValue(ByteBuffer.class);
        return this;
    }

    public TransferExpectation withNullPayload() {
        this.payloadMatcher = nullValue(ByteBuffer.class);
        return this;
    }

    public TransferExpectation withPayload(byte[] buffer) {
        final ByteBuffer copy = ByteBuffer.allocate(buffer.length);
        copy.put(buffer).flip();

        // TODO - Create Matcher which describes the mismatch in detail
        this.payloadMatcher = Matchers.equalTo(copy.asReadOnlyBuffer());
        return this;
    }

    //----- Matcher based with methods for more complex validation

    public TransferExpectation withHandle(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.HANDLE, m);
        return this;
    }

    public TransferExpectation withDeliveryId(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.DELIVERY_ID, m);
        return this;
    }

    public TransferExpectation withDeliveryTag(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.DELIVERY_TAG, m);
        return this;
    }

    public TransferExpectation withMessageFormat(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.MESSAGE_FORMAT, m);
        return this;
    }

    public TransferExpectation withSettled(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.SETTLED, m);
        return this;
    }

    public TransferExpectation withMore(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.MORE, m);
        return this;
    }

    public TransferExpectation withRcvSettleMode(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public TransferExpectation withState(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.STATE, m);
        return this;
    }

    public TransferExpectation withResume(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.RESUME, m);
        return this;
    }

    public TransferExpectation withAborted(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.ABORTED, m);
        return this;
    }

    public TransferExpectation withBatchable(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.BATCHABLE, m);
        return this;
    }

    public TransferExpectation withPayload(Matcher<ByteBuffer> payloadMatcher) {
        this.payloadMatcher = payloadMatcher;
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Matcher<ByteBuffer> getPayloadMatcher() {
        return payloadMatcher;
    }

    @Override
    protected Class<Transfer> getExpectedTypeClass() {
        return Transfer.class;
    }

    public final class DeliveryStateBuilder {

        public TransferExpectation accepted() {
            withState(Accepted.getInstance());
            return TransferExpectation.this;
        }

        public TransferExpectation released() {
            withState(Released.getInstance());
            return TransferExpectation.this;
        }

        public TransferExpectation rejected() {
            withState(new Rejected());
            return TransferExpectation.this;
        }

        public TransferExpectation rejected(String condition, String description) {
            withState(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return TransferExpectation.this;
        }

        public TransferExpectation modified() {
            withState(new Modified());
            return TransferExpectation.this;
        }

        public TransferExpectation modified(boolean failed) {
            withState(new Modified());
            return TransferExpectation.this;
        }

        public TransferExpectation modified(boolean failed, boolean undeliverableHere) {
            withState(new Modified());
            return TransferExpectation.this;
        }

        public TransferTransactionalStateMatcher transactional() {
            TransferTransactionalStateMatcher matcher = new TransferTransactionalStateMatcher(TransferExpectation.this);
            withState(matcher);
            return matcher;
        }
    }

    //----- Extend the TransactionalStateMatcher type to have an API suitable for Transfer expectation setup

    public static class TransferTransactionalStateMatcher extends TransactionalStateMatcher {

        private final TransferExpectation expectation;

        public TransferTransactionalStateMatcher(TransferExpectation expectation) {
            this.expectation = expectation;
        }

        public TransferExpectation also() {
            return expectation;
        }

        public TransferExpectation and() {
            return expectation;
        }

        @Override
        public TransferTransactionalStateMatcher withTxnId(byte[] txnId) {
            super.withTxnId(equalTo(new Binary(txnId)));
            return this;
        }

        @Override
        public TransferTransactionalStateMatcher withTxnId(Binary txnId) {
            super.withTxnId(equalTo(txnId));
            return this;
        }

        @Override
        public TransferTransactionalStateMatcher withOutcome(DeliveryState outcome) {
            super.withOutcome(equalTo(outcome));
            return this;
        }

        //----- Matcher based with methods for more complex validation

        @Override
        public TransferTransactionalStateMatcher withTxnId(Matcher<?> m) {
            super.withOutcome(m);
            return this;
        }

        @Override
        public TransferTransactionalStateMatcher withOutcome(Matcher<?> m) {
            super.withOutcome(m);
            return this;
        }

        // ----- Add a layer to allow configuring the outcome without specific type dependencies

        public TransferTransactionalStateMatcher withAccepted() {
            super.withOutcome(Accepted.getInstance());
            return this;
        }

        public TransferTransactionalStateMatcher withReleased() {
            super.withOutcome(Released.getInstance());
            return this;
        }

        public TransferTransactionalStateMatcher withRejected() {
            super.withOutcome(new Rejected());
            return this;
        }

        public TransferTransactionalStateMatcher withRejected(String condition, String description) {
            super.withOutcome(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return this;
        }

        public TransferTransactionalStateMatcher withModified() {
            super.withOutcome(new Modified());
            return this;
        }

        public TransferTransactionalStateMatcher withModified(boolean failed) {
            super.withOutcome(new Modified().setDeliveryFailed(failed));
            return this;
        }

        public TransferTransactionalStateMatcher withModified(boolean failed, boolean undeliverableHere) {
            super.withOutcome(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere));
            return this;
        }
    }
}
