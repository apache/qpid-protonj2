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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.Disposition;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.AcceptedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.ModifiedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.RejectedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.ReleasedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.DeclaredMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.TransactionalStateMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.DispositionMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.ErrorConditionMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Disposition performative
 */
public class DispositionExpectation extends AbstractExpectation<Disposition> {

    private final DispositionMatcher matcher = new DispositionMatcher();
    private final DeliveryStateBuilder stateBuilder = new DeliveryStateBuilder();

    public DispositionExpectation(AMQPTestDriver driver) {
        super(driver);

        // Default mandatory field validation
        withRole(notNullValue());
        withFirst(notNullValue());
    }

    @Override
    public DispositionExpectation optional() {
        return (DispositionExpectation) super.optional();
    }

    @Override
    public DispositionExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    @Override
    public DispositionExpectation withPredicate(Predicate<Disposition> predicate) {
        super.withPredicate(predicate);
        return this;
    }

    @Override
    public DispositionExpectation withCapture(Consumer<Disposition> capture) {
        super.withCapture(capture);
        return this;
    }

    //----- Handle the incoming Disposition validation and update local side if able

    @Override
    public void handleDisposition(int frameSize, Disposition disposition, ByteBuffer payload, int channel, AMQPTestDriver context) {
        super.handleDisposition(frameSize, disposition, payload, channel, context);

        final UnsignedShort remoteChannel = UnsignedShort.valueOf(channel);
        final SessionTracker session = driver.sessions().getSessionFromRemoteChannel(remoteChannel);

        if (session == null) {
            throw new AssertionError(String.format(
                "Received Disposition on channel [%s] that has no matching Session for that remote channel. ", remoteChannel));
        }

        session.handleDisposition(disposition);
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
            withState(new AcceptedMatcher());
            return DispositionExpectation.this;
        }

        public DispositionExpectation released() {
            withState(new ReleasedMatcher());
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected() {
            withState(new RejectedMatcher());
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(String condition) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Symbol condition) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Matcher<?> condition) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(String condition, String description) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Symbol condition, String description) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(String condition, Matcher<?> description) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Symbol condition, Matcher<?> description) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(String condition, String description, Map<String, Object> info) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Symbol condition, String description, Map<String, Object> info) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(String condition, String description, Matcher<?> info) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Symbol condition, String description, Matcher<?> info) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Matcher<?> condition, Matcher<?> description) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation rejected(Matcher<?> condition, Matcher<?> description, Matcher<?> info) {
            withState(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified() {
            withState(new ModifiedMatcher());
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified(boolean failed) {
            withState(new ModifiedMatcher().withDeliveryFailed(failed));
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified(boolean failed, boolean undeliverableHere) {
            withState(new ModifiedMatcher().withDeliveryFailed(failed).withUndeliverableHere(undeliverableHere));
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified(boolean failed, boolean undeliverableHere, Map<String, Object> annotations) {
            withState(new ModifiedMatcher().withDeliveryFailed(failed).withUndeliverableHere(undeliverableHere).withMessageAnnotations(annotations));
            return DispositionExpectation.this;
        }

        public DispositionExpectation modified(boolean failed, boolean undeliverableHere, Matcher<?> annotations) {
            withState(new ModifiedMatcher().withDeliveryFailed(failed).withUndeliverableHere(undeliverableHere).withMessageAnnotations(annotations));
            return DispositionExpectation.this;
        }

        public DispositionExpectation declared() {
            final byte[] txnId = new byte[4];

            Random rand = new Random();
            rand.setSeed(System.nanoTime());
            rand.nextBytes(txnId);

            withState(new DeclaredMatcher().withTxnId(txnId));

            return DispositionExpectation.this;
        }

        public DispositionExpectation declared(byte[] txnId) {
            withState(new DeclaredMatcher().withTxnId(txnId));
            return DispositionExpectation.this;
        }

        public DispositionTransactionalStateMatcher transactional() {
            DispositionTransactionalStateMatcher matcher = new DispositionTransactionalStateMatcher(DispositionExpectation.this);
            withState(matcher);
            return matcher;
        }
    }

    //----- Extend the TransactionalStateMatcher type to have an API suitable for Transfer expectation setup

    public static class DispositionTransactionalStateMatcher extends TransactionalStateMatcher {

        private final DispositionExpectation expectation;

        public DispositionTransactionalStateMatcher(DispositionExpectation expectation) {
            this.expectation = expectation;
        }

        public DispositionExpectation also() {
            return expectation;
        }

        public DispositionExpectation and() {
            return expectation;
        }

        @Override
        public DispositionTransactionalStateMatcher withTxnId(byte[] txnId) {
            super.withTxnId(equalTo(new Binary(txnId)));
            return this;
        }

        @Override
        public DispositionTransactionalStateMatcher withTxnId(Binary txnId) {
            super.withTxnId(equalTo(txnId));
            return this;
        }

        @Override
        public DispositionTransactionalStateMatcher withOutcome(DeliveryState outcome) {
            super.withOutcome(equalTo(outcome));
            return this;
        }

        //----- Matcher based with methods for more complex validation

        @Override
        public DispositionTransactionalStateMatcher withTxnId(Matcher<?> m) {
            super.withOutcome(m);
            return this;
        }

        @Override
        public DispositionTransactionalStateMatcher withOutcome(Matcher<?> m) {
            super.withOutcome(m);
            return this;
        }

        // ----- Add a layer to allow configuring the outcome without specific type dependencies

        public DispositionTransactionalStateMatcher withAccepted() {
            super.withOutcome(new AcceptedMatcher());
            return this;
        }

        public DispositionTransactionalStateMatcher withReleased() {
            super.withOutcome(new ReleasedMatcher());
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected() {
            super.withOutcome(new RejectedMatcher());
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected(String condition, String description) {
            super.withOutcome(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected(Symbol condition, String description) {
            super.withOutcome(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description)));
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected(String condition, String description, Map<String, Object> info) {
            super.withOutcome(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected(Symbol condition, String description, Map<String, Object> info) {
            super.withOutcome(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected(String condition, String description, Matcher<?> info) {
            super.withOutcome(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return this;
        }

        public DispositionTransactionalStateMatcher withRejected(Symbol condition, String description, Matcher<?> info) {
            super.withOutcome(new RejectedMatcher().withError(new ErrorConditionMatcher().withCondition(condition).withDescription(description).withInfo(info)));
            return this;
        }

        public DispositionTransactionalStateMatcher withModified() {
            super.withOutcome(new ModifiedMatcher());
            return this;
        }

        public DispositionTransactionalStateMatcher withModified(boolean failed) {
            super.withOutcome(new ModifiedMatcher().withDeliveryFailed(failed));
            return this;
        }

        public DispositionTransactionalStateMatcher withModified(boolean failed, boolean undeliverableHere) {
            super.withOutcome(new ModifiedMatcher().withDeliveryFailed(failed).withUndeliverableHere(undeliverableHere));
            return this;
        }

        public DispositionTransactionalStateMatcher withModified(boolean failed, boolean undeliverableHere, Map<String, Object> annotations) {
            super.withOutcome(new ModifiedMatcher().withDeliveryFailed(failed).withUndeliverableHere(undeliverableHere).withMessageAnnotations(annotations));
            return this;
        }

        public DispositionTransactionalStateMatcher withModified(boolean failed, boolean undeliverableHere, Matcher<?> annotations) {
            super.withOutcome(new ModifiedMatcher().withDeliveryFailed(failed).withUndeliverableHere(undeliverableHere).withMessageAnnotations(annotations));
            return this;
        }
    }
}
