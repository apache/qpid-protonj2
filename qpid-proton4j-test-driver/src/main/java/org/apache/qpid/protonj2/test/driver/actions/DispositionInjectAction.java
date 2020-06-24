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
package org.apache.qpid.protonj2.test.driver.actions;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Modified;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Rejected;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Released;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.transactions.TransactionalState;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.Disposition;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;

/**
 * AMQP Disposition injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class DispositionInjectAction extends AbstractPerformativeInjectAction<Disposition> {

    private final Disposition disposition = new Disposition();
    private final DeliveryStateBuilder stateBuilder = new DeliveryStateBuilder();

    public DispositionInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public Disposition getPerformative() {
        return disposition;
    }

    public DispositionInjectAction withRole(boolean role) {
        disposition.setRole(role);
        return this;
    }

    public DispositionInjectAction withRole(Boolean role) {
        disposition.setRole(role);
        return this;
    }

    public DispositionInjectAction withRole(Role role) {
        disposition.setRole(role.getValue());
        return this;
    }

    public DispositionInjectAction withFirst(long first) {
        disposition.setFirst(UnsignedInteger.valueOf(first));
        return this;
    }

    public DispositionInjectAction withFirst(UnsignedInteger first) {
        disposition.setFirst(first);
        return this;
    }

    public DispositionInjectAction withLast(long last) {
        disposition.setLast(UnsignedInteger.valueOf(last));
        return this;
    }

    public DispositionInjectAction withLast(UnsignedInteger last) {
        disposition.setLast(last);
        return this;
    }

    public DispositionInjectAction withSettled(boolean settled) {
        disposition.setSettled(settled);
        return this;
    }

    public DeliveryStateBuilder withState() {
        return stateBuilder;
    }

    public DispositionInjectAction withState(DeliveryState state) {
        disposition.setState(state);
        return this;
    }

    public DispositionInjectAction withBatchable(boolean batchable) {
        disposition.setBatchable(batchable);
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }

        // TODO - Process disposition in the local side of the link when needed for added validation
    }

    public final class DeliveryStateBuilder {

        public DispositionInjectAction accepted() {
            withState(Accepted.getInstance());
            return DispositionInjectAction.this;
        }

        public DispositionInjectAction released() {
            withState(Released.getInstance());
            return DispositionInjectAction.this;
        }

        public DispositionInjectAction rejected() {
            withState(new Rejected());
            return DispositionInjectAction.this;
        }

        public DispositionInjectAction rejected(String condition, String description) {
            withState(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return DispositionInjectAction.this;
        }

        public DispositionInjectAction modified() {
            withState(new Modified());
            return DispositionInjectAction.this;
        }

        public DispositionInjectAction modified(boolean failed) {
            withState(new Modified().setDeliveryFailed(failed));
            return DispositionInjectAction.this;
        }

        public DispositionInjectAction modified(boolean failed, boolean undeliverableHere) {
            withState(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere));
            return DispositionInjectAction.this;
        }

        public TransactionalStateBuilder transactional() {
            TransactionalStateBuilder builder = new TransactionalStateBuilder(DispositionInjectAction.this);
            withState(builder.getState());
            return builder;
        }
    }

    //----- Provide a complex builder for Transactional DeliveryState

    public static class TransactionalStateBuilder {

        private final DispositionInjectAction action;
        private final TransactionalState state = new TransactionalState();

        public TransactionalStateBuilder(DispositionInjectAction action) {
            this.action = action;
        }

        public TransactionalState getState() {
            return state;
        }

        public DispositionInjectAction also() {
            return action;
        }

        public DispositionInjectAction and() {
            return action;
        }

        public TransactionalStateBuilder withTxnId(byte[] txnId) {
            state.setTxnId(new Binary(txnId));
            return this;
        }

        public TransactionalStateBuilder withTxnId(Binary txnId) {
            state.setTxnId(txnId);
            return this;
        }

        public TransactionalStateBuilder withOutcome(DeliveryState outcome) {
            state.setOutcome(outcome);
            return this;
        }

        // ----- Add a layer to allow configuring the outcome without specific type dependencies

        public TransactionalStateBuilder withAccepted() {
            withOutcome(Accepted.getInstance());
            return this;
        }

        public TransactionalStateBuilder withReleased() {
            withOutcome(Released.getInstance());
            return this;
        }

        public TransactionalStateBuilder withRejected() {
            withOutcome(new Rejected());
            return this;
        }

        public TransactionalStateBuilder withRejected(String condition, String description) {
            withOutcome(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return this;
        }

        public TransactionalStateBuilder withModified() {
            withOutcome(new Modified());
            return this;
        }

        public TransactionalStateBuilder withModified(boolean failed) {
            withOutcome(new Modified().setDeliveryFailed(failed));
            return this;
        }

        public TransactionalStateBuilder withModified(boolean failed, boolean undeliverableHere) {
            withOutcome(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere));
            return this;
        }
    }
}
