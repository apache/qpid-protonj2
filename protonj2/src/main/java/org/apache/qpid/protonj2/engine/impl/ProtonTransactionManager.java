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
package org.apache.qpid.protonj2.engine.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Transaction;
import org.apache.qpid.protonj2.engine.Transaction.DischargeState;
import org.apache.qpid.protonj2.engine.TransactionManager;
import org.apache.qpid.protonj2.engine.TransactionState;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.Declare;
import org.apache.qpid.protonj2.types.transactions.Discharge;
import org.apache.qpid.protonj2.types.transactions.TransactionErrors;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * {@link TransactionManager} implementation that implements the abstraction
 * around a receiver link that responds to requests to {@link Declare} and to
 * {@link Discharge} AMQP {@link Transaction} instance.
 */
public final class ProtonTransactionManager extends ProtonEndpoint<TransactionManager> implements TransactionManager {

    private final ProtonReceiver receiverLink;
    private final Decoder payloadDecoder;

    private EventHandler<Transaction<TransactionManager>> declareEventHandler;
    private EventHandler<Transaction<TransactionManager>> dischargeEventHandler;

    private EventHandler<TransactionManager> parentEndpointClosedEventHandler;

    private Map<ProtonBuffer, ProtonManagerTransaction> transactions = new HashMap<>();

    public ProtonTransactionManager(ProtonReceiver receiverLink) {
        super(receiverLink.getEngine());

        this.payloadDecoder = CodecFactory.getDecoder();

        this.receiverLink = receiverLink;
        this.receiverLink.openHandler(this::handleReceiverLinkOpened)
                         .closeHandler(this::handleReceiverLinkClosed)
                         .localOpenHandler(this::handleReceiverLinkLocallyOpened)
                         .localCloseHandler(this::handleReceiverLinkLocallyClosed)
                         .parentEndpointClosedHandler(this::handleParentEndpointClosed)
                         .engineShutdownHandler(this::handleEngineShutdown)
                         .deliveryReadHandler(this::handleDeliveryRead)
                         .deliveryStateUpdatedHandler(this::handleDeliveryStateUpdate);
    }

    @Override
    public ProtonSession getParent() {
        return receiverLink.getSession();
    }

    @Override
    ProtonTransactionManager self() {
        return this;
    }

    @Override
    public TransactionManager addCredit(int additional) {
        receiverLink.addCredit(additional);
        return this;
    }

    @Override
    public int getCredit() {
        return receiverLink.getCredit();
    }

    @Override
    public TransactionManager declared(Transaction<TransactionManager> transaction, Binary txnId) {
        ProtonManagerTransaction txn = (ProtonManagerTransaction) transaction;

        if (txn.parent() != this) {
            throw new IllegalArgumentException("Cannot complete declaration of a transaction from another transaction manager.");
        }

        if (txnId == null || txnId.getArray() == null || txnId.getArray().length == 0) {
            throw new IllegalArgumentException("Cannot declare a transaction without a transaction Id");
        }

        txn.setState(TransactionState.DECLARED);
        txn.setTxnId(txnId);

        // Start tracking this transaction as active.
        transactions.put(txnId.asProtonBuffer(), txn);

        TransactionalState declaration = new TransactionalState();
        declaration.setOutcome(Accepted.getInstance());
        declaration.setTxnId(txnId);

        txn.getDeclare().disposition(declaration, true);

        return this;
    }

    @Override
    public TransactionManager discharged(Transaction<TransactionManager> transaction) {
        ProtonManagerTransaction txn = (ProtonManagerTransaction) transaction;

        // Before sending the disposition remove if from tracking in case the write fails.
        transactions.remove(txn.getTxnId().asProtonBuffer());

        if (txn.parent() != this) {
            throw new IllegalArgumentException("Cannot complete discharge of a transaction from another transaction manager.");
        }

        txn.setState(TransactionState.DISCHARGED);
        txn.getDischarge().disposition(Accepted.getInstance(), true);

        return this;
    }

    @Override
    public TransactionManager declareFailed(Transaction<TransactionManager> transaction, ErrorCondition condition) {
        ProtonManagerTransaction txn = (ProtonManagerTransaction) transaction;

        if (txn.parent() != this) {
            throw new IllegalArgumentException("Cannot fail a declared transaction from another transaction manager.");
        }

        txn.setState(TransactionState.DECLARE_FAILED);
        txn.getDeclare().disposition(new Rejected().setError(condition), true);

        return this;
    }

    @Override
    public TransactionManager dischargeFailed(Transaction<TransactionManager> transaction, ErrorCondition condition) {
        ProtonManagerTransaction txn = (ProtonManagerTransaction) transaction;

        if (txn.parent() != this) {
            throw new IllegalArgumentException("Cannot fail a discharge of a transaction from another transaction manager.");
        }

        transactions.remove(txn.getTxnId().asProtonBuffer());

        // TODO: We should be closing the link if the remote did not report that it supports the
        //       rejected outcome although most don't regardless of what they actually do support.

        txn.setState(TransactionState.DISCHARGE_FAILED);
        txn.getDischarge().disposition(new Rejected().setError(condition), true);

        return this;
    }

    //----- Transaction event APIs

    @Override
    public TransactionManager declareHandler(EventHandler<Transaction<TransactionManager>> declaredEventHandler) {
        this.declareEventHandler = declaredEventHandler;
        return this;
    }

    @Override
    public TransactionManager dischargeHandler(EventHandler<Transaction<TransactionManager>> dischargeEventHandler) {
        this.dischargeEventHandler = dischargeEventHandler;
        return this;
    }

    @Override
    public TransactionManager parentEndpointClosedHandler(EventHandler<TransactionManager> handler) {
        this.parentEndpointClosedEventHandler = handler;
        return this;
    }

    private void fireDeclare(ProtonManagerTransaction transaction) {
        if (declareEventHandler != null) {
            declareEventHandler.handle(transaction);
        }
    }

    private void fireDischarge(ProtonManagerTransaction transaction) {
        if (dischargeEventHandler != null) {
            dischargeEventHandler.handle(transaction);
        }
    }

    private void fireParentEndpointClosed() {
        if (parentEndpointClosedEventHandler != null && isLocallyOpen()) {
            parentEndpointClosedEventHandler.handle(self());
        }
    }

    //----- Hand off methods for link specific elements.

    @Override
    public TransactionManager open() throws IllegalStateException, EngineStateException {
        receiverLink.open();
        return this;
    }

    @Override
    public TransactionManager close() throws EngineFailedException {
        receiverLink.close();
        return this;
    }

    @Override
    public boolean isLocallyOpen() {
        return receiverLink.isLocallyOpen();
    }

    @Override
    public boolean isLocallyClosed() {
        return receiverLink.isLocallyClosed();
    }

    @Override
    public TransactionManager setSource(Source source) throws IllegalStateException {
        receiverLink.setSource(source);
        return this;
    }

    @Override
    public Source getSource() {
        return receiverLink.getSource();
    }

    @Override
    public TransactionManager setCoordinator(Coordinator coordinator) throws IllegalStateException {
        receiverLink.setTarget(coordinator);
        return this;
    }

    @Override
    public Coordinator getCoordinator() {
        return receiverLink.getTarget();
    }

    @Override
    public ErrorCondition getCondition() {
        return receiverLink.getCondition();
    }

    @Override
    public TransactionManager setCondition(ErrorCondition condition) {
        receiverLink.setCondition(condition);
        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        return receiverLink.getProperties();
    }

    @Override
    public TransactionManager setProperties(Map<Symbol, Object> properties) throws IllegalStateException {
        receiverLink.setProperties(properties);
        return this;
    }

    @Override
    public TransactionManager setOfferedCapabilities(Symbol... offeredCapabilities) throws IllegalStateException {
        receiverLink.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        return receiverLink.getOfferedCapabilities();
    }

    @Override
    public TransactionManager setDesiredCapabilities(Symbol... desiredCapabilities) throws IllegalStateException {
        receiverLink.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        return receiverLink.getDesiredCapabilities();
    }

    @Override
    public boolean isRemotelyOpen() {
        return receiverLink.isRemotelyOpen();
    }

    @Override
    public boolean isRemotelyClosed() {
        return receiverLink.isRemotelyClosed();
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        return receiverLink.getRemoteOfferedCapabilities();
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        return receiverLink.getRemoteDesiredCapabilities();
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        return receiverLink.getRemoteProperties();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return receiverLink.getRemoteCondition();
    }

    @Override
    public Source getRemoteSource() {
        return receiverLink.getRemoteSource();
    }

    @Override
    public Coordinator getRemoteCoordinator() {
        return receiverLink.getRemoteTarget();
    }

    //----- Link event handlers

    private void handleReceiverLinkLocallyOpened(Receiver receiver) {
        fireLocalOpen();
    }

    private void handleReceiverLinkLocallyClosed(Receiver receiver) {
        fireLocalClose();
    }

    private void handleReceiverLinkOpened(Receiver receiver) {
        fireRemoteOpen();
    }

    private void handleReceiverLinkClosed(Receiver receiver) {
        fireRemoteClose();
    }

    private void handleEngineShutdown(Engine engine) {
        fireEngineShutdown();
    }

    private void handleParentEndpointClosed(Receiver receiver) {
        fireParentEndpointClosed();
    }

    private void handleDeliveryRead(IncomingDelivery delivery) {
        if (delivery.isAborted()) {
            delivery.settle();
        } else if (!delivery.isPartial()) {
            ProtonBuffer payload = delivery.readAll();

            @SuppressWarnings( "rawtypes" )
            AmqpValue<?> container = (AmqpValue) payloadDecoder.readObject(payload, payloadDecoder.getCachedDecoderState());

            if (container.getValue() instanceof Declare) {
                ProtonManagerTransaction transaction = new ProtonManagerTransaction(this);

                transaction.setDeclare(delivery);
                transaction.setState(TransactionState.DECLARING);

                fireDeclare(transaction);
            } else if (container.getValue() instanceof Discharge) {
                Discharge discharge = (Discharge) container.getValue();
                Binary txnId = discharge.getTxnId();

                ProtonManagerTransaction transaction = transactions.get(txnId.asProtonBuffer());

                if (transaction != null) {
                    transaction.setState(TransactionState.DISCHARGING);
                    transaction.setDischargeState(discharge.getFail() ? DischargeState.ROLLBACK : DischargeState.COMMIT);

                    fireDischarge(transaction.setDischarge(delivery));
                } else {
                    // TODO: If the remote did not indicate it supports reject we should really close the link.
                    ErrorCondition rejection = new ErrorCondition(
                        TransactionErrors.UNKNOWN_ID, "Transaction Manager is not tracking the given transaction ID.");
                    delivery.disposition(new Rejected(rejection), true);
                }
            } else {
                throw new ProtocolViolationException("TXN Coordinator expects Declare and Dishcahrge Delivery payloads only");
            }
        }
    }

    private void handleDeliveryStateUpdate(IncomingDelivery delivery) {
        // Nothing to do yet
    }

    //----- The Manager specific Transaction implementation

    private static final class ProtonManagerTransaction extends ProtonTransaction<TransactionManager> {

        private final ProtonTransactionManager manager;

        private IncomingDelivery declare;
        private IncomingDelivery discharge;

        public ProtonManagerTransaction(ProtonTransactionManager manager) {
            this.manager = manager;
        }

        @Override
        public ProtonTransactionManager parent() {
            return manager;
        }

        public ProtonManagerTransaction setDeclare(IncomingDelivery delivery) {
            this.declare = delivery;
            return this;
        }

        public IncomingDelivery getDeclare() {
            return declare;
        }

        public ProtonManagerTransaction setDischarge(IncomingDelivery delivery) {
            this.discharge = delivery;
            return this;
        }

        public IncomingDelivery getDischarge() {
            return discharge;
        }
    }
}
