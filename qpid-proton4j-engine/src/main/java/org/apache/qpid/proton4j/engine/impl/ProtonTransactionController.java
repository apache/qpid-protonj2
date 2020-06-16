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
package org.apache.qpid.proton4j.engine.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.apache.qpid.proton4j.engine.TransactionState;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.logging.ProtonLogger;
import org.apache.qpid.proton4j.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.messaging.AmqpValue;
import org.apache.qpid.proton4j.types.messaging.Rejected;
import org.apache.qpid.proton4j.types.messaging.Source;
import org.apache.qpid.proton4j.types.transactions.Coordinator;
import org.apache.qpid.proton4j.types.transactions.Declare;
import org.apache.qpid.proton4j.types.transactions.Declared;
import org.apache.qpid.proton4j.types.transactions.Discharge;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.DeliveryState.DeliveryStateType;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;

/**
 * {@link TransactionController} implementation that implements the abstraction
 * around a sender link that initiates requests to {@link Declare} and to
 * {@link Discharge} AMQP {@link Transaction} instance.
 */
public class ProtonTransactionController extends ProtonEndpoint<TransactionController> implements TransactionController {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonTransactionController.class);

    private static final ProtonBuffer ENCODED_DECLARE;

    static {
        Encoder declareEncoder = CodecFactory.getEncoder();
        EncoderState state = declareEncoder.newEncoderState();

        ENCODED_DECLARE = ProtonByteBufferAllocator.DEFAULT.allocate();

        try {
            declareEncoder.writeObject(ENCODED_DECLARE, state, new AmqpValue(new Declare()));
        } finally {
            state.reset();
        }
    }

    private final ProtonSender senderLink;
    private final Encoder commandEncoder = CodecFactory.getEncoder();
    private final ProtonBuffer encoding = ProtonByteBufferAllocator.DEFAULT.allocate();

    private EventHandler<Transaction<TransactionController>> declaredEventHandler;
    private EventHandler<Transaction<TransactionController>> declareFailureEventHandler;
    private EventHandler<Transaction<TransactionController>> dischargedEventHandler;
    private EventHandler<Transaction<TransactionController>> dischargeFailureEventHandler;

    private List<EventHandler<TransactionController>> capacityObservers = new ArrayList<>();

    public ProtonTransactionController(ProtonSender senderLink) {
        super(senderLink.getEngine());

        this.senderLink = senderLink;
        this.senderLink.deliveryStateUpdatedHandler(this::handleDeliveryRemotelyUpdated)
                       .creditStateUpdateHandler(this::handleLinkCreditUpdated)
                       .openHandler(this::handleSenderLinkOpened)
                       .closeHandler(this::handleSenderLinkClosed)
                       .localOpenHandler(this::handleSenderLinkLocallyOpened)
                       .localCloseHandler(this::handleSenderLinkLocallyClosed)
                       .engineShutdownHandler(this::handleEngineShutdown);
    }

    @Override
    public ProtonSession getParent() {
        return senderLink.getSession();
    }

    @Override
    ProtonTransactionController self() {
        return this;
    }

    @Override
    public boolean hasCapacity() {
        return senderLink.isSendable();
    }

    @Override
    public ProtonTransactionController registerCapacityAvailableHandler(EventHandler<TransactionController> handler) {
        if (hasCapacity()) {
            handler.handle(this);
        } else {
            capacityObservers.add(handler);
        }

        return this;
    }


    @Override
    public Transaction<TransactionController> newTransaction() {
        return new ProtonControllerTransaction(this);
    }

    @Override
    public TransactionController declare(Transaction<TransactionController> transaction) {
        if (!senderLink.isSendable()) {
            throw new IllegalStateException("Cannot Declare due to current capicity restrictions.");
        }

        if (transaction.getState() != TransactionState.IDLE) {
            throw new IllegalStateException("Cannot declare a transaction that has already been used previously");
        }

        if (transaction.parent() != this) {
            throw new IllegalArgumentException("Cannot declare a transaction that was created by another controller.");
        }

        ProtonControllerTransaction protonTransaction = (ProtonControllerTransaction) transaction;

        protonTransaction.setState(TransactionState.DECLARING);

        OutgoingDelivery command = senderLink.next();

        command.setLinkedResource(protonTransaction);
        try {
            command.writeBytes(ENCODED_DECLARE);
        } finally {
            ENCODED_DECLARE.setReadIndex(0);
        }

        return this;
    }

    @Override
    public Transaction<TransactionController> declare() {
        if (!senderLink.isSendable()) {
            throw new IllegalStateException("Cannot Declare due to current capicity restrictions.");
        }

        ProtonControllerTransaction transaction = new ProtonControllerTransaction(this);
        declare(transaction);

        return transaction;
    }

    @Override
    public TransactionController discharge(Transaction<TransactionController> transaction, boolean failed) {
        if (transaction.getState() != TransactionState.DECLARED) {
            throw new IllegalStateException("Cannot discharge a transaction that is not currently actively declared.");
        }

        if (transaction.parent() != this) {
            throw new IllegalArgumentException("Cannot discharge a transaction that was created by another controller.");
        }

        ((ProtonTransaction<TransactionController>) transaction).setState(TransactionState.DISCHARGING);

        Discharge discharge = new Discharge();
        discharge.setFail(failed);
        discharge.setTxnId(transaction.getTxnId());

        commandEncoder.writeObject(encoding.clear(), commandEncoder.getCachedEncoderState(), new AmqpValue(discharge));

        OutgoingDelivery command = senderLink.next();
        command.setLinkedResource(transaction);
        command.writeBytes(encoding);

        return this;
    }

    @Override
    public TransactionController declaredHandler(EventHandler<Transaction<TransactionController>> declaredEventHandler) {
        this.declaredEventHandler = declaredEventHandler;
        return this;
    }

    @Override
    public TransactionController declareFailureHandler(EventHandler<Transaction<TransactionController>> declareFailureEventHandler) {
        this.declareFailureEventHandler = declareFailureEventHandler;
        return this;
    }

    @Override
    public TransactionController dischargedHandler(EventHandler<Transaction<TransactionController>> dischargedEventHandler) {
        this.dischargedEventHandler = dischargedEventHandler;
        return this;
    }

    @Override
    public TransactionController dischargeFailureHandler(EventHandler<Transaction<TransactionController>> dischargeFailureEventHandler) {
        this.dischargeFailureEventHandler = dischargeFailureEventHandler;
        return this;
    }

    private void fireDeclaredEvent(ProtonControllerTransaction transaction) {
        if (declaredEventHandler != null) {
            declaredEventHandler.handle(transaction);
        } else {
            LOG.debug("Transaction {} declared successfully but no handler registered to signal result", transaction);
        }
    }

    private void fireDeclareFailureEvent(ProtonControllerTransaction transaction) {
        if (declareFailureEventHandler != null) {
            declareFailureEventHandler.handle(transaction);
        } else {
            LOG.debug("Transaction {} declare failed but no handler registered to signal result", transaction);
        }
    }

    private void fireDischargedEvent(ProtonControllerTransaction transaction) {
        if (dischargedEventHandler != null) {
            dischargedEventHandler.handle(transaction);
        } else {
            LOG.debug("Transaction {} discharged successfully but no handler registered to signal result", transaction);
        }
    }

    private void fireDischargeFailureEvent(ProtonControllerTransaction transaction) {
        if (dischargeFailureEventHandler != null) {
            dischargeFailureEventHandler.handle(transaction);
        } else {
            LOG.debug("Transaction {} discharge failed but no handler registered to signal result", transaction);
        }
    }

    //----- Hand off methods for link specific elements.

    @Override
    public TransactionController open() throws IllegalStateException, EngineStateException {
        senderLink.open();
        return this;
    }

    @Override
    public TransactionController close() throws EngineFailedException {
        senderLink.close();
        return this;
    }

    @Override
    public boolean isLocallyOpen() {
        senderLink.isLocallyOpen();
        return false;
    }

    @Override
    public boolean isLocallyClosed() {
        senderLink.isLocallyClosed();
        return false;
    }

    @Override
    public TransactionController setSource(Source source) throws IllegalStateException {
        senderLink.setSource(source);
        return this;
    }

    @Override
    public Source getSource() {
        return senderLink.getSource();
    }

    @Override
    public TransactionController setCoordinator(Coordinator coordinator) throws IllegalStateException {
        senderLink.setTarget(coordinator);
        return this;
    }

    @Override
    public Coordinator getCoordinator() {
        return senderLink.getTarget();
    }

    @Override
    public ErrorCondition getCondition() {
        return senderLink.getCondition();
    }

    @Override
    public TransactionController setCondition(ErrorCondition condition) {
        senderLink.setCondition(condition);
        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        return senderLink.getProperties();
    }

    @Override
    public TransactionController setProperties(Map<Symbol, Object> properties) throws IllegalStateException {
        senderLink.setProperties(properties);
        return this;
    }

    @Override
    public TransactionController setOfferedCapabilities(Symbol... offeredCapabilities) throws IllegalStateException {
        senderLink.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        return senderLink.getOfferedCapabilities();
    }

    @Override
    public TransactionController setDesiredCapabilities(Symbol... desiredCapabilities) throws IllegalStateException {
        senderLink.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        return senderLink.getDesiredCapabilities();
    }

    @Override
    public boolean isRemotelyOpen() {
        return senderLink.isRemotelyOpen();
    }

    @Override
    public boolean isRemotelyClosed() {
        return senderLink.isRemotelyClosed();
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        return senderLink.getRemoteOfferedCapabilities();
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        return senderLink.getRemoteDesiredCapabilities();
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        return senderLink.getRemoteProperties();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return senderLink.getRemoteCondition();
    }

    @Override
    public Source getRemoteSource() {
        return senderLink.getRemoteSource();
    }

    @Override
    public Coordinator getRemoteCoordinator() {
        return senderLink.getRemoteTarget();
    }

    //----- Link event handlers

    private void handleSenderLinkLocallyOpened(Sender sender) {
        fireLocalOpen();
    }

    private void handleSenderLinkLocallyClosed(Sender sender) {
        fireLocalClose();
    }

    private void handleSenderLinkOpened(Sender sender) {
        fireRemoteOpen();
    }

    private void handleSenderLinkClosed(Sender sender) {
        fireRemoteClose();
    }

    private void handleEngineShutdown(Engine engine) {
        fireEngineShutdown();
    }

    private void handleLinkCreditUpdated(Sender sender) {
        if (sender.isSendable()) {
            // Remove all that can be invoked and leave the rest in place for next credit update.
            capacityObservers.removeIf(handler -> {
                if (hasCapacity()) {
                    handler.handle(this);
                    return true;
                }

                return false;
            });
        }

        if (sender.isDraining()) {
            sender.drained();
        }
    }

    private void handleDeliveryRemotelyUpdated(OutgoingDelivery delivery) {
        ProtonControllerTransaction transaction = delivery.getLinkedResource();

        DeliveryState state = delivery.getRemoteState();
        TransactionState transactionState = transaction.getState();

        // TODO: Check error states for disposition outside of expected state boundaries
        //       and deal with settlement after delivery state sent as unexpected but possible
        //       case of responding to a requested transaction command..

        try {
            switch (state.getType()) {
                case Declared:
                    Declared declared = (Declared) state;
                    transaction.setState(TransactionState.DECLARED);
                    transaction.setTxnId(declared.getTxnId());
                    fireDeclaredEvent(transaction);
                    break;
                case Accepted:
                    transaction.setState(TransactionState.DISCHARGED);
                    fireDischargedEvent(transaction);
                    break;
                default:
                    if (state.getType() == DeliveryStateType.Rejected) {
                        Rejected rejected = (Rejected) state;
                        transaction.setCondition(rejected.getError());
                    }

                    if (transactionState == TransactionState.DECLARING) {
                        transaction.setState(TransactionState.DECLARE_FAILED);
                        fireDeclareFailureEvent(transaction);
                    } else {
                        transaction.setState(TransactionState.DISCHARGE_FAILED);
                        fireDischargeFailureEvent(transaction);
                    }

                    break;
            }
        } finally {
            delivery.settle();
        }
    }

    //----- The Controller specific Transaction implementation

    private final class ProtonControllerTransaction extends ProtonTransaction<TransactionController> implements Transaction<TransactionController> {

        private final ProtonTransactionController controller;

        public ProtonControllerTransaction(ProtonTransactionController controller) {
            this.controller = controller;
        }

        @Override
        public ProtonTransactionController parent() {
            return controller;
        }
    }
}
