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
package org.messaginghub.amqperative.impl;

import java.util.Arrays;

import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.apache.qpid.proton4j.engine.TransactionState;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.messaging.Accepted;
import org.apache.qpid.proton4j.types.messaging.Modified;
import org.apache.qpid.proton4j.types.messaging.Outcome;
import org.apache.qpid.proton4j.types.messaging.Rejected;
import org.apache.qpid.proton4j.types.messaging.Released;
import org.apache.qpid.proton4j.types.messaging.Source;
import org.apache.qpid.proton4j.types.transactions.Coordinator;
import org.apache.qpid.proton4j.types.transactions.TransactionalState;
import org.apache.qpid.proton4j.types.transactions.TxnCapability;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.exceptions.ClientException;
import org.messaginghub.amqperative.exceptions.ClientIllegalStateException;
import org.messaginghub.amqperative.exceptions.ClientTransactionInDoubtException;
import org.messaginghub.amqperative.exceptions.ClientTransactionRolledBackException;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction context used to manage a running transaction within a single {@link Session}
 */
public class ClientTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTransactionContext.class);

    private static final Symbol[] SUPPORTED_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                                      Rejected.DESCRIPTOR_SYMBOL,
                                                                      Released.DESCRIPTOR_SYMBOL,
                                                                      Modified.DESCRIPTOR_SYMBOL };

    private final String DECLARE_FUTURE_NAME = "Declare:Future";
    private final String DISCHARGE_FUTURE_NAME = "Discharge:Future";
    private final String START_TRANSACTION_MARKER = "Transaction:Start";

    private final ClientSession session;

    private Transaction<TransactionController> currentTxn;
    private TransactionController txnController;

    private TransactionalState cachedSenderOutcome;
    private TransactionalState cachedReceiverOutcome;

    public ClientTransactionContext(ClientSession session) {
        this.session = session;
    }

    public void begin(ClientFuture<Session> beginFuture) throws ClientIllegalStateException {
        checkCanBeginNewTransaction();
        beginNewTransaction(beginFuture);
    }

    public void commit(ClientFuture<Session> commitFuture, boolean startNew) throws ClientIllegalStateException {
        checkCanCommitTransaction();

        TransactionController txnController = getOrCreateNewTxnController();

        currentTxn.getAttachments().set(DISCHARGE_FUTURE_NAME, commitFuture);
        currentTxn.getAttachments().set(START_TRANSACTION_MARKER, startNew);

        txnController.registerCapacityAvailableHandler(controller -> {
            txnController.discharge(currentTxn, false);
        });
    }

    public void rollback(ClientFuture<Session> rollbackFuture, boolean startNew) throws ClientIllegalStateException {
        checkCanRollbackTransaction();

        TransactionController txnController = getOrCreateNewTxnController();

        currentTxn.getAttachments().set(DISCHARGE_FUTURE_NAME, rollbackFuture);
        currentTxn.getAttachments().set(START_TRANSACTION_MARKER, startNew);

        txnController.registerCapacityAvailableHandler(controller -> {
            txnController.discharge(currentTxn, false);
        });
    }

    public boolean isInTransaction() {
        return currentTxn != null && currentTxn.getState() == TransactionState.DECLARED;
    }

    public DeliveryState enlistSendInCurrentTransaction(ClientSender seder) {
        if (isInTransaction()) {
            return cachedSenderOutcome != null ?
                cachedSenderOutcome : (cachedSenderOutcome = new TransactionalState().setTxnId(currentTxn.getTxnId()));
        } else {
            return null;
        }
    }

    public DeliveryState enlistAcknowledgeInCurrentTransaction(ClientReceiver receiver, Outcome outcome) {
        if (isInTransaction()) {
            return cachedReceiverOutcome != null ? cachedReceiverOutcome :
                (cachedReceiverOutcome = new TransactionalState().setTxnId(currentTxn.getTxnId()).setOutcome(outcome));
        } else {
            return null;
        }
    }

    private void beginNewTransaction(ClientFuture<Session> beginFuture) {
        TransactionController txnController = getOrCreateNewTxnController();

        currentTxn = txnController.newTransaction();
        currentTxn.setLinkedResource(this);
        currentTxn.getAttachments().set(DECLARE_FUTURE_NAME, beginFuture);

        cachedReceiverOutcome = null;
        cachedSenderOutcome = null;

        txnController.registerCapacityAvailableHandler(controller -> {
            txnController.declare(currentTxn);
        });
    }

    private TransactionController getOrCreateNewTxnController() {
        if (txnController == null) {
            Coordinator coordinator = new Coordinator();
            coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

            Source source = new Source();
            source.setOutcomes(Arrays.copyOf(SUPPORTED_OUTCOMES, SUPPORTED_OUTCOMES.length));

            TransactionController txnController = session.getProtonSession().coordinator("Coordinator:" + session.id());
            txnController.setSource(source)
                         .setCoordinator(coordinator)
                         .declaredHandler(this::handleTransactionDeclared)
                         .declareFailureHandler(this::handleTransactionDeclareFailed)
                         .dischargedHandler(this::handleTransactionDischarged)
                         .dischargeFailureHandler(this::handleTransactionDischargeFailed)
                         .openHandler(this::handleCoordinatorOpen)
                         .closeHandler(this::handleCoordinatorClose)
                         .open();

            this.txnController = txnController;
        }

        return txnController;
    }

    private void checkCanBeginNewTransaction() throws ClientIllegalStateException {
        if (currentTxn != null) {
            switch (currentTxn.getState()) {
                case DISCHARGED:
                    break;
                case DECLARING:
                    throw new ClientIllegalStateException("A transaction is already in the process of being started");
                case DECLARED:
                    throw new ClientIllegalStateException("A transaction is already active in this Session");
                case DISCHARGING:
                    throw new ClientIllegalStateException("A transaction is still being retired and a new one cannot yet be started");
                default:
                    throw new ClientIllegalStateException("Cannot begin a new transaction until the existing transaction completes");
                }
        }
    }

    private void checkCanCommitTransaction() throws ClientIllegalStateException {
        if (currentTxn == null) {
            throw new ClientIllegalStateException("Commit called with no active transaction");
        } else {
            switch (currentTxn.getState()) {
                case DISCHARGED:
                    throw new ClientIllegalStateException("Commit called before transaction declare started.");
                case DECLARING:
                    throw new ClientIllegalStateException("Commit called before transaction declare completed.");
                case DISCHARGING:
                    throw new ClientIllegalStateException("Commit called before transaction discharge completed.");
                case FAILED:
                    throw new ClientIllegalStateException("Commit called on a transaction that has failed due to an error.");
                case IDLE:
                    throw new ClientIllegalStateException("Commit called on a transaction that has not yet been declared");
                default:
                    break;
            }
        }
    }

    private void checkCanRollbackTransaction() throws ClientIllegalStateException {
        if (currentTxn == null) {
            throw new ClientIllegalStateException("Rollback called with no active transaction");
        } else {
            switch (currentTxn.getState()) {
                case DISCHARGED:
                    throw new ClientIllegalStateException("Rollback called before transaction declare started.");
                case DECLARING:
                    throw new ClientIllegalStateException("Rollback called before transaction declare completed.");
                case DISCHARGING:
                    throw new ClientIllegalStateException("Rollback called before transaction discharge completed.");
                case FAILED:
                    throw new ClientIllegalStateException("Rollback called on a transaction that has failed due to an error.");
                case IDLE:
                    throw new ClientIllegalStateException("Rollback called on a transaction that has not yet been declared");
                default:
                    break;
            }
        }
    }

    //----- Handle events from the Transaction Controller

    private void handleTransactionDeclared(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DECLARE_FUTURE_NAME);
        LOG.trace("Declare of trasaction:{} completed", transaction);
        future.complete(session);
    }

    private void handleTransactionDeclareFailed(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DECLARE_FUTURE_NAME);
        LOG.trace("Declare of trasaction:{} failed", transaction);
        ClientException cause = ClientErrorSupport.convertToNonFatalException(transaction.getCondition());
        future.failed(new ClientTransactionInDoubtException(cause.getMessage(), cause));
    }

    private void handleTransactionDischarged(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DISCHARGE_FUTURE_NAME);
        LOG.trace("Discharge of trasaction:{} completed", transaction);
        future.complete(session);

        if (Boolean.TRUE.equals(transaction.getAttachments().get(START_TRANSACTION_MARKER))) {
            beginNewTransaction(future);
        }
    }

    private void handleTransactionDischargeFailed(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DISCHARGE_FUTURE_NAME);
        LOG.trace("Discharge of trasaction:{} failed", transaction);
        ClientException cause = ClientErrorSupport.convertToNonFatalException(transaction.getCondition());
        future.failed(new ClientTransactionRolledBackException(cause.getMessage(), cause));
    }

    private void handleCoordinatorOpen(TransactionController controller) {
        // If remote doesn't set a remote Coordinator then a close is incoming.
        if (controller.getRemoteCoordinator() != null) {
            this.txnController = controller;
        }
    }

    private void handleCoordinatorClose(TransactionController controller) {
        // TODO - Handle closed due to some error

        this.cachedReceiverOutcome = null;
        this.cachedSenderOutcome = null;
        this.txnController = null;
        this.currentTxn = null;
    }
}
