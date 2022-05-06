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
package org.apache.qpid.protonj2.client.impl;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionDeclarationException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionNotActiveException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionRolledBackException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Transaction;
import org.apache.qpid.protonj2.engine.Transaction.DischargeState;
import org.apache.qpid.protonj2.engine.TransactionController;
import org.apache.qpid.protonj2.engine.TransactionState;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Outcome;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transactions.TxnCapability;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction context used to manage a running transaction within a single {@link Session}
 */
final class ClientLocalTransactionContext implements ClientTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(ClientLocalTransactionContext.class);

    private static final Symbol[] SUPPORTED_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                                      Rejected.DESCRIPTOR_SYMBOL,
                                                                      Released.DESCRIPTOR_SYMBOL,
                                                                      Modified.DESCRIPTOR_SYMBOL };

    private final String DECLARE_FUTURE_NAME = "Declare:Future";
    private final String DISCHARGE_FUTURE_NAME = "Discharge:Future";
    private final String START_TRANSACTION_MARKER = "Transaction:Start";

    private final AtomicInteger coordinatorCounter = new AtomicInteger();
    private final ClientSession session;

    private Transaction<TransactionController> currentTxn;
    private TransactionController txnController;

    private TransactionalState cachedSenderOutcome;
    private TransactionalState cachedReceiverOutcome;

    ClientLocalTransactionContext(ClientSession session) {
        this.session = session;
    }

    @Override
    public ClientLocalTransactionContext begin(ClientFuture<Session> beginFuture) throws ClientIllegalStateException {
        checkCanBeginNewTransaction();
        beginNewTransaction(beginFuture);
        return this;
    }

    @Override
    public ClientLocalTransactionContext commit(ClientFuture<Session> commitFuture, boolean startNew) throws ClientIllegalStateException {
        checkCanCommitTransaction();

        if (txnController.isLocallyOpen()) {
            currentTxn.getAttachments().set(DISCHARGE_FUTURE_NAME, commitFuture);
            currentTxn.getAttachments().set(START_TRANSACTION_MARKER, startNew);

            if (session.options().requestTimeout() > 0) {
                session.scheduleRequestTimeout(commitFuture, session.options().requestTimeout(), () -> {
                    try {
                        txnController.close();
                    } catch (Exception ignore) {
                    }

                    return new ClientTransactionRolledBackException("Timed out waiting for Transaction commit to complete");
                });
            }

            txnController.addCapacityAvailableHandler(controller -> {
                try {
                    txnController.discharge(currentTxn, false);
                } catch (EngineFailedException efe) {
                    commitFuture.failed(ClientExceptionSupport.createOrPassthroughFatal(efe));
                }
            });
        } else {
            currentTxn = null;
            // The coordinator link closed which amount to a roll back of the declared
            // transaction so we just complete the request as a failure.
            commitFuture.failed(createRolledBackErrorFromClosedCoordinator());
        }

        return this;
    }

    @Override
    public ClientLocalTransactionContext rollback(ClientFuture<Session> rollbackFuture, boolean startNew) throws ClientIllegalStateException {
        checkCanRollbackTransaction();

        if (txnController.isLocallyOpen()) {
            currentTxn.getAttachments().set(DISCHARGE_FUTURE_NAME, rollbackFuture);
            currentTxn.getAttachments().set(START_TRANSACTION_MARKER, startNew);

            if (session.options().requestTimeout() > 0) {
                session.scheduleRequestTimeout(rollbackFuture, session.options().requestTimeout(), () -> {
                    try {
                        txnController.close();
                    } catch (Exception ignore) {
                    }

                    return new ClientOperationTimedOutException("Timed out waiting for Transaction rollback to complete");
                });
            }

            txnController.addCapacityAvailableHandler(controller -> {
                try {
                    txnController.discharge(currentTxn, true);
                } catch (EngineFailedException efe) {
                    // The engine has failed and the connection will be closed so the transaction
                    // is implicitly rolled back on the remote.
                    rollbackFuture.complete(session);
                } catch (Throwable efe) {
                    // Some internal error has occurred and should be communicated as this is not
                    // expected under normal circumstances.
                    rollbackFuture.failed(ClientExceptionSupport.createOrPassthroughFatal(efe));
                }
            });
        } else {
            currentTxn = null;
            // Coordinator was closed after transaction was declared which amounts
            // to a roll back of the transaction so we let this complete as normal.
            rollbackFuture.complete(session);
        }

        return this;
    }

    @Override
    public boolean isInTransaction() {
        return currentTxn != null && currentTxn.getState() == TransactionState.DECLARED;
    }

    @Override
    public boolean isRollbackOnly() {
        if (isInTransaction()) {
            return txnController.isLocallyClosed();
        } else {
            return false;
        }
    }

    @Override
    public ClientTransactionContext send(Sendable sendable, DeliveryState outcome, boolean settled) {
        if (isInTransaction()) {
            if (isRollbackOnly()) {
                sendable.discard();
            } else if (outcome == null) {
                DeliveryState txnOutcome = cachedSenderOutcome != null ?
                    cachedSenderOutcome : (cachedSenderOutcome = new TransactionalState().setTxnId(currentTxn.getTxnId()));
                sendable.send(txnOutcome, settled);
            } else {
                sendable.send(new TransactionalState().setTxnId(currentTxn.getTxnId()).setOutcome((Outcome) outcome), settled);
            }
        } else {
            sendable.send(outcome, settled);
        }

        return this;
    }

    @Override
    public ClientTransactionContext disposition(IncomingDelivery delivery, DeliveryState outcome, boolean settled) {
        if (isInTransaction()) {
            final DeliveryState txnOutcome;
            if (outcome instanceof Accepted) {
                txnOutcome = cachedReceiverOutcome != null ? cachedReceiverOutcome :
                    (cachedReceiverOutcome = new TransactionalState().setTxnId(currentTxn.getTxnId()).setOutcome(Accepted.getInstance()));
            } else {
                txnOutcome = new TransactionalState().setTxnId(currentTxn.getTxnId()).setOutcome((Outcome) outcome);
            }

            delivery.disposition(txnOutcome, true);
        } else {
            delivery.disposition(outcome, settled);
        }

        return this;
    }

    //------ Internals of Transaction State management

    private void beginNewTransaction(ClientFuture<Session> beginFuture) {
        TransactionController txnController = getOrCreateNewTxnController();

        currentTxn = txnController.newTransaction();
        currentTxn.setLinkedResource(this);
        currentTxn.getAttachments().set(DECLARE_FUTURE_NAME, beginFuture);

        cachedReceiverOutcome = null;
        cachedSenderOutcome = null;

        if (session.options().requestTimeout() > 0) {
            session.scheduleRequestTimeout(beginFuture, session.options().requestTimeout(), () -> {
                try {
                    txnController.close();
                } catch (Exception ignore) {
                }

                return new ClientTransactionDeclarationException("Timed out waiting for Transaction declaration to complete");
            });
        }

        txnController.addCapacityAvailableHandler(controller -> {
            try {
                txnController.declare(currentTxn);
            } catch (EngineFailedException efe) {
                beginFuture.failed(ClientExceptionSupport.createOrPassthroughFatal(efe));
            }
        });
    }

    private TransactionController getOrCreateNewTxnController() {
        if (txnController == null || txnController.isLocallyClosed()) {
            Coordinator coordinator = new Coordinator();
            coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

            Source source = new Source();
            source.setOutcomes(Arrays.copyOf(SUPPORTED_OUTCOMES, SUPPORTED_OUTCOMES.length));

            TransactionController txnController = session.getProtonSession().coordinator(nextCoordinatorId());
            txnController.setSource(source)
                         .setCoordinator(coordinator)
                         .declaredHandler(this::handleTransactionDeclared)
                         .declareFailureHandler(this::handleTransactionDeclareFailed)
                         .dischargedHandler(this::handleTransactionDischarged)
                         .dischargeFailureHandler(this::handleTransactionDischargeFailed)
                         .openHandler(this::handleCoordinatorOpen)
                         .closeHandler(this::handleCoordinatorClose)
                         .localCloseHandler(this::handleCoordinatorLocalClose)
                         .parentEndpointClosedHandler(this::handleParentEndpointClosed)
                         .engineShutdownHandler(this::handleEngineShutdown)
                         .open();

            this.txnController = txnController;
        }

        return txnController;
    }

    private void checkCanBeginNewTransaction() throws ClientIllegalStateException {
        if (currentTxn != null) {
            switch (currentTxn.getState()) {
                case DISCHARGED:
                case DISCHARGE_FAILED:
                case DECLARE_FAILED:
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
            throw new ClientTransactionNotActiveException("Commit called with no active transaction");
        } else {
            switch (currentTxn.getState()) {
                case DISCHARGED:
                    throw new ClientTransactionNotActiveException("Commit called with no active transaction");
                case DECLARING:
                    throw new ClientIllegalStateException("Commit called before transaction declare completed.");
                case DISCHARGING:
                    throw new ClientIllegalStateException("Commit called before transaction discharge completed.");
                case DECLARE_FAILED:
                    throw new ClientTransactionNotActiveException("Commit called on a transaction that has failed due to an error during declare.");
                case DISCHARGE_FAILED:
                    throw new ClientTransactionNotActiveException("Commit called on a transaction that has failed due to an error during discharge.");
                case IDLE:
                    throw new ClientTransactionNotActiveException("Commit called on a transaction that has not yet been declared");
                default:
                    break;
            }
        }
    }

    private void checkCanRollbackTransaction() throws ClientIllegalStateException {
        if (currentTxn == null) {
            throw new ClientTransactionNotActiveException("Rollback called with no active transaction");
        } else {
            switch (currentTxn.getState()) {
                case DISCHARGED:
                    throw new ClientTransactionNotActiveException("Rollback called with no active transaction");
                case DECLARING:
                    throw new ClientIllegalStateException("Rollback called before transaction declare completed.");
                case DISCHARGING:
                    throw new ClientIllegalStateException("Rollback called before transaction discharge completed.");
                case DECLARE_FAILED:
                    throw new ClientTransactionNotActiveException("Rollback called on a transaction that has failed due to an error during declare.");
                case DISCHARGE_FAILED:
                    throw new ClientTransactionNotActiveException("Rollback called on a transaction that has failed due to an error during discharge.");
                case IDLE:
                    throw new ClientTransactionNotActiveException("Rollback called on a transaction that has not yet been declared");
                default:
                    break;
            }
        }
    }

    //----- Handle events from the Transaction Controller

    private void handleTransactionDeclared(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DECLARE_FUTURE_NAME);
        LOG.trace("Declare of transaction:{} completed", transaction);

        if (future.isComplete() || future.isCancelled()) {
            // The original declare operation cancelled the future likely due to timeout
            // which means this transaction will never be completed at a higher level so we
            // must discharge it now to ensure the remote can clean up associated resources.
            try {
                rollback(session.getFutureFactory().createFuture(), false);
            } catch (Exception ignore) {}
        } else {
            future.complete(session);
        }
    }

    private void handleTransactionDeclareFailed(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DECLARE_FUTURE_NAME);
        LOG.trace("Declare of transaction:{} failed", transaction);
        ClientException cause = ClientExceptionSupport.convertToNonFatalException(transaction.getCondition());
        future.failed(new ClientTransactionDeclarationException(cause.getMessage(), cause));
    }

    private void handleTransactionDischarged(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DISCHARGE_FUTURE_NAME);
        LOG.trace("Discharge of transaction:{} completed", transaction);
        future.complete(session);

        if (Boolean.TRUE.equals(transaction.getAttachments().get(START_TRANSACTION_MARKER))) {
            beginNewTransaction(future);
        }
    }

    private void handleTransactionDischargeFailed(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DISCHARGE_FUTURE_NAME);
        LOG.trace("Discharge of transaction:{} failed", transaction);
        ClientException cause = ClientExceptionSupport.convertToNonFatalException(transaction.getCondition());
        future.failed(new ClientTransactionRolledBackException(cause.getMessage(), cause));
    }

    private void handleCoordinatorOpen(TransactionController controller) {
        // If remote doesn't set a remote Coordinator then a close is incoming.
        if (controller.getRemoteCoordinator() != null) {
            this.txnController = controller;
        }
    }

    private void handleCoordinatorClose(TransactionController controller) {
        if (txnController != null) {
            txnController.close();
        }
    }

    private ClientTransactionRolledBackException createRolledBackErrorFromClosedCoordinator() {
        ClientException cause = ClientExceptionSupport.convertToNonFatalException(txnController.getRemoteCondition());

        if (!(cause instanceof ClientTransactionRolledBackException)) {
            cause = new ClientTransactionRolledBackException(cause.getMessage(), cause);
        }

        return (ClientTransactionRolledBackException) cause;
    }

    private ClientTransactionDeclarationException createDeclarationErrorFromClosedCoordinator() {
        ClientException cause = ClientExceptionSupport.convertToNonFatalException(txnController.getRemoteCondition());

        if (!(cause instanceof ClientTransactionDeclarationException)) {
            cause = new ClientTransactionDeclarationException(cause.getMessage(), cause);
        }

        return (ClientTransactionDeclarationException) cause;
    }

    private void handleCoordinatorLocalClose(TransactionController controller) {
        // Disconnect from the controllers event points since we could create a new
        // controller if a new transaction is requested by the client.
        controller.declaredHandler(null)
                  .declareFailureHandler(null)
                  .dischargedHandler(null)
                  .dischargeFailureHandler(null)
                  .openHandler(null)
                  .closeHandler(null)
                  .localCloseHandler(null)
                  .parentEndpointClosedHandler(null)
                  .engineShutdownHandler(null);

        if (currentTxn != null) {
            ClientFuture<Session> future = null;

            switch (currentTxn.getState()) {
                case IDLE:
                case DECLARING:
                    future = currentTxn.getAttachments().get(DECLARE_FUTURE_NAME);
                    future.failed(createDeclarationErrorFromClosedCoordinator());
                    currentTxn = null;
                    break;
                case DISCHARGING:
                    future = currentTxn.getAttachments().get(DISCHARGE_FUTURE_NAME);
                    if (currentTxn.getDischargeState() == DischargeState.COMMIT) {
                        future.failed(createRolledBackErrorFromClosedCoordinator());
                    } else {
                        future.complete(session);
                    }
                    currentTxn = null;
                    break;
                default:
                    break;
            }
        }
    }

    private String nextCoordinatorId() {
        return session.id() + ":" + coordinatorCounter.incrementAndGet();
    }

    private void handleParentEndpointClosed(TransactionController txnController) {
        txnController.close();
    }

    private void handleEngineShutdown(Engine engine) {
        if (txnController != null) {
            txnController.close();
        }
    }
}
