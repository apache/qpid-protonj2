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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.transactions.TxnCapability;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction context used to manage a running transaction within a single {@link Session}
 */
public class ClientTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTransactionContext.class);

    private final String DECLARE_FUTURE_NAME = "Declare:Future";
    private final String DISCHARGE_FUTURE_NAME = "Discharge:Future";
    private final String START_TRANSACTION_MARKER = "Transaction:Start";

    private final ClientSession session;

    private Transaction<TransactionController> currentTxn;
    private TransactionController txnController;

    public ClientTransactionContext(ClientSession session) {
        this.session = session;
    }

    public void begin(ClientFuture<Session> beginFuture) {
        TransactionController txnController = getOrCreateNewTxnController();

        txnController.registerCapacityAvailableHandler(controller -> {
            currentTxn = txnController.declare();
            currentTxn.setLinkedResource(this);
            currentTxn.getAttachments().set(DECLARE_FUTURE_NAME, beginFuture);
        });
    }

    public void commit(ClientFuture<Session> commitFuture, boolean startNew) {
        TransactionController txnController = getOrCreateNewTxnController();

        currentTxn.getAttachments().set(DISCHARGE_FUTURE_NAME, commitFuture);
        currentTxn.getAttachments().set(START_TRANSACTION_MARKER, startNew);

        txnController.registerCapacityAvailableHandler(controller -> {
            txnController.discharge(currentTxn, false);
        });
    }

    public void rollback(ClientFuture<Session> rollbackFuture, boolean startNew) {
        TransactionController txnController = getOrCreateNewTxnController();

        currentTxn.getAttachments().set(DISCHARGE_FUTURE_NAME, rollbackFuture);
        currentTxn.getAttachments().set(START_TRANSACTION_MARKER, startNew);

        txnController.registerCapacityAvailableHandler(controller -> {
            txnController.discharge(currentTxn, false);
        });
    }

    private TransactionController getOrCreateNewTxnController() {
        if (txnController == null) {
            Coordinator coordinator = new Coordinator();
            coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

            Symbol[] outcomes = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                               Rejected.DESCRIPTOR_SYMBOL,
                                               Released.DESCRIPTOR_SYMBOL,
                                               Modified.DESCRIPTOR_SYMBOL };

            Source source = new Source();
            source.setOutcomes(outcomes);

            TransactionController txnController = session.getProtonSession().coordinator("Coordinator:" + session.id());
            txnController.setSource(source);
            txnController.setCoordinator(coordinator);

            txnController.declaredHandler(this::handleTransactionDeclared)
                         .declareFailureHandler(this::handleTransactionDeclareFailed)
                         .dischargedHandler(this::handleTransactionDischarged)
                         .dischargeFailureHandler(this::handleTransactionDischargeFailed)
                         .openHandler(this::handleCoordinatorOpen)
                         .closeHandler(this::handleCoordinatorClose)
                         .open();
        }

        return txnController;
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
        // TODO future.failed();
    }

    private void handleTransactionDischarged(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DISCHARGE_FUTURE_NAME);
        LOG.trace("Discharge of trasaction:{} completed", transaction);
        future.complete(session);

        if (Boolean.TRUE.equals(transaction.getAttachments().get(START_TRANSACTION_MARKER))) {
            txnController.registerCapacityAvailableHandler(controller -> {
                currentTxn = txnController.declare();
                currentTxn.setLinkedResource(this);
                currentTxn.getAttachments().set(DECLARE_FUTURE_NAME, future);
            });
        }
    }

    private void handleTransactionDischargeFailed(Transaction<TransactionController> transaction) {
        ClientFuture<Session> future = transaction.getAttachments().get(DECLARE_FUTURE_NAME);
        LOG.trace("Discharge of trasaction:{} failed", transaction);
        // TODO future.failed();
    }

    private void handleCoordinatorOpen(TransactionController controller) {
        // If remote doesn't set a remote Coordinator then a close is incoming.
        if (controller.getRemoteCoordinator() != null) {
            this.txnController = controller;
        }
    }

    private void handleCoordinatorClose(TransactionController controller) {
        this.txnController = null;
    }
}
