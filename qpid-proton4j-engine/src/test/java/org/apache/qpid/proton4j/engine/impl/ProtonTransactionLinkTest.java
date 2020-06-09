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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.transactions.TransactionErrors;
import org.apache.qpid.proton4j.amqp.transactions.TxnCapability;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.apache.qpid.proton4j.engine.TransactionManager;
import org.apache.qpid.proton4j.engine.TransactionState;
import org.junit.Test;

/**
 * Tests for AMQP transaction over normal {@link Sender} and {@link Receiver} links.
 */
public class ProtonTransactionLinkTest extends ProtonEngineTestSupport {

    private Symbol[] DEFAULT_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                       Rejected.DESCRIPTOR_SYMBOL,
                                                       Released.DESCRIPTOR_SYMBOL,
                                                       Modified.DESCRIPTOR_SYMBOL };

    @Test(timeout = 20_000)
    public void testCreateDefaultCoordinatorSender() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        Source source = new Source();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectCoordinatorAttach().respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test-coordinator");

        sender.setSource(source);
        sender.setTarget(coordinator);

        sender.open();
        sender.detach();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testCreateCoordinatorSender() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource(source).withTarget(coordinator).respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test-coordinator");

        sender.setSource(source);
        sender.setTarget(coordinator);

        final AtomicBoolean openedWithCoordinatorTarget = new AtomicBoolean();
        sender.openHandler(result -> {
            if (result.getRemoteTarget() instanceof Coordinator) {
                openedWithCoordinatorTarget.set(true);
            }
        });

        sender.open();

        assertTrue(openedWithCoordinatorTarget.get());

        Coordinator remoteCoordinator = sender.getRemoteTarget();

        assertEquals(TxnCapability.LOCAL_TXN, remoteCoordinator.getCapabilities()[0]);

        sender.detach();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testRemoteCoordinatorSenderSignalsTransactionManagerFromSessionWhenEnabled() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .withRole(Role.SENDER)
                           .withSource(source)
                           .withInitialDeliveryCount(0)
                           .withTarget(coordinator).queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER)
                           .withSource(source)
                           .withTarget(coordinator);
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        TransactionManager manager = transactionManager.get();

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testRemoteCoordinatorSenderSignalsTransactionManagerFromConnectionWhenEnabled() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .withRole(Role.SENDER)
                           .withSource(source)
                           .withInitialDeliveryCount(0)
                           .withTarget(coordinator).queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        connection.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER)
                           .withSource(source)
                           .withTarget(coordinator);
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        TransactionManager manager = transactionManager.get();

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testRemoteCoordinatorTriggersSenderCreateWhenManagerHandlerNotSet() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .withRole(Role.SENDER)
                           .withSource(source)
                           .withInitialDeliveryCount(0)
                           .withTarget(coordinator).queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<Receiver> transactionManager = new AtomicReference<>();
        session.receiverOpenHandler(txnReceiver -> {
            transactionManager.set(txnReceiver);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER)
                           .withSource(source)
                           .withTarget(coordinator);
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Receiver manager = transactionManager.get();

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteTarget());

        assertEquals(TxnCapability.LOCAL_TXN, manager.<Coordinator>getRemoteTarget().getCapabilities()[0]);

        manager.setTarget(manager.<Coordinator>getRemoteTarget().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testTransactionControllerDeclaresTransaction() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource(source).withTarget(coordinator).respond();
        peer.remoteFlow().withLinkCredit(1).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        final AtomicBoolean openedWithCoordinatorTarget = new AtomicBoolean();
        txnController.openHandler(result -> {
            if (result.getRemoteCoordinator() instanceof Coordinator) {
                openedWithCoordinatorTarget.set(true);
            }
        });

        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();
        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });

        txnController.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare().accept(TXN_ID);

        assertTrue(openedWithCoordinatorTarget.get());

        assertNotNull(txnController.declare());

        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertArrayEquals(TXN_ID, declaredTxnId.get());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testTransactionControllerDeclareAndDischargeOneTransaction() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource(source).withTarget(coordinator).respond();
        peer.remoteFlow().withLinkCredit(2).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();
        final AtomicReference<byte[]> dischargedTxnId = new AtomicReference<>();

        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });
        txnController.dischargedHandler(result -> {
            dischargedTxnId.set(result.getTxnId().arrayCopy());
        });

        txnController.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare().accept(TXN_ID);

        final Transaction<TransactionController> txn = txnController.declare();
        assertNotNull(txn);

        peer.waitForScriptToComplete();
        peer.expectDischarge().withTxnId(TXN_ID).withFail(false).accept();

        assertArrayEquals(TXN_ID, declaredTxnId.get());

        txnController.discharge(txn, false);

        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertArrayEquals(TXN_ID, dischargedTxnId.get());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testTransactionDeclareRejected() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource(source).withTarget(coordinator).respond();
        peer.remoteFlow().withLinkCredit(2).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        final AtomicBoolean decalreFailure = new AtomicBoolean();
        final AtomicReference<Transaction<TransactionController>> failedTxn = new AtomicReference<>();

        final ErrorCondition failureError =
            new ErrorCondition(AmqpError.INTERNAL_ERROR, "Cannot Declare Transaction at this time");

        txnController.declareFailureHandler(result -> {
            decalreFailure.set(true);
            failedTxn.set(result);
        });

        txnController.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare().reject(failureError);

        final Transaction<TransactionController> txn = txnController.declare();

        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(decalreFailure.get());
        assertSame(txn, failedTxn.get());
        assertEquals(TransactionState.FAILED, txn.getState());
        assertEquals(failureError, txn.getCondition());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testTransactionDischargeRejected() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource(source).withTarget(coordinator).respond();
        peer.remoteFlow().withLinkCredit(2).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        final AtomicBoolean dischargeFailure = new AtomicBoolean();
        final AtomicReference<Transaction<TransactionController>> failedTxn = new AtomicReference<>();
        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.TRANSACTION_TIMEOUT, "Transaction timed out");

        txnController.dischargeFailureHandler(result -> {
            dischargeFailure.set(true);
            failedTxn.set(result);
        });

        txnController.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare().accept();

        final Transaction<TransactionController> txn = txnController.declare();

        peer.waitForScriptToComplete();
        peer.expectDischarge().reject(failureError);

        txnController.discharge(txn, false);

        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(dischargeFailure.get());
        assertSame(txn, failedTxn.get());
        assertEquals(TransactionState.FAILED, txn.getState());
        assertEquals(failureError, txn.getCondition());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }
}
