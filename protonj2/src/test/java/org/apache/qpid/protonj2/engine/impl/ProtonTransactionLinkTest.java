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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.Transaction;
import org.apache.qpid.protonj2.engine.TransactionController;
import org.apache.qpid.protonj2.engine.TransactionManager;
import org.apache.qpid.protonj2.engine.TransactionState;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.test.driver.ProtonTestPeer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.TransactionErrors;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transactions.TxnCapability;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.Test;

/**
 * Tests for AMQP transaction over normal {@link Sender} and {@link Receiver} links.
 */
public class ProtonTransactionLinkTest extends ProtonEngineTestSupport {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonTransactionLinkTest.class);

    private Symbol[] DEFAULT_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                       Rejected.DESCRIPTOR_SYMBOL,
                                                       Released.DESCRIPTOR_SYMBOL,
                                                       Modified.DESCRIPTOR_SYMBOL };

    private String[] DEFAULT_OUTCOMES_STRINGS = new String[] { Accepted.DESCRIPTOR_SYMBOL.toString(),
                                                               Rejected.DESCRIPTOR_SYMBOL.toString(),
                                                               Released.DESCRIPTOR_SYMBOL.toString(),
                                                               Modified.DESCRIPTOR_SYMBOL.toString() };

    @Test(timeout = 20_000)
    public void testCreateDefaultCoordinatorSender() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
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
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .withRole(Role.SENDER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
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
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .withRole(Role.SENDER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        connection.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
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
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .withRole(Role.SENDER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<Receiver> transactionManager = new AtomicReference<>();
        session.receiverOpenHandler(txnReceiver -> {
            transactionManager.set(txnReceiver);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
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
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
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
    public void testTransactionControllerBeginComiitBeginRollback() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        final byte[] TXN_ID1 = new byte[] { 1, 2, 3, 4 };
        final byte[] TXN_ID2 = new byte[] { 2, 2, 3, 4 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
        peer.remoteFlow().withLinkCredit(4).queue();
        peer.expectDeclare().accept(TXN_ID1);
        peer.expectDischarge().withFail(false).withTxnId(TXN_ID1).accept();
        peer.expectDeclare().accept(TXN_ID2);
        peer.expectDischarge().withFail(true).withTxnId(TXN_ID2).accept();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);
        txnController.open();

        assertTrue(txnController.isLocallyOpen());
        assertTrue(txnController.isRemotelyOpen());
        assertFalse(txnController.isLocallyClosed());
        assertFalse(txnController.isRemotelyClosed());

        Transaction<TransactionController> txn1 = txnController.newTransaction();
        Transaction<TransactionController> txn2 = txnController.newTransaction();

        // Begin / Commit
        txnController.declare(txn1);
        txnController.discharge(txn1, false);

        // Begin / Rollback
        txnController.declare(txn2);
        txnController.discharge(txn2, true);

        txnController.close();

        assertFalse(txnController.isLocallyOpen());
        assertFalse(txnController.isRemotelyOpen());
        assertTrue(txnController.isLocallyClosed());
        assertTrue(txnController.isRemotelyClosed());

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testTransactionControllerDeclareAndDischargeOneTransactionDirect() {
        doTestTransactionControllerDeclareAndDischargeOneTransaction(false);
    }

    @Test(timeout = 20_000)
    public void testTransactionControllerDeclareAndDischargeOneTransactionInDirect() {
        doTestTransactionControllerDeclareAndDischargeOneTransaction(true);
    }

    private void doTestTransactionControllerDeclareAndDischargeOneTransaction(boolean useNewTransactionAPI) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
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

        final Transaction<TransactionController> txn;
        if (useNewTransactionAPI) {
            txn = txnController.newTransaction();
            assertEquals(TransactionState.IDLE, txn.getState());
            txnController.declare(txn);
        } else {
            txn = txnController.declare();
        }

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
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
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
        peer.expectDeclare().reject(AmqpError.INTERNAL_ERROR.toString(), "Cannot Declare Transaction at this time");

        final Transaction<TransactionController> txn = txnController.declare();

        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(decalreFailure.get());
        assertSame(txn, failedTxn.get());
        assertEquals(TransactionState.DECLARE_FAILED, txn.getState());
        assertEquals(failureError, txn.getCondition());
        assertTrue(txnController.transactions().isEmpty());

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
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
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
        peer.expectDischarge().reject(TransactionErrors.TRANSACTION_TIMEOUT.toString(), "Transaction timed out");

        txnController.discharge(txn, false);

        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(dischargeFailure.get());
        assertSame(txn, failedTxn.get());
        assertEquals(TransactionState.DISCHARGE_FAILED, txn.getState());
        assertEquals(failureError, txn.getCondition());
        assertTrue(txnController.transactions().isEmpty());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testCannotDeclareTransactionFromOneControllerInAnother() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
        peer.remoteFlow().withLinkCredit(2).queue();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
        peer.remoteFlow().withLinkCredit(2).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();

        TransactionController txnController1 = session.coordinator("test-coordinator-1");
        TransactionController txnController2 = session.coordinator("test-coordinator-2");

        txnController1.setSource(source);
        txnController1.setCoordinator(coordinator);
        txnController1.open();

        txnController2.setSource(source);
        txnController2.setCoordinator(coordinator);
        txnController2.open();

        peer.waitForScriptToComplete();

        assertTrue(txnController1.hasCapacity());
        assertTrue(txnController2.hasCapacity());

        final Transaction<TransactionController> txn1 = txnController1.newTransaction();
        final Transaction<TransactionController> txn2 = txnController2.newTransaction();

        try {
            txnController1.declare(txn2);
            fail("Should not be able to declare a transaction with TXN created from another controller");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        try {
            txnController2.declare(txn1);
            fail("Should not be able to declare a transaction with TXN created from another controller");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        assertEquals(1, txnController1.transactions().size());
        assertEquals(1, txnController2.transactions().size());

        peer.expectDetach().withClosed(true).respond();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        txnController1.close();
        txnController2.close();

        session.close();
        connection.close();

        // Never discharged so they remain in the controller now
        assertEquals(1, txnController1.transactions().size());
        assertEquals(1, txnController2.transactions().size());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testCannotDischargeTransactionFromOneControllerInAnother() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
        peer.remoteFlow().withLinkCredit(2).queue();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
        peer.remoteFlow().withLinkCredit(2).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();

        TransactionController txnController1 = session.coordinator("test-coordinator-1");
        TransactionController txnController2 = session.coordinator("test-coordinator-2");

        txnController1.setSource(source);
        txnController1.setCoordinator(coordinator);
        txnController1.open();

        txnController2.setSource(source);
        txnController2.setCoordinator(coordinator);
        txnController2.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare().accept();
        peer.expectDeclare().accept();

        assertTrue(txnController1.hasCapacity());
        assertTrue(txnController2.hasCapacity());

        final Transaction<TransactionController> txn1 = txnController1.declare();
        final Transaction<TransactionController> txn2 = txnController2.declare();

        peer.waitForScriptToComplete();

        try {
            txnController1.discharge(txn2, false);
            fail("Should not be able to discharge a transaction with TXN created from another controller");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        try {
            txnController2.discharge(txn1, false);
            fail("Should not be able to discharge a transaction with TXN created from another controller");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        peer.expectDetach().withClosed(true).respond();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        txnController1.close();
        txnController2.close();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testSendMessageInsideOfTransaction() throws Exception {
        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };
        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};
        final ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withLinkCredit(1).queue();
        peer.expectCoordinatorAttach().respond();
        peer.remoteFlow().withLinkCredit(2).queue();
        peer.expectDeclare().accept(TXN_ID);

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test").open();

        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);
        txnController.open();

        Transaction<TransactionController> txn = txnController.declare();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withHandle(0)
                             .withNonNullPayload()
                             .withState().transactional().withTxnId(TXN_ID).and()
                             .respond()
                             .withState().transactional().withTxnId(TXN_ID).withAccepted().and()
                             .withSettled(true);
        peer.expectDischarge().withFail(false).withTxnId(TXN_ID).accept();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(sender.isSendable());

        OutgoingDelivery delivery = sender.next();

        delivery.disposition(new TransactionalState().setTxnId(new Binary(TXN_ID)), false);
        delivery.writeBytes(payload);

        assertTrue(txnController.transactions().contains(txn));

        txnController.discharge(txn, false);

        assertFalse(txnController.transactions().contains(txn));

        assertNotNull(delivery);
        assertNotNull(delivery.getRemoteState());
        assertEquals(delivery.getRemoteState().getType(), DeliveryState.DeliveryStateType.Transactional);
        assertNotNull(delivery.getState());
        assertEquals(delivery.getState().getType(), DeliveryState.DeliveryStateType.Transactional);
        assertFalse(delivery.isSettled());

        session.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test(timeout = 20_000)
    public void testTransactionManagerSignalsTxnDeclarationAndDischarge() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        final byte[] TXN_ID = new byte[] {0, 1, 2, 3};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link")
                           .withHandle(0)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().ofReceiver()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.declareHandler(declared -> {
            manager.declared(declared, new Binary(TXN_ID));
        });
        manager.dischargeHandler(discharged -> {
            manager.discharged(discharged);
        });

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withTxnId(TXN_ID).withFail(false).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();
        peer.expectDisposition().withState().accepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20_000)
    public void testCommitTransactionAfterConnectionDropsFollowingTxnDeclared() throws Exception {
        dischargeTransactionAfterConnectionDropsFollowingTxnDeclared(true);
    }

    @Test(timeout = 20_000)
    public void testRollbackTransactionAfterConnectionDropsFollowingTxnDeclared() throws Exception {
        dischargeTransactionAfterConnectionDropsFollowingTxnDeclared(false);
    }

    public void dischargeTransactionAfterConnectionDropsFollowingTxnDeclared(boolean commit) throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectCoordinatorAttach().respond();
        peer.remoteFlow().withLinkCredit(2).queue();
        peer.expectDeclare().accept(txnId);
        peer.rejectIncomingIOAfterLastScriptedElement();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);
        txnController.open();

        Transaction<TransactionController> txn = txnController.newTransaction();

        txnController.addCapacityAvailableHandler(controller -> {
            controller.declare(txn);
        });

        peer.waitForScriptToComplete();

        // The write that are triggered here should fail and throw an exception

        try {
            if (commit) {
                txnController.discharge(txn, false);
            } else {
                txnController.discharge(txn, true);
            }

            fail("Should have failed to discharge transaction");
        } catch (EngineFailedException ex) {
            // Expected error as a simulated IO disconnect was requested
            LOG.info("Caught expected EngineFailedException on write of discharge", ex);
        }

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }
}
