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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.Transaction;
import org.apache.qpid.protonj2.engine.TransactionController;
import org.apache.qpid.protonj2.engine.TransactionState;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for AMQP transaction controller abstraction used on client side normally
 */
@Timeout(20)
class ProtonTransactionControllerTest extends ProtonEngineTestSupport {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonTransactionControllerTest.class);

    private Symbol[] DEFAULT_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                       Rejected.DESCRIPTOR_SYMBOL,
                                                       Released.DESCRIPTOR_SYMBOL,
                                                       Modified.DESCRIPTOR_SYMBOL };

    private String[] DEFAULT_OUTCOMES_STRINGS = new String[] { Accepted.DESCRIPTOR_SYMBOL.toString(),
                                                               Rejected.DESCRIPTOR_SYMBOL.toString(),
                                                               Released.DESCRIPTOR_SYMBOL.toString(),
                                                               Modified.DESCRIPTOR_SYMBOL.toString() };

    @Test
    public void testTransactionControllerDeclaresTransaction() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

        assertSame(session, txnController.getParent());

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

    @Test
    public void testTransactionControllerSignalsWhenParentSessionClosed() {
        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
        peer.expectDeclare().accept(TXN_ID);
        peer.expectEnd().respond();
        peer.expectClose().respond();

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

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();
        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });

        final AtomicBoolean parentEndpointClosed = new AtomicBoolean();
        txnController.parentEndpointClosedHandler((controller) -> {
            parentEndpointClosed.set(true);
        });

        txnController.open();
        txnController.declare();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertTrue(parentEndpointClosed.get());
        assertNull(failure);
    }

    @Test
    public void testTransactionControllerSignalsWhenParentConnectionClosed() {
        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
        peer.expectDeclare().accept(TXN_ID);
        peer.expectClose().respond();

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

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();
        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });

        final AtomicBoolean parentEndpointClosed = new AtomicBoolean();
        txnController.parentEndpointClosedHandler((controller) -> {
            parentEndpointClosed.set(true);
        });

        txnController.open();
        txnController.declare();

        connection.close();

        peer.waitForScriptToComplete();

        assertTrue(parentEndpointClosed.get());
        assertNull(failure);
    }

    @Test
    public void testTransactionControllerSignalsWhenEngineShutdown() {
        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
        peer.expectDeclare().accept(TXN_ID);

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

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();
        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });

        final AtomicBoolean engineShutdown = new AtomicBoolean();
        txnController.engineShutdownHandler((theEngine) -> {
            engineShutdown.set(true);
        });

        txnController.open();
        txnController.declare();

        engine.shutdown();

        peer.waitForScriptToComplete();

        assertTrue(engineShutdown.get());
        assertNull(failure);
    }

    @Test
    public void testTransactionControllerDoesNotSignalsWhenParentConnectionClosedIfAlreadyClosed() {
        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
        peer.expectDeclare().accept(TXN_ID);
        peer.expectDetach().respond();
        peer.expectClose().respond();

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

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();
        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });

        final AtomicBoolean parentEndpointClosed = new AtomicBoolean();
        txnController.parentEndpointClosedHandler((controller) -> {
            parentEndpointClosed.set(true);
        });

        txnController.open();
        txnController.declare();
        txnController.close();

        connection.close();

        peer.waitForScriptToComplete();

        assertFalse(parentEndpointClosed.get());
        assertNull(failure);
    }

    @Test
    public void testTransactionControllerBeginCommitBeginRollback() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

    @Test
    public void testTransactionControllerDeclareAndDischargeOneTransactionDirect() {
        doTestTransactionControllerDeclareAndDischargeOneTransaction(false);
    }

    @Test
    public void testTransactionControllerDeclareAndDischargeOneTransactionInDirect() {
        doTestTransactionControllerDeclareAndDischargeOneTransaction(true);
    }

    private void doTestTransactionControllerDeclareAndDischargeOneTransaction(boolean useNewTransactionAPI) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
            if (useNewTransactionAPI) {
                assertEquals(txnController, result.getLinkedResource(TransactionController.class) );
            } else {
                assertNull(result.getLinkedResource());
            }
        });
        txnController.dischargedHandler(result -> {
            dischargedTxnId.set(result.getTxnId().arrayCopy());
            if (useNewTransactionAPI) {
                assertEquals(txnController, result.getLinkedResource(TransactionController.class) );
            } else {
                assertNull(result.getLinkedResource());
            }
        });

        txnController.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare().accept(TXN_ID);

        final Transaction<TransactionController> txn;
        if (useNewTransactionAPI) {
            txn = txnController.newTransaction();
            txn.setLinkedResource(txnController);
            assertEquals(TransactionState.IDLE, txn.getState());
            txnController.declare(txn);
        } else {
            txn = txnController.declare();
            assertNotNull(txn.getAttachments());
            assertSame(txn.getAttachments(), txn.getAttachments());
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
        assertFalse(txn.isFailed());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionDeclareRejected() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

        final AtomicBoolean declareFailure = new AtomicBoolean();
        final AtomicReference<Transaction<TransactionController>> failedTxn = new AtomicReference<>();

        final ErrorCondition failureError =
            new ErrorCondition(AmqpError.INTERNAL_ERROR, "Cannot Declare Transaction at this time");

        txnController.declareFailureHandler(result -> {
            declareFailure.set(true);
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

        assertTrue(declareFailure.get());
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

    @Test
    public void testTransactionDischargeRejected() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

    @Test
    public void testCannotDeclareTransactionFromOneControllerInAnother() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

    @Test
    public void testCannotDischargeTransactionFromOneControllerInAnother() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

    @Test
    public void testSendMessageInsideOfTransaction() throws Exception {
        final byte[] TXN_ID = new byte[] { 1, 2, 3, 4 };
        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};
        final ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

    @Test
    public void testCommitTransactionAfterConnectionDropsFollowingTxnDeclared() throws Exception {
        dischargeTransactionAfterConnectionDropsFollowingTxnDeclared(true);
    }

    @Test
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
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectCoordinatorAttach().respond();
        peer.remoteFlow().withLinkCredit(2).queue();
        peer.expectDeclare().accept(txnId);
        peer.dropAfterLastHandler();

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

    @Test
    public void testTransactionControllerSignalsHandlerWhenCreditAvailableDirect() throws Exception {
        doTestTransactionControllerSignalsHandlerWhenCreditAvailable(false);
    }

    @Test
    public void testTransactionControllerSignalsHandlerWhenCreditAvailableInDirect() throws Exception {
        doTestTransactionControllerSignalsHandlerWhenCreditAvailable(true);
    }

    private void doTestTransactionControllerSignalsHandlerWhenCreditAvailable(boolean useNewTransactionAPI) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();

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

        final AtomicReference<Transaction<TransactionController>> txn = new AtomicReference<>();

        if (useNewTransactionAPI) {
            txn.set(txnController.newTransaction());
            try {
                txnController.declare(txn.get());
                fail("Should not be able to declare as there is no link credit to do so.");
            } catch (IllegalStateException ise) {
            }

            txnController.addCapacityAvailableHandler((controller) -> {
                txnController.declare(txn.get());
            });
        } else {
            try {
                txnController.declare();
                fail("Should not be able to declare as there is no link credit to do so.");
            } catch (IllegalStateException ise) {
            }

            txnController.addCapacityAvailableHandler((controller) -> {
                txn.set(txnController.declare());
            });
        }

        peer.remoteFlow().withNextIncomingId(1).withDeliveryCount(0).withLinkCredit(1).now();
        peer.waitForScriptToComplete();
        peer.expectDischarge().withTxnId(TXN_ID).withFail(false).accept();

        assertNotNull(txn.get());
        assertArrayEquals(TXN_ID, declaredTxnId.get());

        try {
            txnController.discharge(txn.get(), false);
            fail("Should not be able to discharge as there is no link credit to do so.");
        } catch (IllegalStateException ise) {
        }

        txnController.addCapacityAvailableHandler((controller) -> {
            txnController.discharge(txn.get(), false);
        });

        peer.remoteFlow().withNextIncomingId(2).withDeliveryCount(1).withLinkCredit(1).now();
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

    @Test
    public void testCapacityAvailableHandlersAreQueuedAndNotifiedWhenCreditGranted() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();

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

        final AtomicReference<Transaction<TransactionController>> txn = new AtomicReference<>();

        txnController.addCapacityAvailableHandler((controller) -> {
            txn.set(txnController.declare());
        });

        txnController.addCapacityAvailableHandler((controller) -> {
            txnController.discharge(txn.get(), false);
        });

        peer.remoteFlow().withNextIncomingId(1).withDeliveryCount(0).withLinkCredit(1).now();
        peer.waitForScriptToComplete();

        assertTrue(txn.get().isDeclared());

        peer.expectDischarge().withTxnId(TXN_ID).withFail(false).accept();

        assertNotNull(txn.get());
        assertArrayEquals(TXN_ID, declaredTxnId.get());

        peer.remoteFlow().withNextIncomingId(2).withDeliveryCount(1).withLinkCredit(1).now();
        peer.waitForScriptToComplete();
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(txn.get().isDischarged());
        assertArrayEquals(TXN_ID, dischargedTxnId.get());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionControllerDeclareIsIdempotent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();
        source.setOutcomes(DEFAULT_OUTCOMES);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().respond();
        peer.remoteFlow().withLinkCredit(3).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        final AtomicReference<byte[]> declaredTxnId = new AtomicReference<>();

        txnController.declaredHandler(result -> {
            declaredTxnId.set(result.getTxnId().arrayCopy());
        });

        txnController.open();

        peer.waitForScriptToComplete();
        peer.expectDeclare();

        final Transaction<TransactionController> txn = txnController.newTransaction();

        txnController.declare(txn);

        assertFalse(txn.isDeclared());  // No response yet
        assertFalse(txn.isDischarged());

        try {
            txnController.declare(txn);
            fail("Should not be able to declare the same transaction a second time.");
        } catch (IllegalStateException ise) {
        }

        try {
            assertEquals(txn.getState(), TransactionState.DECLARING);
            txnController.discharge(txn, false);
            fail("Should not be able to discharge a transaction that is not activated by the remote.");
        } catch (IllegalStateException ise) {
        }

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionDeclareRejectedWithNoHandlerRegistered() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
        peer.expectDeclare().reject(AmqpError.INTERNAL_ERROR.toString(), "Cannot Declare Transaction at this time");
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        final ErrorCondition failureError =
            new ErrorCondition(AmqpError.INTERNAL_ERROR, "Cannot Declare Transaction at this time");

        txnController.open();

        final Transaction<TransactionController> txn = txnController.declare();

        assertNotNull(txn.getCondition());
        assertEquals(TransactionState.DECLARE_FAILED, txn.getState());
        assertEquals(failureError, txn.getCondition());
        assertTrue(txnController.transactions().isEmpty());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionDischargeRejectedWithNoHandlerRegistered() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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
        peer.expectDeclare().accept();
        peer.expectDischarge().reject(TransactionErrors.TRANSACTION_TIMEOUT.toString(), "Transaction timed out");
        peer.expectDetach().withClosed(true).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        TransactionController txnController = session.coordinator("test-coordinator");

        txnController.setSource(source);
        txnController.setCoordinator(coordinator);

        txnController.open();

        final Transaction<TransactionController> txn = txnController.declare();
        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.TRANSACTION_TIMEOUT, "Transaction timed out");

        txnController.discharge(txn, false);

        assertNotNull(txn.getCondition());
        assertEquals(TransactionState.DISCHARGE_FAILED, txn.getState());
        assertEquals(failureError, txn.getCondition());
        assertTrue(txnController.transactions().isEmpty());
        assertTrue(txn.isFailed());

        txnController.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }
}
