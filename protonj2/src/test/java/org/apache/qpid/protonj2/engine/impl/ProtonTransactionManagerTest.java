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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.Transaction;
import org.apache.qpid.protonj2.engine.TransactionManager;
import org.apache.qpid.protonj2.test.driver.ProtonInVMTestPeer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transactions.TransactionErrors;
import org.apache.qpid.protonj2.types.transactions.TxnCapability;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for AMQP transaction manager abstraction used on server side normally
 */
@Timeout(20)
class ProtonTransactionManagerTest extends ProtonEngineTestSupport  {

    private String[] DEFAULT_OUTCOMES_STRINGS = new String[] { Accepted.DESCRIPTOR_SYMBOL.toString(),
                                                               Rejected.DESCRIPTOR_SYMBOL.toString(),
                                                               Released.DESCRIPTOR_SYMBOL.toString(),
                                                               Modified.DESCRIPTOR_SYMBOL.toString() };

    @Test
    public void testRemoteCoordinatorSenderSignalsTransactionManagerFromSessionWhenEnabled() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        assertSame(transactionManager.get().getParent(), session);

        TransactionManager manager = transactionManager.get();

        assertFalse(manager.isLocallyClosed());
        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertTrue(manager.isLocallyClosed());
        assertNull(failure);
    }

    @Test
    public void testCloseRemotelyInitiatedTxnManagerWithErrorCondition() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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

        final ErrorCondition condition = new ErrorCondition(AmqpError.NOT_IMPLEMENTED, "Transactions are not supported");
        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource(nullValue())
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectDetach().withError(condition.getCondition().toString(), condition.getDescription()).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());
        assertSame(transactionManager.get().getParent(), session);

        TransactionManager manager = transactionManager.get();

        assertFalse(manager.isLocallyClosed());
        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(null);
        manager.open();
        manager.setCondition(condition);
        manager.close();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNotNull(manager.getCondition());
        assertTrue(manager.isLocallyClosed());
        assertTrue(manager.isRemotelyClosed());
        assertNull(failure);
    }

    @Test
    public void testTransactionManagerAlertedIfParentSessionClosed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());
        assertSame(transactionManager.get().getParent(), session);

        final AtomicBoolean parentClosed = new AtomicBoolean();

        TransactionManager manager = transactionManager.get();
        manager.parentEndpointClosedHandler((txnMgr) -> parentClosed.set(true));

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();

        session.close();

        assertTrue(parentClosed.get());

        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionManagerAlertedIfParentConnectionClosed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        peer.expectClose().respond();

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());
        assertSame(transactionManager.get().getParent(), session);

        final AtomicBoolean parentClosed = new AtomicBoolean();

        TransactionManager manager = transactionManager.get();
        manager.parentEndpointClosedHandler((txnMgr) -> parentClosed.set(true));

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());

        assertNotNull(manager.getSource());
        assertNotNull(manager.getCoordinator());

        manager.open();

        connection.close();

        assertTrue(parentClosed.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionManagerAlertedIfEngineShutdown() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());
        assertSame(transactionManager.get().getParent(), session);

        final AtomicBoolean engineShutdown = new AtomicBoolean();

        TransactionManager manager = transactionManager.get();
        manager.engineShutdownHandler((theEngine) -> engineShutdown.set(true));

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();

        engine.shutdown();

        assertTrue(engineShutdown.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testRemoteCoordinatorSenderSignalsTransactionManagerFromConnectionWhenEnabled() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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

    @Test
    public void testTransactionManagerSignalsTxnDeclarationAndDischargeSucceeds() {
        doTestTransactionManagerSignalsTxnDeclarationAndDischarge(false);
    }

    @Test
    public void testTransactionManagerSignalsTxnDeclarationAndDischargeFailed() {
        doTestTransactionManagerSignalsTxnDeclarationAndDischarge(true);
    }

    private void doTestTransactionManagerSignalsTxnDeclarationAndDischarge(boolean txnFailed) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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

        final AtomicBoolean txnRolledBack = new AtomicBoolean();
        final AtomicReference<TransactionManager> transactionManager = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            transactionManager.set(manager);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().ofReceiver()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withLinkCredit(2);

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
            txnRolledBack.set(discharged.getDischargeState().equals(Transaction.DischargeState.ROLLBACK));
            manager.discharged(discharged);
        });
        manager.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withTxnId(TXN_ID).withFail(txnFailed).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();
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
        if (txnFailed) {
            assertTrue(txnRolledBack.get());
        } else {
            assertFalse(txnRolledBack.get());
        }
        assertNull(failure);
    }

    @Test
    public void testTransactionManagerSignalsTxnDeclarationFailed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        peer.expectFlow().withLinkCredit(1);

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();
        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.TRANSACTION_TIMEOUT, "Transaction timed out");

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.declareHandler(declared -> {
            manager.declareFailed(declared, failureError);
        });
        manager.addCredit(1);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().rejected(failureError.getCondition().toString(), failureError.getDescription());
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

    @Test
    public void testTransactionManagerSignalsTxnDischargeFailed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        peer.expectFlow().withLinkCredit(2);

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();
        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.TRANSACTION_TIMEOUT, "Transaction timed out");

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.declareHandler(declared -> {
            manager.declared(declared, new Binary(TXN_ID));
        });
        manager.dischargeHandler(discharged -> {
            manager.dischargeFailed(discharged, failureError);
        });
        manager.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withTxnId(TXN_ID).withFail(false).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();
        peer.expectDisposition().withState().rejected(failureError.getCondition().toString(), failureError.getDescription());
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertEquals(0, manager.getCredit());

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testManagerChecksDeclaredArgumentsForSomeCorrectness() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        peer.expectFlow().withLinkCredit(1);

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();
        final AtomicReference<Transaction<TransactionManager>> txn = new AtomicReference<>();

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.declareHandler(declared -> {
            txn.set(declared);
        });
        manager.addCredit(1);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID).withAccepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertNotNull(txn.get());

        assertThrows(IllegalArgumentException.class, () -> manager.declared(txn.get(), (Binary) null));
        assertThrows(IllegalArgumentException.class, () -> manager.declared(txn.get(), new Binary(new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> manager.declared(txn.get(), new Binary((ProtonBuffer) null)));
        assertThrows(NullPointerException.class, () -> manager.declared(txn.get(), (byte[]) null));
        assertThrows(IllegalArgumentException.class, () -> manager.declared(txn.get(), new byte[0]));

        manager.declared(txn.get(), new Binary(TXN_ID));

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testManagerIgnoresAbortedTransfers() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

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
        peer.expectFlow().withLinkCredit(1);

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();
        final AtomicReference<Transaction<TransactionManager>> txn = new AtomicReference<>();
        final AtomicInteger declareCounter = new AtomicInteger();

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.declareHandler(declared -> {
            declareCounter.incrementAndGet();
            txn.set(declared);
        });
        manager.addCredit(1);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID).withAccepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames aborting first attempt and then getting it right.
        peer.remoteDeclare().withMore(true).withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();
        peer.remoteTransfer().withDeliveryId(0).withAborted(true).now();
        peer.remoteDeclare().withDeliveryId(1).withDeliveryTag(new byte[] {1}).now();

        assertNotNull(txn.get());
        assertEquals(1, declareCounter.get());

        manager.declared(txn.get(), new Binary(TXN_ID));

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCannotSignalDeclaredFromAnotherTransactionManager() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

        final byte[] TXN_ID = new byte[] {0, 1, 2, 3};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link-1")
                           .withHandle(0)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();
        peer.remoteAttach().withName("TXN-Link-2")
                           .withHandle(1)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager1 = new AtomicReference<>();
        final AtomicReference<TransactionManager> transactionManager2 = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            if (transactionManager1.get() == null) {
                transactionManager1.set(manager);
            } else {
                transactionManager2.set(manager);
            }
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().ofReceiver()
                           .withHandle(0)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(0).withLinkCredit(2);
        peer.expectAttach().ofReceiver()
                           .withHandle(1)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(1).withLinkCredit(2);

        assertNotNull(transactionManager1.get());
        assertNotNull(transactionManager1.get().getRemoteCoordinator());
        assertNotNull(transactionManager2.get());
        assertNotNull(transactionManager2.get().getRemoteCoordinator());

        final TransactionManager manager1 = transactionManager1.get();
        final TransactionManager manager2 = transactionManager2.get();

        final AtomicReference<Transaction<TransactionManager>> txn = new AtomicReference<>();

        manager1.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager1.setSource(manager1.getRemoteSource().copy());
        manager1.open();
        manager1.declareHandler(declared -> {
            txn.set(declared);
        });
        manager1.dischargeHandler(discharged -> {
            manager1.discharged(discharged);
        });
        manager1.addCredit(2);

        // Put number two into a valid state as well.
        manager2.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager2.setSource(manager1.getRemoteSource().copy());
        manager2.open();
        manager2.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withHandle(0).withTxnId(TXN_ID).withFail(false).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();
        peer.expectDisposition().withState().accepted();
        peer.expectDetach().withHandle(0).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withHandle(0).withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertNotNull(txn.get());

        assertThrows(IllegalArgumentException.class, () -> manager2.declared(txn.get(), new Binary(TXN_ID)));

        manager1.declared(txn.get(), new Binary(TXN_ID));

        manager1.close();
        manager2.close();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCannotSignalDeclareFailedFromAnotherTransactionManager() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link-1")
                           .withHandle(0)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();
        peer.remoteAttach().withName("TXN-Link-2")
                           .withHandle(1)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager1 = new AtomicReference<>();
        final AtomicReference<TransactionManager> transactionManager2 = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            if (transactionManager1.get() == null) {
                transactionManager1.set(manager);
            } else {
                transactionManager2.set(manager);
            }
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().ofReceiver()
                           .withHandle(0)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(0).withLinkCredit(2);
        peer.expectAttach().ofReceiver()
                           .withHandle(1)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(1).withLinkCredit(2);

        assertNotNull(transactionManager1.get());
        assertNotNull(transactionManager1.get().getRemoteCoordinator());
        assertNotNull(transactionManager2.get());
        assertNotNull(transactionManager2.get().getRemoteCoordinator());

        final TransactionManager manager1 = transactionManager1.get();
        final TransactionManager manager2 = transactionManager2.get();

        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.UNKNOWN_ID, "Transaction unknown for some reason");

        final AtomicReference<Transaction<TransactionManager>> txn = new AtomicReference<>();

        manager1.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager1.setSource(manager1.getRemoteSource().copy());
        manager1.open();
        manager1.declareHandler(declared -> {
            txn.set(declared);
        });
        manager1.addCredit(2);

        // Put number two into a valid state as well.
        manager2.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager2.setSource(manager1.getRemoteSource().copy());
        manager2.open();
        manager2.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().rejected(failureError.getCondition().toString(), failureError.getDescription());
        peer.expectDetach().withHandle(0).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withHandle(0).withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertNotNull(txn.get());

        assertThrows(IllegalArgumentException.class, () -> manager2.declareFailed(txn.get(), failureError));

        manager1.declareFailed(txn.get(), failureError);

        manager1.close();
        manager2.close();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCannotSignalDischargedFromAnotherTransactionManager() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

        final byte[] TXN_ID = new byte[] {0, 1, 2, 3};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link-1")
                           .withHandle(0)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();
        peer.remoteAttach().withName("TXN-Link-2")
                           .withHandle(1)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager1 = new AtomicReference<>();
        final AtomicReference<TransactionManager> transactionManager2 = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            if (transactionManager1.get() == null) {
                transactionManager1.set(manager);
            } else {
                transactionManager2.set(manager);
            }
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().ofReceiver()
                           .withHandle(0)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(0).withLinkCredit(2);
        peer.expectAttach().ofReceiver()
                           .withHandle(1)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(1).withLinkCredit(2);

        assertNotNull(transactionManager1.get());
        assertNotNull(transactionManager1.get().getRemoteCoordinator());
        assertNotNull(transactionManager2.get());
        assertNotNull(transactionManager2.get().getRemoteCoordinator());

        final TransactionManager manager1 = transactionManager1.get();
        final TransactionManager manager2 = transactionManager2.get();

        final AtomicReference<Transaction<TransactionManager>> txn = new AtomicReference<>();

        manager1.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager1.setSource(manager1.getRemoteSource().copy());
        manager1.open();
        manager1.declareHandler(declared -> {
            txn.set(declared);
            manager1.declared(declared, TXN_ID);
        });
        manager1.addCredit(2);

        // Put number two into a valid state as well.
        manager2.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager2.setSource(manager1.getRemoteSource().copy());
        manager2.open();
        manager2.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withHandle(0).withTxnId(TXN_ID).withFail(false).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withHandle(0).withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertNotNull(txn.get());

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().accepted();
        peer.expectDetach().withHandle(0).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertThrows(IllegalArgumentException.class, () -> manager2.discharged(txn.get()));

        manager1.discharged(txn.get());

        manager1.close();
        manager2.close();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCannotSignalDischargeFailedFromAnotherTransactionManager() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

        final byte[] TXN_ID = new byte[] {0, 1, 2, 3};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteAttach().withName("TXN-Link-1")
                           .withHandle(0)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();
        peer.remoteAttach().withName("TXN-Link-2")
                           .withHandle(1)
                           .ofSender()
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withInitialDeliveryCount(0)
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and().queue();

        Connection connection = engine.start().open();
        Session session = connection.session();

        final AtomicReference<TransactionManager> transactionManager1 = new AtomicReference<>();
        final AtomicReference<TransactionManager> transactionManager2 = new AtomicReference<>();
        session.transactionManagerOpenHandler(manager -> {
            if (transactionManager1.get() == null) {
                transactionManager1.set(manager);
            } else {
                transactionManager2.set(manager);
            }
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().ofReceiver()
                           .withHandle(0)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(0).withLinkCredit(2);
        peer.expectAttach().ofReceiver()
                           .withHandle(1)
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectFlow().withHandle(1).withLinkCredit(2);

        assertNotNull(transactionManager1.get());
        assertNotNull(transactionManager1.get().getRemoteCoordinator());
        assertNotNull(transactionManager2.get());
        assertNotNull(transactionManager2.get().getRemoteCoordinator());

        final TransactionManager manager1 = transactionManager1.get();
        final TransactionManager manager2 = transactionManager2.get();

        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.UNKNOWN_ID, "Transaction unknown for some reason");

        final AtomicReference<Transaction<TransactionManager>> txn = new AtomicReference<>();

        manager1.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager1.setSource(manager1.getRemoteSource().copy());
        manager1.open();
        manager1.declareHandler(declared -> {
            txn.set(declared);
            manager1.declared(declared, TXN_ID);
        });
        manager1.addCredit(2);

        // Put number two into a valid state as well.
        manager2.setCoordinator(manager1.getRemoteCoordinator().copy());
        manager2.setSource(manager1.getRemoteSource().copy());
        manager2.open();
        manager2.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withHandle(0).withTxnId(TXN_ID).withFail(false).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withHandle(0).withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertNotNull(txn.get());

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().rejected(failureError.getCondition().toString(), failureError.getDescription());
        peer.expectDetach().withHandle(0).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertThrows(IllegalArgumentException.class, () -> manager2.dischargeFailed(txn.get(), failureError));

        manager1.dischargeFailed(txn.get(), failureError);

        manager1.close();
        manager2.close();

        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTransactionManagerRejectsAttemptedDischargeOfUnkownTxnId() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

        final byte[] TXN_ID = new byte[] {0, 1, 2, 3};
        final byte[] TXN_ID_UNKNOWN= new byte[] {3, 2, 1, 0};

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
        peer.expectFlow().withLinkCredit(2);

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();
        final ErrorCondition failureError =
            new ErrorCondition(TransactionErrors.UNKNOWN_ID, "Transaction Manager is not tracking the given transaction ID.");

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.declareHandler(declared -> {
            manager.declared(declared, new Binary(TXN_ID));
        });
        manager.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().transactional().withTxnId(TXN_ID);
        peer.remoteDischarge().withTxnId(TXN_ID_UNKNOWN).withFail(false).withDeliveryId(1).withDeliveryTag(new byte[] {1}).queue();
        peer.expectDisposition().withState().rejected(failureError.getCondition().toString(), failureError.getDescription());
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Starts the flow of Transaction frames
        peer.remoteDeclare().withDeliveryId(0).withDeliveryTag(new byte[] {0}).now();

        assertEquals(0, manager.getCredit());

        manager.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testEngineFailedIfNonTxnRelatedTransferArrivesAtCoordinator() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonInVMTestPeer peer = createTestPeer(engine);

        final byte[] payload = createEncodedMessage(new AmqpValue<>("test"));

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
        peer.expectFlow().withLinkCredit(2);

        assertNotNull(transactionManager.get());
        assertNotNull(transactionManager.get().getRemoteCoordinator());

        final TransactionManager manager = transactionManager.get();

        assertEquals(TxnCapability.LOCAL_TXN, manager.getRemoteCoordinator().getCapabilities()[0]);

        manager.setCoordinator(manager.getRemoteCoordinator().copy());
        manager.setSource(manager.getRemoteSource().copy());
        manager.open();
        manager.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectClose().withError(notNullValue());
        // Send the invalid Transfer to trigger engine shutdown
        peer.remoteTransfer().withDeliveryTag(new byte[] {0})
                             .withPayload(payload)
                             .withMore(false)
                             .now();

        assertTrue(engine.isFailed());

        // The transfer write should trigger an error back into the peer which we can ignore.
        peer.waitForScriptToCompleteIgnoreErrors();
        assertNotNull(failure);
    }
}
