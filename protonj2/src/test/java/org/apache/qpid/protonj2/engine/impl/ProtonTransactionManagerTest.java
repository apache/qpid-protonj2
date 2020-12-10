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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.TransactionManager;
import org.apache.qpid.protonj2.test.driver.ProtonTestPeer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transactions.TxnCapability;
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

    @Test
    public void testRemoteCoordinatorSenderSignalsTransactionManagerFromConnectionWhenEnabled() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
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

    @Test
    public void testTransactionManagerSignalsTxnDeclarationAndDischarge() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
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

}
