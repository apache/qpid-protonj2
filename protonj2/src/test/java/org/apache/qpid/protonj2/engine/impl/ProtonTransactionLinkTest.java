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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.TxnCapability;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for AMQP transaction over normal {@link Sender} and {@link Receiver} links.
 */
@Timeout(20)
public class ProtonTransactionLinkTest extends ProtonEngineTestSupport {

    private Symbol[] DEFAULT_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                       Rejected.DESCRIPTOR_SYMBOL,
                                                       Released.DESCRIPTOR_SYMBOL,
                                                       Modified.DESCRIPTOR_SYMBOL };

    private String[] DEFAULT_OUTCOMES_STRINGS = new String[] { Accepted.DESCRIPTOR_SYMBOL.toString(),
                                                               Rejected.DESCRIPTOR_SYMBOL.toString(),
                                                               Released.DESCRIPTOR_SYMBOL.toString(),
                                                               Modified.DESCRIPTOR_SYMBOL.toString() };

    @Test
    public void testCreateDefaultCoordinatorSender() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

    @Test
    public void testCreateCoordinatorSender() {
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

    @Test
    public void testRemoteCoordinatorTriggersSenderCreateWhenManagerHandlerNotSet() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

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

        final AtomicReference<Receiver> transactionReceiver = new AtomicReference<>();
        session.receiverOpenHandler(txnReceiver -> {
            transactionReceiver.set(txnReceiver);
        });

        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource().withOutcomes(DEFAULT_OUTCOMES_STRINGS).and()
                           .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Receiver manager = transactionReceiver.get();

        assertNotNull(transactionReceiver.get());
        assertNotNull(transactionReceiver.get().getRemoteTarget());

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
}
