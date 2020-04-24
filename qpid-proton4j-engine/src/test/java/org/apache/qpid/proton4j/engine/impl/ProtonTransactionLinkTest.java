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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.transactions.TxnCapability;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.junit.Test;

/**
 * Tests for AMQP transaction over normal {@link Sender} and {@link Receiver} links.
 */
public class ProtonTransactionLinkTest extends ProtonEngineTestSupport {

    @Test(timeout = 20_000)
    public void testCreateCoordinatorSender() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Symbol[] outcomes = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                           Rejected.DESCRIPTOR_SYMBOL,
                                           Released.DESCRIPTOR_SYMBOL,
                                           Modified.DESCRIPTOR_SYMBOL };

        Source source = new Source();
        source.setOutcomes(outcomes);

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
}
