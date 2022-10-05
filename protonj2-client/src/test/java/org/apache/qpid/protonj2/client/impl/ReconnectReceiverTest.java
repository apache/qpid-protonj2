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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that validate Receiver behavior after a client reconnection.
 */
@Timeout(20)
class ReconnectReceiverTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReconnectReceiverTest.class);

    @Test
    public void testOpenedReceiverRecoveredAfterConnectionDroppedCreditWindow() throws Exception {
        doTestOpenedReceiverRecoveredAfterConnectionDropped(false);
    }

    @Test
    public void testOpenedReceiverRecoveredAfterConnectionDroppedFixedCreditGrant() throws Exception {
        doTestOpenedReceiverRecoveredAfterConnectionDropped(true);
    }

    private void doTestOpenedReceiverRecoveredAfterConnectionDropped(boolean fixedCredit) throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            final int FIXED_CREDIT = 25;
            final int CREDIT_WINDOW = 15;

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofReceiver().withSource().withAddress("test").and().respond();
            if (fixedCredit) {
                firstPeer.expectFlow().withLinkCredit(FIXED_CREDIT);
            } else {
                firstPeer.expectFlow().withLinkCredit(CREDIT_WINDOW);
            }
            firstPeer.dropAfterLastHandler(5);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofReceiver().withSource().withAddress("test").and().respond();
            if (fixedCredit) {
                finalPeer.expectFlow().withLinkCredit(FIXED_CREDIT);
            } else {
                finalPeer.expectFlow().withLinkCredit(CREDIT_WINDOW);
            }
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();
            ReceiverOptions receiverOptions = new ReceiverOptions();
            if (fixedCredit) {
                receiverOptions.creditWindow(0);
            } else {
                receiverOptions.creditWindow(CREDIT_WINDOW);
            }

            Receiver receiver = session.openReceiver("test", receiverOptions);
            if (fixedCredit) {
                receiver.addCredit(FIXED_CREDIT);
            }

            firstPeer.waitForScriptToComplete();
            finalPeer.waitForScriptToComplete();
            finalPeer.expectDetach().withClosed(true).respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            receiver.close();
            session.close();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testDynamicReceiverLinkNotRecovered() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofReceiver()
                                    .withSource().withDynamic(true).withAddress((String) null)
                                    .and().respond()
                                    .withSource().withDynamic(true).withAddress("test-dynamic-node");
            firstPeer.dropAfterLastHandler(5);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();
            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(0);
            Receiver receiver = session.openDynamicReceiver(receiverOptions);

            firstPeer.waitForScriptToComplete();
            finalPeer.waitForScriptToComplete();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            try {
                receiver.drain();
                fail("Should not be able to drain as dynamic receiver not recovered");
            } catch (ClientConnectionRemotelyClosedException ex) {
                LOG.trace("Error caught: ", ex);
            }

            receiver.close();
            session.close();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testDispositionFromDeliveryReceivedBeforeDisconnectIsNoOp() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            firstPeer.expectFlow().withLinkCredit(10);
            firstPeer.remoteTransfer().withHandle(0)
                                      .withDeliveryId(0)
                                      .withDeliveryTag(new byte[] { 1 })
                                      .withMore(false)
                                      .withSettled(true)
                                      .withMessageFormat(0)
                                      .withPayload(payload).queue();
            firstPeer.dropAfterLastHandler(100);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            finalPeer.expectFlow().withLinkCredit(9);
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();
            ReceiverOptions rcvOpts = new ReceiverOptions().autoAccept(false);
            Receiver receiver = session.openReceiver("test-queue", rcvOpts);
            Delivery delivery = receiver.receive(10, TimeUnit.SECONDS);

            firstPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            finalPeer.waitForScriptToComplete();
            finalPeer.expectDetach().respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            assertNotNull(delivery);

            delivery.accept();

            receiver.close();
            session.close();
            connection.close();

            assertNotNull(delivery);
        }
    }

    @Test
    public void testReceiverWaitsWhenConnectionForcedDisconnect() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            firstPeer.expectFlow().withLinkCredit(10);
            firstPeer.remoteClose()
                     .withErrorCondition(ConnectionError.CONNECTION_FORCED.toString(), "Forced disconnect").queue().afterDelay(20);
            firstPeer.expectClose();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            finalPeer.expectFlow().withLinkCredit(10);
            finalPeer.remoteTransfer().withHandle(0)
                                      .withDeliveryId(0)
                                      .withDeliveryTag(new byte[] { 1 })
                                      .withMore(false)
                                      .withSettled(true)
                                      .withMessageFormat(0)
                                      .withPayload(payload).queue().afterDelay(5);
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();
            ReceiverOptions rcvOpts = new ReceiverOptions().autoAccept(false);
            Receiver receiver = session.openReceiver("test-queue", rcvOpts);

            Delivery delivery = null;
            try {
                delivery = receiver.receive(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                fail("Should not have failed on blocking receive call." + ex.getMessage());
            }

            assertNotNull(delivery);

            firstPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            finalPeer.waitForScriptToComplete();
            finalPeer.expectDetach().respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            delivery.accept();

            receiver.close();
            session.close();
            connection.close();

            assertNotNull(delivery);
        }
    }
}
