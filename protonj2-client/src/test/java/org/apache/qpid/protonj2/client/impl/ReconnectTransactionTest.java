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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionRolledBackException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.test.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test client implementation with Transactions and connection recovery
 */
@Timeout(20)
class ReconnectTransactionTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReconnectTransactionTest.class);

    @Test
    public void testDeclareTransactionAfterConnectionDropsAndReconnects() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.dropAfterLastHandler();
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
            Session session = connection.openSession().openFuture().get();

            firstPeer.waitForScriptToComplete();

            finalPeer.waitForScriptToComplete();
            finalPeer.expectCoordinatorAttach().respond();
            finalPeer.remoteFlow().withLinkCredit(2).queue();
            finalPeer.expectDeclare().accept(txnId);
            finalPeer.expectClose().respond();

            try {
                session.beginTransaction();
            } catch (ClientException cliEx) {
                LOG.info("Caught unexpected error from test", cliEx);
                fail("Should not have failed to declare transaction");
            }

            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTransactionInDoubtAfterReconnect() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectCoordinatorAttach().respond();
            firstPeer.remoteFlow().withLinkCredit(2).queue();
            firstPeer.expectDeclare().accept(txnId);
            firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            firstPeer.remoteFlow().withLinkCredit(1).queue();
            firstPeer.expectTransfer().withNonNullPayload();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession().openFuture().get();

            session.beginTransaction();

            Sender sender = session.openSender("test").openFuture().get();
            sender.send(Message.create("Hello"));

            firstPeer.waitForScriptToComplete();

            finalPeer.waitForScriptToComplete();
            finalPeer.expectClose().respond();

            try {
                session.commitTransaction();
                fail("Should have failed to declare transaction");
            } catch (ClientTransactionRolledBackException cliEx) {
                LOG.info("Caught expected error from test", cliEx);
            }

            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendInTransactionIsNoOpAfterReconnect() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectCoordinatorAttach().respond();
            firstPeer.remoteFlow().withLinkCredit(2).queue();
            firstPeer.expectDeclare().accept(txnId);
            firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            firstPeer.remoteFlow().withLinkCredit(1).queue();
            firstPeer.expectTransfer().withNonNullPayload();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            finalPeer.remoteFlow().withLinkCredit(1).queue();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession().openFuture().get();

            session.beginTransaction();

            Sender sender = session.openSender("test").openFuture().get();
            sender.send(Message.create("Hello"));

            firstPeer.waitForScriptToComplete();

            finalPeer.waitForScriptToComplete();
            finalPeer.expectClose().respond();

            sender.send(Message.create("Hello Again"));

            try {
                session.commitTransaction();
                fail("Should have failed to declare transaction");
            } catch (ClientTransactionRolledBackException cliEx) {
                LOG.info("Caught expected error from test", cliEx);
            }

            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNewTransactionCanBeCreatedAfterOldInstanceRolledBackByReconnect() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            firstPeer.expectCoordinatorAttach().respond();
            firstPeer.remoteFlow().withLinkCredit(2).queue();
            firstPeer.expectDeclare().accept(txnId);
            firstPeer.dropAfterLastHandler(5);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            finalPeer.remoteFlow().withLinkCredit(1).queue();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession().openFuture().get();
            Sender sender = session.openSender("test").openFuture().get();

            session.beginTransaction();

            firstPeer.waitForScriptToComplete();

            finalPeer.waitForScriptToComplete();
            finalPeer.expectCoordinatorAttach().respond();
            finalPeer.remoteFlow().withLinkCredit(2).queue();
            finalPeer.expectDeclare().accept(txnId);
            finalPeer.expectTransfer().withHandle(0)
                                      .withNonNullPayload()
                                      .withState().transactional().withTxnId(txnId).and()
                                      .respond()
                                      .withState().transactional().withTxnId(txnId).withAccepted().and()
                                      .withSettled(true);
            finalPeer.expectDischarge().withFail(false).withTxnId(txnId).accept();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            try {
                session.commitTransaction();
                fail("Should have failed to declare transaction");
            } catch (ClientTransactionRolledBackException cliEx) {
                LOG.info("Caught expected error from test", cliEx);
            }

            session.beginTransaction();

            final Tracker tracker = sender.send(Message.create("test-message"));

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().get());
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.TRANSACTIONAL,
                         "Delivery inside transaction should have Transactional state");
            assertNotNull(tracker.state());
            assertEquals(tracker.state().getType(), DeliveryState.Type.TRANSACTIONAL,
                         "Delivery inside transaction should have Transactional state: " + tracker.state().getType());
            Wait.assertTrue("Delivery in transaction should be locally settled after response", () -> tracker.settled());

            try {
                session.commitTransaction();
            } catch (ClientException cliEx) {
                LOG.info("Caught umexpected error from test", cliEx);
                fail("Should not have failed to declare transaction");
            }

            session.closeAsync();
            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
