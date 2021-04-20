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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests that validate Sender behavior after a client reconnection.
 */
@Timeout(20)
class ReconnectSenderTest extends ImperativeClientTestCase {

    @Test
    public void testOpenedSenderRecoveredAfterConnectionDropped() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            firstPeer.dropAfterLastHandler(5);
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
            options.reconnectOptions().addReconnectHost(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();
            Sender sender = session.openSender("test");

            firstPeer.waitForScriptToComplete();
            finalPeer.waitForScriptToComplete();
            finalPeer.expectDetach().withClosed(true).respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            sender.close();
            session.close();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testInFlightSendFailedAfterConnectionDroppedAndNotResent() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

           firstPeer.expectSASLAnonymousConnect();
           firstPeer.expectOpen().respond();
           firstPeer.expectBegin().respond();
           firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
           firstPeer.remoteFlow().withLinkCredit(1).queue();
           firstPeer.expectTransfer().withNonNullPayload();
           firstPeer.dropAfterLastHandler(15);
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
           options.reconnectOptions().addReconnectHost(backupURI.getHost(), backupURI.getPort());

           Client container = Client.create();
           Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
           Session session = connection.openSession();
           Sender sender = session.openSender("test");

           final AtomicReference<Tracker> tracker = new AtomicReference<>();
           final AtomicReference<ClientException> error = new AtomicReference<>();
           final CountDownLatch latch = new CountDownLatch(1);

           ForkJoinPool.commonPool().execute(() -> {
               try {
                   tracker.set(sender.send(Message.create("Hello")));
               } catch (ClientException e) {
                   error.set(e);
               } finally {
                   latch.countDown();
               }
           });

           firstPeer.waitForScriptToComplete();
           finalPeer.waitForScriptToComplete();
           finalPeer.expectDetach().withClosed(true).respond();
           finalPeer.expectEnd().respond();
           finalPeer.expectClose().respond();

           assertTrue(latch.await(10, TimeUnit.SECONDS), "Should have failed previously sent message");
           assertNotNull(tracker.get());
           assertNull(error.get());
           assertThrows(ClientConnectionRemotelyClosedException.class, () -> tracker.get().awaitSettlement());

           sender.close();
           session.close();
           connection.close();

           finalPeer.waitForScriptToComplete();
       }
    }

    @Test
    public void testSendBlockedOnCreditGetsSentAfterReconnectAndCreditGranted() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

           firstPeer.expectSASLAnonymousConnect();
           firstPeer.expectOpen().respond();
           firstPeer.expectBegin().respond();
           firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
           firstPeer.dropAfterLastHandler(15);
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
           options.reconnectOptions().addReconnectHost(backupURI.getHost(), backupURI.getPort());

           Client container = Client.create();
           Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
           Session session = connection.openSession();
           Sender sender = session.openSender("test");

           final AtomicReference<Tracker> tracker = new AtomicReference<>();
           final AtomicReference<Exception> sendError = new AtomicReference<>();
           final CountDownLatch latch = new CountDownLatch(1);

           ForkJoinPool.commonPool().execute(() -> {
               try {
                   tracker.set(sender.send(Message.create("Hello")));
               } catch (ClientException e) {
                   sendError.set(e);
               } finally {
                   latch.countDown();
               }
           });

           firstPeer.waitForScriptToComplete();
           finalPeer.waitForScriptToComplete();
           finalPeer.expectTransfer().withNonNullPayload()
                                     .respond()
                                     .withSettled(true).withState().accepted();
           finalPeer.expectDetach().withClosed(true).respond();
           finalPeer.expectEnd().respond();
           finalPeer.expectClose().respond();

           // Grant credit now and await expected message send.
           finalPeer.remoteFlow().withDeliveryCount(0)
                                 .withLinkCredit(10)
                                 .withIncomingWindow(10)
                                 .withOutgoingWindow(10)
                                 .withNextIncomingId(0)
                                 .withNextOutgoingId(1).now();

           assertTrue(latch.await(10, TimeUnit.SECONDS), "Should have sent blocked message");
           assertNull(sendError.get());
           assertNotNull(tracker.get());

           Tracker send = tracker.get();
           assertSame(tracker.get(), send.awaitSettlement(10, TimeUnit.SECONDS));
           assertTrue(send.remoteSettled());
           assertEquals(DeliveryState.accepted(), send.remoteState());

           sender.close();
           session.close();
           connection.close();

           finalPeer.waitForScriptToComplete();
       }
    }

    @Test
    public void testAwaitSettlementOnSendFiredBeforeConnectionDrops() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

           firstPeer.expectSASLAnonymousConnect();
           firstPeer.expectOpen().respond();
           firstPeer.expectBegin().respond();
           firstPeer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
           firstPeer.remoteFlow().withLinkCredit(1).queue();
           firstPeer.expectTransfer().withNonNullPayload();
           firstPeer.dropAfterLastHandler(15);
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
           options.reconnectOptions().addReconnectHost(backupURI.getHost(), backupURI.getPort());

           Client container = Client.create();
           Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
           Session session = connection.openSession();
           Sender sender = session.openSender("test");
           Tracker tracker = sender.send(Message.create("Hello"));

           firstPeer.waitForScriptToComplete();
           finalPeer.waitForScriptToComplete();
           finalPeer.expectDetach().withClosed(true).respond();
           finalPeer.expectEnd().respond();
           finalPeer.expectClose().respond();

           try {
               tracker.awaitSettlement(120, TimeUnit.SECONDS);
               fail("Should not be able to successfully await settlement");
           } catch (ClientConnectionRemotelyClosedException crce) {}

           assertFalse(tracker.remoteSettled());
           assertNull(tracker.remoteState());

           sender.close();
           session.close();
           connection.close();

           finalPeer.waitForScriptToComplete();
       }
    }
}
