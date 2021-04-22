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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.DistributionMode;
import org.apache.qpid.protonj2.client.DurabilityMode;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.ExpiryPolicy;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Source;
import org.apache.qpid.protonj2.client.Target;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRedirectedException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Released;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusDurability;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.DeliveryAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.LinkError;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(20)
public class SenderTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SenderTest.class);

    @Test
    public void testCreateSenderAndClose() throws Exception {
        doTestCreateSenderAndCloseOrDeatch(true);
    }

    @Test
    public void testCreateSenderAndDetach() throws Exception {
        doTestCreateSenderAndCloseOrDeatch(false);
    }

    private void doTestCreateSenderAndCloseOrDeatch(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                sender.closeAsync().get(10, TimeUnit.SECONDS);
            } else {
                sender.detachAsync().get(10, TimeUnit.SECONDS);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateSenderAndCloseSync() throws Exception {
        doTestCreateSenderAndCloseOrDeatchSync(true);
    }

    @Test
    public void testCreateSenderAndDetachSync() throws Exception {
        doTestCreateSenderAndCloseOrDeatchSync(false);
    }

    private void doTestCreateSenderAndCloseOrDeatchSync(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                sender.close();
            } else {
                sender.detach();
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateSenderAndCloseWithErrorSync() throws Exception {
        doTestCreateSenderAndCloseOrDeatchWithErrorSync(true);
    }

    @Test
    public void testCreateSenderAndDetachWithErrorSync() throws Exception {
        doTestCreateSenderAndCloseOrDeatchWithErrorSync(false);
    }

    private void doTestCreateSenderAndCloseOrDeatchWithErrorSync(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().withError("amqp-resource-deleted", "an error message").withClosed(close).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                sender.close(ErrorCondition.create("amqp-resource-deleted", "an error message", null));
            } else {
                sender.detach(ErrorCondition.create("amqp-resource-deleted", "an error message", null));
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSenderOpenRejectedByRemote() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond().withNullTarget();
            peer.remoteDetach().withErrorCondition(AmqpError.UNAUTHORIZED_ACCESS.toString(), "Cannot read from this address").queue();
            peer.expectDetach();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            try {
                sender.openFuture().get(10, TimeUnit.SECONDS);
                fail("Open of sender should fail due to remote indicating pending close.");
            } catch (ExecutionException exe) {
                assertNotNull(exe.getCause());
                assertTrue(exe.getCause() instanceof ClientLinkRemotelyClosedException);
                ClientLinkRemotelyClosedException linkClosed = (ClientLinkRemotelyClosedException) exe.getCause();
                assertNotNull(linkClosed.getErrorCondition());
                assertEquals(AmqpError.UNAUTHORIZED_ACCESS.toString(), linkClosed.getErrorCondition().condition());
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not result in any close being sent now, already closed.
            sender.closeAsync().get(10, TimeUnit.SECONDS);

            peer.expectClose().respond();
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRemotelyCloseSenderLinkWithRedirect() throws Exception {
        final String redirectVhost = "vhost";
        final String redirectNetworkHost = "localhost";
        final String redirectAddress = "redirect-queue";
        final int redirectPort = 5677;
        final String redirectScheme = "wss";
        final String redirectPath = "/websockets";

        // Tell the test peer to close the connection when executing its last handler
        final Map<String, Object> errorInfo = new HashMap<>();
        errorInfo.put(ClientConstants.OPEN_HOSTNAME.toString(), redirectVhost);
        errorInfo.put(ClientConstants.NETWORK_HOST.toString(), redirectNetworkHost);
        errorInfo.put(ClientConstants.PORT.toString(), redirectPort);
        errorInfo.put(ClientConstants.SCHEME.toString(), redirectScheme);
        errorInfo.put(ClientConstants.PATH.toString(), redirectPath);
        errorInfo.put(ClientConstants.ADDRESS.toString(), redirectAddress);

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond().withNullTarget();
            peer.remoteDetach().withClosed(true)
                               .withErrorCondition(LinkError.REDIRECT.toString(), "Not accepting links here", errorInfo).queue();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");

            try {
                sender.openFuture().get();
                fail("Should not be able to create sender since the remote is redirecting.");
            } catch (Exception ex) {
                LOG.debug("Received expected exception from sender open: {}", ex.getMessage());
                Throwable cause = ex.getCause();
                assertTrue(cause instanceof ClientLinkRedirectedException);

                ClientLinkRedirectedException linkRedirect = (ClientLinkRedirectedException) ex.getCause();

                assertEquals(redirectVhost, linkRedirect.getHostname());
                assertEquals(redirectNetworkHost, linkRedirect.getNetworkHost());
                assertEquals(redirectPort, linkRedirect.getPort());
                assertEquals(redirectScheme, linkRedirect.getScheme());
                assertEquals(redirectPath, linkRedirect.getPath());
                assertEquals(redirectAddress, linkRedirect.getAddress());

                URI redirect = linkRedirect.getRedirectionURI();

                assertEquals(redirectNetworkHost, redirect.getHost());
                assertEquals(redirectPort, redirect.getPort());
                assertEquals(redirectScheme, redirect.getScheme());
                assertEquals(redirectPath, redirect.getPath());
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenSenderTimesOutWhenNoAttachResponseReceivedTimeout() throws Exception {
        doTestOpenSenderTimesOutWhenNoAttachResponseReceived(true);
    }

    @Test
    public void testOpenSenderTimesOutWhenNoAttachResponseReceivedNoTimeout() throws Exception {
        doTestOpenSenderTimesOutWhenNoAttachResponseReceived(false);
    }

    private void doTestOpenSenderTimesOutWhenNoAttachResponseReceived(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();
            Sender sender = session.openSender("test-queue", new SenderOptions().openTimeout(10));

            try {
                if (timeout) {
                    sender.openFuture().get(20, TimeUnit.SECONDS);
                } else {
                    sender.openFuture().get();
                }

                fail("Should not complete the open future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                assertTrue(cause instanceof ClientOperationTimedOutException);
            }

            LOG.info("Closing connection after waiting for sender open");

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenSenderWaitWithTimeoutFailsWhenConnectionDrops() throws Exception {
        doTestOpenSenderWaitFailsWhenConnectionDrops(true);
    }

    @Test
    public void testOpenSenderWaitWithNoTimeoutFailsWhenConnectionDrops() throws Exception {
        doTestOpenSenderWaitFailsWhenConnectionDrops(false);
    }

    private void doTestOpenSenderWaitFailsWhenConnectionDrops(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");

            Thread.sleep(10);

            try {
                if (timeout) {
                    sender.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    sender.openFuture().get();
                }

                fail("Should not complete the open future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                LOG.trace("Caught exception caused by: {}", exe);
                assertTrue(cause instanceof ClientConnectionRemotelyClosedException);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseSenderTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(true, true);
    }

    @Test
    public void testCloseSenderTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(true, false);
    }

    @Test
    public void testDetachSenderTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(false, true);
    }

    @Test
    public void testDetachSenderTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(false, false);
    }

    private void doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(boolean close, boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.closeTimeout(10);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            try {
                if (close) {
                    if (timeout) {
                        sender.closeAsync().get(10, TimeUnit.SECONDS);
                    } else {
                        sender.closeAsync().get();
                    }
                } else {
                    if (timeout) {
                        sender.detachAsync().get(10, TimeUnit.SECONDS);
                    } else {
                        sender.detachAsync().get();
                    }
                }

                fail("Should not complete the close or detach future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                assertTrue(cause instanceof ClientOperationTimedOutException);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendTimesOutWhenNoCreditIssued() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.sendTimeout(1);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            Message<String> message = Message.create("Hello World");
            try {
                sender.send(message);
                fail("Should throw a send timed out exception");
            } catch (ClientSendTimedOutException ex) {
                // Expected error, ignore
            }

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendCompletesWhenCreditEventuallyOffered() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.sendTimeout(200);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Expect a transfer but only after the flow which is delayed to allow the
            // client time to block on credit.
            peer.expectTransfer().withNonNullPayload();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(1)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).later(30);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");
            try {
                LOG.debug("Attempting send with sender: {}", sender);
                sender.send(message);
            } catch (ClientSendTimedOutException ex) {
                fail("Should not throw a send timed out exception");
            }

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendWhenCreditIsAvailable() throws Exception {
        doTestSendWhenCreditIsAvailable(false, false);
    }

    @Test
    public void testTrySendWhenCreditIsAvailable() throws Exception {
        doTestSendWhenCreditIsAvailable(true, false);
    }

    @Test
    public void testSendWhenCreditIsAvailableWithDeliveryAnnotations() throws Exception {
        doTestSendWhenCreditIsAvailable(false, true);
    }

    @Test
    public void testTrySendWhenCreditIsAvailableWithDeliveryAnnotations() throws Exception {
        doTestSendWhenCreditIsAvailable(true, true);
    }

    private void doTestSendWhenCreditIsAvailable(boolean trySend, boolean addDeliveryAnnotations) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).queue();
            peer.expectAttach().ofReceiver().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            Map<String, Object> deliveryAnnotations = new HashMap<>();
            deliveryAnnotations.put("da1", 1);
            deliveryAnnotations.put("da2", 2);
            deliveryAnnotations.put("da3", 3);
            DeliveryAnnotationsMatcher daMatcher = new DeliveryAnnotationsMatcher(true);
            daMatcher.withEntry("da1", Matchers.equalTo(1));
            daMatcher.withEntry("da2", Matchers.equalTo(2));
            daMatcher.withEntry("da3", Matchers.equalTo(3));
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            if (addDeliveryAnnotations) {
                payloadMatcher.setDeliveryAnnotationsMatcher(daMatcher);
            }
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            final Tracker tracker;
            if (trySend) {
                if (addDeliveryAnnotations) {
                    tracker = sender.trySend(message, deliveryAnnotations);
                } else {
                    tracker = sender.trySend(message);
                }
            } else {
                if (addDeliveryAnnotations) {
                    tracker = sender.send(message, deliveryAnnotations);
                } else {
                    tracker = sender.send(message);
                }
            }

            assertNotNull(tracker);

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTrySendWhenNoCreditAvailable() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.sendTimeout(1);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            Message<String> message = Message.create("Hello World");
            assertNull(sender.trySend(message));

            sender.closeAsync().get(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateSenderWithQoSOfAtMostOnce() throws Exception {
        doTestCreateSenderWithConfiguredQoS(DeliveryMode.AT_MOST_ONCE);
    }

    @Test
    public void testCreateSenderWithQoSOfAtLeastOnce() throws Exception {
        doTestCreateSenderWithConfiguredQoS(DeliveryMode.AT_LEAST_ONCE);
    }

    private void doTestCreateSenderWithConfiguredQoS(DeliveryMode qos) throws Exception {
        byte sndMode = qos == DeliveryMode.AT_MOST_ONCE ? SenderSettleMode.SETTLED.byteValue() : SenderSettleMode.UNSETTLED.byteValue();

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender()
                               .withSndSettleMode(sndMode)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST.byteValue())
                               .respond()
                               .withSndSettleMode(sndMode)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST.byteValue());
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();

            SenderOptions options = new SenderOptions().deliveryMode(qos);
            Sender sender = session.openSender("test-qos", options);
            sender.openFuture().get();

            assertEquals("test-qos", sender.address());

            sender.closeAsync();

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendAutoSettlesOnceRemoteSettles() throws Exception {
        doTestSentMessageGetsAutoSettledAfterRemtoeSettles(false);
    }

    @Test
    public void testTrySendAutoSettlesOnceRemoteSettles() throws Exception {
        doTestSentMessageGetsAutoSettledAfterRemtoeSettles(true);
    }

    private void doTestSentMessageGetsAutoSettledAfterRemtoeSettles(boolean trySend) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).queue();
            peer.expectAttach().ofReceiver().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload()
                                 .respond()
                                 .withSettled(true).withState().accepted();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().get(5, TimeUnit.SECONDS));
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.ACCEPTED);

            sender.closeAsync();

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendDoesNotAutoSettlesOnceRemoteSettlesIfAutoSettleOff() throws Exception {
        doTestSentMessageNotAutoSettledAfterRemtoeSettles(false);
    }

    @Test
    public void testTrySendDoesNotAutoSettlesOnceRemoteSettlesIfAutoSettleOff() throws Exception {
        doTestSentMessageNotAutoSettledAfterRemtoeSettles(true);
    }

    private void doTestSentMessageNotAutoSettledAfterRemtoeSettles(boolean trySend) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).queue();
            peer.expectAttach().ofReceiver().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue", new SenderOptions().autoSettle(false));
            sender.openFuture().get(10, TimeUnit.SECONDS);

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload()
                                 .respond()
                                 .withSettled(true).withState().accepted();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().get(5, TimeUnit.SECONDS));
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.ACCEPTED);
            assertNull(tracker.state());
            assertFalse(tracker.settled());

            sender.closeAsync();

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSenderSendingSettledCompletesTrackerAcknowledgeFuture() throws Exception {
        doTestSenderSendingSettledCompletesTrackerAcknowledgeFuture(false);
    }

    @Test
    public void testSenderTrySendingSettledCompletesTrackerAcknowledgeFuture() throws Exception {
        doTestSenderSendingSettledCompletesTrackerAcknowledgeFuture(true);
    }

    private void doTestSenderSendingSettledCompletesTrackerAcknowledgeFuture(boolean trySend) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender()
                               .withSenderSettleModeSettled()
                               .withReceivervSettlesFirst()
                               .respond()
                               .withSenderSettleModeSettled()
                               .withReceivervSettlesFirst();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();

            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);
            assertEquals("test-qos", sender.address());
            session.openReceiver("dummy").openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");
            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSenderIncrementsTransferTagOnEachSend() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_LEAST_ONCE).autoSettle(false);
            Sender sender = session.openSender("test-tags", options).openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {0}).respond().withSettled(true).withState().accepted();
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {1}).respond().withSettled(true).withState().accepted();
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {2}).respond().withSettled(true).withState().accepted();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");
            final Tracker tracker1 = sender.send(message);
            final Tracker tracker2 = sender.send(message);
            final Tracker tracker3 = sender.send(message);

            assertNotNull(tracker1);
            assertNotNull(tracker1.settlementFuture().get().settled());
            assertNotNull(tracker2);
            assertNotNull(tracker2.settlementFuture().get().settled());
            assertNotNull(tracker3);
            assertNotNull(tracker3.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSenderSendsSettledInAtLeastOnceMode() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE).autoSettle(false);
            Sender sender = session.openSender("test-tags", options).openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {}).withSettled(true);
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {}).withSettled(true);
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {}).withSettled(true);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");
            final Tracker tracker1 = sender.send(message);
            final Tracker tracker2 = sender.send(message);
            final Tracker tracker3 = sender.send(message);

            assertNotNull(tracker1);
            assertNotNull(tracker1.settlementFuture().get().settled());
            assertNotNull(tracker2);
            assertNotNull(tracker2.settlementFuture().get().settled());
            assertNotNull(tracker3);
            assertNotNull(tracker3.settlementFuture().get().settled());

            sender.closeAsync().get();

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateAnonymousSenderWhenRemoteDoesNotOfferSupportForIt() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            try {
                session.openAnonymousSender();
                fail("Should not be able to open an anonymous sender when remote does not offer anonymous relay");
            } catch (ClientUnsupportedOperationException unsupported) {
                LOG.info("Caught expected error: ", unsupported);
            }

            connection.closeAsync();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateAnonymousSenderBeforeKnowingRemoteDoesNotOfferSupportForIt() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender anonymousSender = session.openAnonymousSender();
            Message<String> message = Message.create("Hello World").to("my-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.remoteOpen().now();
            peer.respondToLastBegin().now();
            peer.expectClose().respond();

            try {
                anonymousSender.send(message);
                fail("Should not be able to open an anonymous sender when remote does not offer anonymous relay");
            } catch (ClientUnsupportedOperationException unsupported) {
                LOG.info("Caught expected error: ", unsupported);
            }

            connection.closeAsync();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateAnonymousSenderAppliesOptions() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond().withOfferedCapabilities("ANONYMOUS-RELAY");
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withSenderSettleModeSettled()
                                          .withReceivervSettlesFirst()
                                          .withTarget().withAddress(Matchers.nullValue()).and().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            SenderOptions senderOptions = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender anonymousSender = session.openAnonymousSender(senderOptions);

            anonymousSender.openFuture().get();

            connection.closeAsync();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAnonymousSenderOpenHeldUntilConnectionOpenedAndSupportConfirmed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openAnonymousSender();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // This should happen after we inject the held open and attach
            peer.expectAttach().ofSender().withTarget().withAddress(Matchers.nullValue()).and().respond();
            peer.expectClose().respond();

            // Inject held responses to get the ball rolling again
            peer.remoteOpen().withOfferedCapabilities("ANONYMOUS-RELAY").now();
            peer.respondToLastBegin().now();

            try {
                sender.openFuture().get();
            } catch (ExecutionException ex) {
                fail("Open of Sender failed waiting for response: " + ex.getCause());
            }

            connection.closeAsync();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSenderGetRemotePropertiesWaitsForRemoteAttach() throws Exception {
        tryReadSenderRemoteProperties(true);
    }

    @Test
    public void testSenderGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadSenderRemoteProperties(false);
    }

    private void tryReadSenderRemoteProperties(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            SenderOptions options = new SenderOptions().openTimeout(75);
            Sender sender = session.openSender("test-sender", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withPropertiesMap(expectedProperties).later(10);
            } else {
                peer.expectDetach();
            }

            if (attachResponse) {
                assertNotNull(sender.properties(), "Remote should have responded with a remote properties value");
                assertEquals(expectedProperties, sender.properties());
            } else {
                try {
                    sender.properties();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                sender.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close when connection not closed and detach sent.");
            }

            LOG.debug("*** Test read remote properties ***");

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testGetRemoteOfferedCapabilitiesWaitsForRemoteAttach() throws Exception {
        tryReadRemoteOfferedCapabilities(true);
    }

    @Test
    public void testGetRemoteOfferedCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadRemoteOfferedCapabilities(false);
    }

    private void tryReadRemoteOfferedCapabilities(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(75);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Sender sender = session.openSender("test-sender");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withOfferedCapabilities("QUEUE").later(10);
            } else {
                peer.expectDetach();
            }

            if (attachResponse) {
                assertNotNull(sender.offeredCapabilities(), "Remote should have responded with a remote offered Capabilities value");
                assertEquals(1, sender.offeredCapabilities().length);
                assertEquals("QUEUE", sender.offeredCapabilities()[0]);
            } else {
                try {
                    sender.offeredCapabilities();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                sender.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close when connection not closed and detach sent.");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testGetRemoteDesiredCapabilitiesWaitsForRemoteAttach() throws Exception {
        tryReadRemoteDesiredCapabilities(true);
    }

    @Test
    public void testGetRemoteDesiredCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadRemoteDesiredCapabilities(false);
    }

    private void tryReadRemoteDesiredCapabilities(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(75);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Sender sender = session.openSender("test-sender");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectDetach();
            }

            if (attachResponse) {
                assertNotNull(sender.desiredCapabilities(), "Remote should have responded with a remote desired Capabilities value");
                assertEquals(1, sender.desiredCapabilities().length);
                assertEquals("Error-Free", sender.desiredCapabilities()[0]);
            } else {
                try {
                    sender.desiredCapabilities();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                sender.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close when connection not closed and detach sent.");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenSenderWithLinCapabilities() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER.getValue())
                               .withTarget().withCapabilities("queue").and()
                               .respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Receiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get(10, TimeUnit.SECONDS);
            SenderOptions senderOptions = new SenderOptions();
            senderOptions.targetOptions().capabilities("queue");
            Sender sender = session.openSender("test-queue", senderOptions);

            sender.openFuture().get();
            sender.close();

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseSenderWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(true);
    }

    @Test
    public void testDetachSenderWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(false);
    }

    public void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
        final String condition = "amqp:link:detach-forced";
        final String description = "something bad happened.";

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().withClosed(close).withError(condition, description).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-sender");
            sender.openFuture().get();

            if (close) {
                sender.closeAsync(ErrorCondition.create(condition, description, null));
            } else {
                sender.detachAsync(ErrorCondition.create(condition, description, null));
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMultipleMessages() throws Exception {
        final int CREDIT = 20;

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withDeliveryCount(0).withLinkCredit(CREDIT).queue();
            peer.expectAttach().ofReceiver().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get();

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final List<Tracker> sentMessages = new ArrayList<>();

            for (int i = 0; i < CREDIT; ++i) {
                peer.expectTransfer().withDeliveryId(i)
                                     .withNonNullPayload()
                                     .withSettled(false)
                                     .respond()
                                     .withSettled(true)
                                     .withState().accepted();
            }
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            for (int i = 0; i < CREDIT; ++i) {
                final Tracker tracker = sender.send(message);
                sentMessages.add(tracker);
                tracker.settlementFuture().get();
            }
            assertEquals(CREDIT, sentMessages.size());

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendBlockedForCreditFailsWhenLinkRemotelyClosed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteDetach().withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Link was deleted").afterDelay(25).queue();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get();

            Message<String> message = Message.create("Hello World");

            try {
                sender.send(message);
                fail("Send should have timed out.");
            } catch (ClientResourceRemotelyClosedException cliEx) {
                // Expected send to throw indicating that the remote closed the link
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendBlockedForCreditFailsWhenSessionRemotelyClosed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteEnd().withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Session was deleted").afterDelay(25).queue();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get();

            Message<String> message = Message.create("Hello World");

            try {
                sender.send(message);
                fail("Send should have timed out.");
            } catch (ClientResourceRemotelyClosedException cliEx) {
                // Expected send to throw indicating that the remote closed the session
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendBlockedForCreditFailsWhenConnectionRemotelyClosed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteClose().withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Connection was deleted").afterDelay(25).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get();

            Message<String> message = Message.create("Hello World");

            try {
                sender.send(message);
                fail("Send should have failed when Connection remotely closed.");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected send to throw indicating that the remote closed the connection
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendBlockedForCreditFailsWhenConnectionDrops() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.dropAfterLastHandler(25);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");
            sender.openFuture().get();

            Message<String> message = Message.create("Hello World");

            try {
                sender.send(message);
                fail("Send should have timed out.");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected send to throw indicating that the remote closed unexpectedly
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendAfterConnectionDropsThrowsConnectionRemotelyClosedError() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            peer.dropAfterLastHandler(25);
            peer.start();

            final CountDownLatch dropped = new CountDownLatch(1);

            ConnectionOptions options = new ConnectionOptions();
            options.disconnectedHandler((connection, event) -> {
                dropped.countDown();
            });

            URI remoteURI = peer.getServerURI();

            Message<String> message = Message.create("test-message");
            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();
            Sender sender = session.openSender("test");

            assertTrue(dropped.await(10, TimeUnit.SECONDS));

            try {
                sender.send(message);
                fail("Send should fail with remotely closed error after remote drops");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected
            }

            try {
                sender.trySend(message);
                fail("trySend should fail with remotely closed error after remote drops");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAwaitSettlementFutureFailedAfterConnectionDropped() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer();
            peer.dropAfterLastHandler();
            peer.start();

            URI remoteURI = peer.getServerURI();

            Message<String> message = Message.create("test-message");
            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test");

            Tracker tracker = null;
            try {
                tracker = sender.send(message);
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                fail("Send not should fail with remotely closed error after remote drops");
            }

            // Connection should be dropped at this point and next call should test that after
            // the drop the future has been completed
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            try {
                tracker.settlementFuture().get();
                fail("Wait for settlement should fail with remotely closed error after remote drops");
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionRemotelyClosedException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAwaitSettlementFailedOnConnectionDropped() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer();
            peer.dropAfterLastHandler(30);
            peer.start();

            URI remoteURI = peer.getServerURI();

            Message<String> message = Message.create("test-message");
            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test");

            Tracker tracker = null;
            try {
                tracker = sender.send(message);
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                fail("Send should not fail with remotely closed error after remote drops");
            }

            // Most of the time this should await before connection drops testing that
            // the drop completes waiting callers.
            try {
                tracker.awaitSettlement();
                fail("Wait for settlement should fail with remotely closed error after remote drops");
            } catch (ClientConnectionRemotelyClosedException cliRCEx) {
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBlockedSendThrowsConnectionRemotelyClosedError() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withTarget().withAddress("test").and().respond();
            peer.dropAfterLastHandler(25);
            peer.start();

            URI remoteURI = peer.getServerURI();

            Message<String> message = Message.create("test-message");
            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test");

            try {
                sender.send(message);
                fail("Send should fail with remotely closed error after remote drops");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testAutoFlushDuringWriteThatExceedConfiguredBufferLimitSessionCreditLimitOnTransfer() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().withNextOutgoingId(0).respond();
            peer.expectAttach().ofSender().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().maxFrameSize(1024);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Sender sender = connection.openSender("test-queue");

            final byte[] payload = new byte[4800];
            Arrays.fill(payload, (byte) 1);

            final AtomicBoolean sendFailed = new AtomicBoolean();
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    sender.send(Message.create(payload));
                } catch (Exception e) {
                    LOG.info("send failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(1).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(2).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(3).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(4).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).accept();

            // Grant the credit to start meeting the above expectations
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(0).withLinkCredit(10).now();

            peer.waitForScriptToComplete(500, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            assertFalse(sendFailed.get());

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testAutoFlushDuringWriteWithRollingIncomingWindowUpdates() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().withNextOutgoingId(0).respond();
            peer.expectAttach().ofSender().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().maxFrameSize(1024);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Sender sender = connection.openSender("test-queue");

            final byte[] payload = new byte[4800];
            Arrays.fill(payload, (byte) 1);

            final AtomicBoolean sendFailed = new AtomicBoolean();
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    sender.send(Message.create(payload));
                } catch (Exception e) {
                    LOG.info("send failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            // Credit should will be refilling as transfers arrive vs being exhausted on each
            // incoming transfer and the send awaiting more credit.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(2).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(3).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(4).withLinkCredit(10).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.expectTransfer().withNonNullPayload().withMore(false).accept();

            // Grant the credit to start meeting the above expectations
            peer.remoteFlow().withIncomingWindow(2).withNextIncomingId(0).withLinkCredit(10).now();

            peer.waitForScriptToComplete(500, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            assertFalse(sendFailed.get());

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testConcurrentSendOnlyBlocksForInitialSendInProgress() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.openSender("test-queue").openFuture().get();
            // Ensure that sender gets its flow before the sends are triggered.
            connection.openReceiver("test-queue").openFuture().get();

            final byte[] payload = new byte[1024];
            Arrays.fill(payload, (byte) 1);

            // One should block on the send waiting for the others send to finish
            // otherwise they should not care about concurrency of sends.

            final AtomicBoolean sendFailed = new AtomicBoolean();
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    LOG.info("Test send 1 is preparing to fire:");
                    Tracker tracker = sender.send(Message.create(payload));
                    tracker.awaitSettlement(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOG.info("Test send 1 failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    LOG.info("Test send 2 is preparing to fire:");
                    Tracker tracker = sender.send(Message.create(payload));
                    tracker.awaitSettlement(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOG.info("Test send 2 failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            assertFalse(sendFailed.get());

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testConcurrentSendBlocksBehindSendWaitingForCredit() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.openSender("test-queue").openFuture().get();

            final byte[] payload = new byte[1024];
            Arrays.fill(payload, (byte) 1);

            final CountDownLatch send1Started = new CountDownLatch(1);
            final CountDownLatch send2Completed = new CountDownLatch(1);

            final AtomicBoolean sendFailed = new AtomicBoolean();
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    LOG.info("Test send 1 is preparing to fire:");
                    ForkJoinPool.commonPool().execute(() -> send1Started.countDown());
                    sender.send(Message.create(payload));
                } catch (Exception e) {
                    LOG.info("Test send 1 failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    assertTrue(send1Started.await(10, TimeUnit.SECONDS));
                    LOG.info("Test send 2 is preparing to fire:");
                    Tracker tracker = sender.send(Message.create(payload));
                    tracker.awaitSettlement(10, TimeUnit.SECONDS);
                    send2Completed.countDown();
                } catch (Exception e) {
                    LOG.info("Test send 2 failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.remoteFlow().withIncomingWindow(1).withDeliveryCount(0).withNextIncomingId(1).withLinkCredit(1).now();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();
            peer.remoteFlow().withIncomingWindow(1).withDeliveryCount(1).withNextIncomingId(2).withLinkCredit(1).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();

            assertTrue(send2Completed.await(10, TimeUnit.SECONDS));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            assertFalse(sendFailed.get());

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testConcurrentSendWaitingOnSplitFramedSendToCompleteIsSentAfterCreditUpdated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().maxFrameSize(1024);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Sender sender = connection.openSender("test-queue");

            final byte[] payload = new byte[1536];
            Arrays.fill(payload, (byte) 1);

            final CountDownLatch send1Started = new CountDownLatch(1);
            final CountDownLatch send2Completed = new CountDownLatch(1);

            final AtomicBoolean sendFailed = new AtomicBoolean();
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    LOG.info("Test send 1 is preparing to fire:");
                    ForkJoinPool.commonPool().execute(() -> send1Started.countDown());
                    sender.send(Message.create(payload));
                } catch (Exception e) {
                    LOG.info("Test send 1 failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    assertTrue(send1Started.await(10, TimeUnit.SECONDS));
                    LOG.info("Test send 2 is preparing to fire:");
                    Tracker tracker = sender.send(Message.create(payload));
                    tracker.awaitSettlement(10, TimeUnit.SECONDS);
                    send2Completed.countDown();
                } catch (Exception e) {
                    LOG.info("Test send 2 failed with error: ", e);
                    sendFailed.set(true);
                }
            });

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.remoteFlow().withIncomingWindow(1).withDeliveryCount(0).withNextIncomingId(1).withLinkCredit(1).now();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withDeliveryCount(0).withNextIncomingId(2).withLinkCredit(1).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();
            peer.remoteFlow().withIncomingWindow(1).withDeliveryCount(1).withNextIncomingId(3).withLinkCredit(1).queue();
            peer.expectTransfer().withNonNullPayload().withMore(true);
            peer.remoteFlow().withIncomingWindow(1).withDeliveryCount(1).withNextIncomingId(4).withLinkCredit(1).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();

            assertTrue(send2Completed.await(10, TimeUnit.SECONDS));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            assertFalse(sendFailed.get());

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateSenderWithDefaultSourceAndTargetOptions() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender()
                               .withSource().withAddress(notNullValue())
                                            .withDistributionMode(nullValue())
                                            .withDefaultTimeout()
                                            .withDurable(TerminusDurability.NONE)
                                            .withExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH)
                                            .withDefaultOutcome(nullValue())
                                            .withCapabilities(nullValue())
                                            .withFilter(nullValue())
                                            .withOutcomes("amqp:accepted:list", "amqp:rejected:list", "amqp:released:list", "amqp:modified:list")
                                            .also()
                               .withTarget().withAddress("test-queue")
                                            .withCapabilities(nullValue())
                                            .withDurable(nullValue())
                                            .withExpiryPolicy(nullValue())
                                            .withDefaultTimeout()
                                            .withDynamic(anyOf(nullValue(), equalTo(false)))
                                            .withDynamicNodeProperties(nullValue())
                               .and().respond();
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue").openFuture().get();

            sender.close();
            session.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateSenderWithUserConfiguredSourceAndTargetOptions() throws Exception {
        final Map<String, Object> filtersToObject = new HashMap<>();
        filtersToObject.put("x-opt-filter", "a = b");

        final Map<String, String> filters = new HashMap<>();
        filters.put("x-opt-filter", "a = b");

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender()
                               .withSource().withAddress(notNullValue())
                                            .withDistributionMode("copy")
                                            .withTimeout(128)
                                            .withDurable(TerminusDurability.UNSETTLED_STATE)
                                            .withExpiryPolicy(TerminusExpiryPolicy.CONNECTION_CLOSE)
                                            .withDefaultOutcome(new Released())
                                            .withCapabilities("QUEUE")
                                            .withFilter(filtersToObject)
                                            .withOutcomes("amqp:accepted:list", "amqp:rejected:list")
                                            .also()
                               .withTarget().withAddress("test-queue")
                                            .withCapabilities("QUEUE")
                                            .withDurable(TerminusDurability.CONFIGURATION)
                                            .withExpiryPolicy(TerminusExpiryPolicy.SESSION_END)
                                            .withTimeout(42)
                                            .withDynamic(anyOf(nullValue(), equalTo(false)))
                                            .withDynamicNodeProperties(nullValue())
                               .and().respond();
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            SenderOptions senderOptions = new SenderOptions();

            senderOptions.sourceOptions().capabilities("QUEUE");
            senderOptions.sourceOptions().distributionMode(DistributionMode.COPY);
            senderOptions.sourceOptions().timeout(128);
            senderOptions.sourceOptions().durabilityMode(DurabilityMode.UNSETTLED_STATE);
            senderOptions.sourceOptions().expiryPolicy(ExpiryPolicy.CONNECTION_CLOSE);
            senderOptions.sourceOptions().defaultOutcome(DeliveryState.released());
            senderOptions.sourceOptions().filters(filters);
            senderOptions.sourceOptions().outcomes(DeliveryState.Type.ACCEPTED, DeliveryState.Type.REJECTED);

            senderOptions.targetOptions().capabilities("QUEUE");
            senderOptions.targetOptions().durabilityMode(DurabilityMode.CONFIGURATION);
            senderOptions.targetOptions().expiryPolicy(ExpiryPolicy.SESSION_CLOSE);
            senderOptions.targetOptions().timeout(42);

            Sender sender = session.openSender("test-queue", senderOptions).openFuture().get();

            sender.close();
            session.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testWaitForAcceptedReturnsOnRemoteAcceptance() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().accepted();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.openSender("test-queue").openFuture().get();
            Tracker tracker = sender.send(Message.create("Hello World"));
            tracker.awaitAccepted();

            assertTrue(tracker.remoteSettled());
            assertTrue(tracker.remoteState().isAccepted());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testWaitForAcceptanceFailsIfRemoteSendsRejceted() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true).withState().rejected();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.openSender("test-queue").openFuture().get();
            Tracker tracker = sender.send(Message.create("Hello World"));

            try {
                tracker.awaitAccepted(10, TimeUnit.SECONDS);
                fail("Should not succeed since remote sent something other than Accepted");
            } catch (ClientDeliveryStateException dlvEx) {
                // Expected
            }

            assertTrue(tracker.remoteSettled());
            assertFalse(tracker.remoteState().isAccepted());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testWaitForAcceptanceFailsIfRemoteSendsNoDisposition() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectTransfer().withNonNullPayload().withMore(false).respond().withSettled(true);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.openSender("test-queue").openFuture().get();
            Tracker tracker = sender.send(Message.create("Hello World"));

            try {
                tracker.awaitAccepted(10, TimeUnit.SECONDS);
                fail("Should not succeed since remote sent something other than Accepted");
            } catch (ClientDeliveryStateException dlvEx) {
                // Expected
            }

            assertTrue(tracker.remoteSettled());
            assertNull(tracker.remoteState());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            sender.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSenderLinkNameOptionAppliedWhenSet() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withName("custom-link-name").respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            SenderOptions senderOptions = new SenderOptions().linkName("custom-link-name");
            Sender sender = session.openSender("test-queue", senderOptions);

            sender.openFuture().get();
            sender.close();

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testInspectRemoteSourceMatchesValuesSent() throws Exception {
        Map<String, Object> remoteFilters = new HashMap<>();
        remoteFilters.put("filter-1", "value1");

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond().withSource().withOutcomes("Accepted", "Released")
                                                                 .withCapabilities("Queue")
                                                                 .withDistributionMode("COPY")
                                                                 .withDynamic(false)
                                                                 .withExpiryPolicy(TerminusExpiryPolicy.SESSION_END)
                                                                 .withDurability(TerminusDurability.UNSETTLED_STATE)
                                                                 .withDefaultOutcome(Released.getInstance())
                                                                 .withTimeout(Integer.MAX_VALUE)
                                                                 .withFilterMap(remoteFilters)
                                                                 .withAddress("test-queue");
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");

            Source remoteSource = sender.source();

            assertTrue(remoteSource.outcomes().contains(DeliveryState.Type.ACCEPTED));
            assertTrue(remoteSource.capabilities().contains("Queue"));
            assertEquals("test-queue", remoteSource.address());
            assertFalse(remoteSource.dynamic());
            assertNull(remoteSource.dynamicNodeProperties());
            assertEquals(DistributionMode.COPY, remoteSource.distributionMode());
            assertEquals(DeliveryState.released(), remoteSource.defaultOutcome());
            assertEquals(Integer.MAX_VALUE, remoteSource.timeout());
            assertEquals(DurabilityMode.UNSETTLED_STATE, remoteSource.durabilityMode());
            assertEquals(ExpiryPolicy.SESSION_CLOSE, remoteSource.expiryPolicy());
            assertEquals(remoteFilters, remoteSource.filters());

            sender.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testInspectRemoteTargetMatchesValuesSent() throws Exception {
        Map<String, Object> remoteFilters = new HashMap<>();
        remoteFilters.put("filter-1", "value1");

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond().withTarget().withCapabilities("Queue")
                                                                 .withDynamic(false)
                                                                 .withExpiryPolicy(TerminusExpiryPolicy.SESSION_END)
                                                                 .withDurability(TerminusDurability.UNSETTLED_STATE)
                                                                 .withTimeout(Integer.MAX_VALUE)
                                                                 .withAddress("test-queue");
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue");

            Target remoteTarget = sender.target();

            assertTrue(remoteTarget.capabilities().contains("Queue"));
            assertEquals("test-queue", remoteTarget.address());
            assertFalse(remoteTarget.dynamic());
            assertEquals(Integer.MAX_VALUE, remoteTarget.timeout());
            assertEquals(DurabilityMode.UNSETTLED_STATE, remoteTarget.durabilityMode());
            assertEquals(ExpiryPolicy.SESSION_CLOSE, remoteTarget.expiryPolicy());

            sender.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
