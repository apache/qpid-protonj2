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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.NextReceiverPolicy;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.test.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behaviors of the Session API
 */
@Timeout(20)
public class SessionTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTest.class);

    @Test
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesTimeout() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(true);
    }

    @Test
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesNoTimeout() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(false);
    }

    private void doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions();
            options.openTimeout(75);
            Session session = connection.openSession(options);

            try {
                if (timeout) {
                    session.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    session.openFuture().get();
                }

                fail("Session Open should timeout when no Begin response and complete future with error.");
            } catch (Throwable error) {
                LOG.info("Session open failed with error: ", error);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionOpenWaitWithTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionOpenWaitCanceledWhenConnectionDrops(true);
    }

    @Test
    public void testSessionOpenWaitWithNoTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionOpenWaitCanceledWhenConnectionDrops(false);
    }

    private void doTestSessionOpenWaitCanceledWhenConnectionDrops(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();

            try {
                if (timeout) {
                    session.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    session.openFuture().get();
                }

                fail("Session Open should wait should abort when connection drops.");
            } catch (ExecutionException error) {
                LOG.info("Session open failed with error: ", error);
                assertTrue(error.getCause() instanceof ClientIOException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionCloseTimeoutWhenNoRemoteEndArrivesTimeout() throws Exception {
        doTestSessionCloseTimeoutWhenNoRemoteEndArrives(true);
    }

    @Test
    public void testSessionCloseTimeoutWhenNoRemoteEndArrivesNoTimeout() throws Exception {
        doTestSessionCloseTimeoutWhenNoRemoteEndArrives(false);
    }

    private void doTestSessionCloseTimeoutWhenNoRemoteEndArrives(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions();
            options.closeTimeout(75);
            Session session = connection.openSession(options).openFuture().get();

            try {
                if (timeout) {
                    session.closeAsync().get(10, TimeUnit.SECONDS);
                } else {
                    session.closeAsync().get();
                }

                fail("Close should throw an error if the Session end doesn't arrive in time");
            } catch (Throwable error) {
                LOG.info("Session close failed with error: ", error);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionCloseWaitWithTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionCloseWaitCanceledWhenConnectionDrops(true);
    }

    @Test
    public void testSessionCloseWaitWithNoTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionCloseWaitCanceledWhenConnectionDrops(false);
    }

    private void doTestSessionCloseWaitCanceledWhenConnectionDrops(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions();
            options.closeTimeout(75);
            Session session = connection.openSession(options).openFuture().get();

            try {
                if (timeout) {
                    session.closeAsync().get(10, TimeUnit.SECONDS);
                } else {
                    session.closeAsync().get();
                }
            } catch (ExecutionException error) {
                fail("Session Close should complete when parent connection drops.");
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionCloseGetsResponseWithErrorDoesNotThrowTimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(true);
    }

    @Test
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowUntimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(false);
    }

    protected void doTestSessionCloseGetsResponseWithErrorThrows(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().respond().withErrorCondition(AmqpError.INTERNAL_ERROR.toString(), "Something odd happened.");
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            if (timeout) {
                // Should close normally and not throw error as we initiated the close.
                session.closeAsync().get(10, TimeUnit.SECONDS);
            } else {
                // Should close normally and not throw error as we initiated the close.
                session.closeAsync().get();
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionGetRemotePropertiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteProperties(true);
    }

    @Test
    public void testSessionGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadSessionRemoteProperties(false);
    }

    private void tryReadSessionRemoteProperties(boolean beginResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions().openTimeout(250, TimeUnit.MILLISECONDS);
            Session session = connection.openSession(options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withProperties(expectedProperties).later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull(session.properties(), "Remote should have responded with a remote properties value");
                assertEquals(expectedProperties, session.properties());
            } else {
                try {
                    session.properties();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                session.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and end was sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionGetRemoteOfferedCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteOfferedCapabilities(true);
    }

    @Test
    public void testSessionGetRemoteOfferedCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadSessionRemoteOfferedCapabilities(false);
    }

    private void tryReadSessionRemoteOfferedCapabilities(boolean beginResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get();
            SessionOptions options = new SessionOptions().openTimeout(250);
            Session session = connection.openSession(options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withOfferedCapabilities("transactions").later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull(session.offeredCapabilities(), "Remote should have responded with a remote offered Capabilities value");
                assertEquals(1, session.offeredCapabilities().length);
                assertEquals("transactions", session.offeredCapabilities()[0]);
            } else {
                try {
                    session.offeredCapabilities();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                session.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and end was sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionGetRemoteDesiredCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteDesiredCapabilities(true);
    }

    @Test
    public void testSessionGetRemoteDesiredCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadSessionRemoteDesiredCapabilities(false);
    }

    private void tryReadSessionRemoteDesiredCapabilities(boolean beginResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions().openTimeout(250);
            Session session = connection.openSession(options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull(session.desiredCapabilities(), "Remote should have responded with a remote desired Capabilities value");
                assertEquals(1, session.desiredCapabilities().length);
                assertEquals("Error-Free", session.desiredCapabilities()[0]);
            } else {
                try {
                    session.desiredCapabilities();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                session.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and end sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testQuickOpenCloseWhenNoBeginResponseFailsFastOnOpenTimeout() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            ConnectionOptions options = new ConnectionOptions();
            options.closeTimeout(TimeUnit.HOURS.toMillis(1));  // Test would timeout if waited on.

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            SessionOptions sessionOptions = new SessionOptions().openTimeout(250, TimeUnit.MILLISECONDS);

            try {
                connection.openSession(sessionOptions).closeAsync().get();
            } catch (ExecutionException error) {
                fail("Should not fail when waiting on close with quick open timeout");
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseWithErrorConditionSync() throws Exception {
        doTestCloseWithErrorCondition(true);
    }

    @Test
    public void testCloseWithErrorConditionAsync() throws Exception {
        doTestCloseWithErrorCondition(false);
    }

    private void doTestCloseWithErrorCondition(boolean sync) throws Exception {
        final String condition = "amqp:precondition-failed";
        final String description = "something bad happened.";

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().withError(condition, description).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();

            session.openFuture().get();

            assertEquals(session.client(), container);

            if (sync) {
                session.close(ErrorCondition.create(condition, description, null));
            } else {
                session.closeAsync(ErrorCondition.create(condition, description, null));
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testCannotCreateResourcesFromClosedSession() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();

            session.openFuture().get();
            session.close();

            assertThrows(ClientIllegalStateException.class, () -> session.openReceiver("test"));
            assertThrows(ClientIllegalStateException.class, () -> session.openReceiver("test", new ReceiverOptions()));
            assertThrows(ClientIllegalStateException.class, () -> session.openDurableReceiver("test", "test"));
            assertThrows(ClientIllegalStateException.class, () -> session.openDurableReceiver("test", "test", new ReceiverOptions()));
            assertThrows(ClientIllegalStateException.class, () -> session.openDynamicReceiver());
            assertThrows(ClientIllegalStateException.class, () -> session.openDynamicReceiver(new HashMap<>()));
            assertThrows(ClientIllegalStateException.class, () -> session.openDynamicReceiver(new ReceiverOptions()));
            assertThrows(ClientIllegalStateException.class, () -> session.openDynamicReceiver(new HashMap<>(), new ReceiverOptions()));
            assertThrows(ClientIllegalStateException.class, () -> session.openSender("test"));
            assertThrows(ClientIllegalStateException.class, () -> session.openSender("test", new SenderOptions()));
            assertThrows(ClientIllegalStateException.class, () -> session.openAnonymousSender());
            assertThrows(ClientIllegalStateException.class, () -> session.openAnonymousSender(new SenderOptions()));

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNextReceiverFromDefaultSessionReturnsSameReceiverForQueuedDeliveries() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            for (int i = 0; i < 10; ++i) {
                peer.remoteTransfer().withDeliveryId(i)
                                     .withMore(false)
                                     .withMessageFormat(0)
                                     .withPayload(payload).queue();
            }
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connOptions = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.FIRST_AVAILABLE);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connOptions);

            ReceiverOptions options = new ReceiverOptions().creditWindow(0).autoAccept(false);
            Receiver receiver = connection.openReceiver("test-receiver", options);
            receiver.addCredit(10);

            Wait.waitFor(() -> receiver.queuedDeliveries() == 10);

            peer.waitForScriptToComplete();

            for (int i = 0; i < 10; ++i) {
                 Receiver nextReceiver = connection.nextReceiver();
                 assertSame(receiver, nextReceiver);
                 Delivery delivery = nextReceiver.receive();
                 assertNotNull(delivery);
            }

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverTimesOut() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openReceiver("test-receiver").openFuture().get();

            peer.waitForScriptToComplete();

            assertNull(connection.nextReceiver(10, TimeUnit.MILLISECONDS));

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverReturnsAllReceiversEventually() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            ReceiverOptions options = new ReceiverOptions().creditWindow(10).autoAccept(false);
            connection.openReceiver("test-receiver1", options).openFuture().get();
            connection.openReceiver("test-receiver2", options).openFuture().get();
            connection.openReceiver("test-receiver3", options).openFuture().get();

            peer.waitForScriptToComplete();

            Receiver receiver1 = connection.nextReceiver(NextReceiverPolicy.FIRST_AVAILABLE);
            assertNotNull(receiver1.receive());
            Receiver receiver2 = connection.nextReceiver(NextReceiverPolicy.FIRST_AVAILABLE);
            assertNotNull(receiver2.receive());
            Receiver receiver3 = connection.nextReceiver(NextReceiverPolicy.FIRST_AVAILABLE);
            assertNotNull(receiver3.receive());

            assertNotSame(receiver1, receiver2);
            assertNotSame(receiver1, receiver3);
            assertNotSame(receiver2, receiver3);

            peer.waitForScriptToComplete();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testConnectionOptionsConfiguresLargestBacklogNextReceiverPolicy() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(3)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.LARGEST_BACKLOG);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = connection.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = connection.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = connection.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 1);
            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            Receiver next = connection.nextReceiver();
            assertSame(next, receiver2);

            peer.waitForScriptToComplete();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testSessionOptionsConfiguresLargestBacklogNextReceiverPolicy() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(3)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.LARGEST_BACKLOG);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 1);
            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            Receiver next = session.nextReceiver();
            assertSame(next, receiver2);

            peer.waitForScriptToComplete();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testUserSpeicifedNextReceiverPolicyOverridesConfiguration() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(3)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.SMALLEST_BACKLOG);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 1);
            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            Receiver next = session.nextReceiver(NextReceiverPolicy.LARGEST_BACKLOG);
            assertSame(next, receiver2);

            peer.waitForScriptToComplete();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testSessionOptionsConfiguresSmallestBacklogNextReceiverPolicy() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(3)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(4)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(5)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.SMALLEST_BACKLOG);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 3);
            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            Receiver next = session.nextReceiver();
            assertSame(next, receiver3);

            peer.waitForScriptToComplete();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverCompletesAfterDeliveryArrivesRoundRobin() throws Exception {
        doTestNextReceiverCompletesAfterDeliveryArrives(NextReceiverPolicy.ROUND_ROBIN);
    }

    @Test
    public void testNextReceiverCompletesAfterDeliveryArrivesRandom() throws Exception {
        doTestNextReceiverCompletesAfterDeliveryArrives(NextReceiverPolicy.RANDOM);
    }

    @Test
    public void testNextReceiverCompletesAfterDeliveryArrivesLargestBacklog() throws Exception {
        doTestNextReceiverCompletesAfterDeliveryArrives(NextReceiverPolicy.LARGEST_BACKLOG);
    }

    @Test
    public void testNextReceiverCompletesAfterDeliveryArrivesSmallestBacklog() throws Exception {
        doTestNextReceiverCompletesAfterDeliveryArrives(NextReceiverPolicy.SMALLEST_BACKLOG);
    }

    @Test
    public void testNextReceiverCompletesAfterDeliveryArrivesFirstAvailable() throws Exception {
        doTestNextReceiverCompletesAfterDeliveryArrives(NextReceiverPolicy.FIRST_AVAILABLE);
    }

    public void doTestNextReceiverCompletesAfterDeliveryArrives(NextReceiverPolicy policy) throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final CountDownLatch done = new CountDownLatch(1);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(policy);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            connection.openReceiver("test-receiver1", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    Receiver receiver = connection.nextReceiver();
                    Delivery delivery = receiver.receive();
                    LOG.info("Next receiver returned delivery with body: {}", delivery.message().body());
                    done.countDown();
                } catch (ClientException e) {
                    e.printStackTrace();
                }
            });

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).later(15);

            peer.waitForScriptToComplete();

            assertTrue(done.await(10, TimeUnit.SECONDS));

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverThrowsAfterSessionClosedRoundRobin() throws Exception {
        doTestNextReceiverThrowsAfterSessionClosed(NextReceiverPolicy.ROUND_ROBIN);
    }

    @Test
    public void testNextReceiverThrowsAfterSessionClosedRandom() throws Exception {
        doTestNextReceiverThrowsAfterSessionClosed(NextReceiverPolicy.RANDOM);
    }

    @Test
    public void testNextReceiverThrowsAfterSessionClosedLargestBacklog() throws Exception {
        doTestNextReceiverThrowsAfterSessionClosed(NextReceiverPolicy.LARGEST_BACKLOG);
    }

    @Test
    public void testNextReceiverThrowsAfterSessionClosedSmallestBacklog() throws Exception {
        doTestNextReceiverThrowsAfterSessionClosed(NextReceiverPolicy.SMALLEST_BACKLOG);
    }

    @Test
    public void testNextReceiverThrowsAfterSessionClosedFirstAvailable() throws Exception {
        doTestNextReceiverThrowsAfterSessionClosed(NextReceiverPolicy.FIRST_AVAILABLE);
    }

    public void doTestNextReceiverThrowsAfterSessionClosed(NextReceiverPolicy policy) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);
            final AtomicReference<Exception> error = new AtomicReference<>();

            final Client container = Client.create();
            final ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(policy);
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            final Session session = connection.openSession().openFuture().get();

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    started.countDown();
                    session.nextReceiver();
                } catch (ClientException e) {
                    error.set(e);
                } finally {
                    done.countDown();
                }
            });

            peer.waitForScriptToComplete();

            assertTrue(started.await(10, TimeUnit.SECONDS));

            peer.expectEnd().respond();

            session.closeAsync();

            assertTrue(done.await(10, TimeUnit.SECONDS));
            assertTrue(error.get() instanceof ClientIllegalStateException);

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverCompletesWhenCalledBeforeReceiverCreateRoundRobin() throws Exception {
        doTestNextReceiverCompletesWhenCalledBeforeReceiverCreate(NextReceiverPolicy.ROUND_ROBIN);
    }

    @Test
    public void testNextReceiverCompletesWhenCalledBeforeReceiverCreateRandom() throws Exception {
        doTestNextReceiverCompletesWhenCalledBeforeReceiverCreate(NextReceiverPolicy.RANDOM);
    }

    @Test
    public void testNextReceiverCompletesWhenCalledBeforeReceiverCreateLargestBacklog() throws Exception {
        doTestNextReceiverCompletesWhenCalledBeforeReceiverCreate(NextReceiverPolicy.LARGEST_BACKLOG);
    }

    @Test
    public void testNextReceiverCompletesWhenCalledBeforeReceiverCreateSmallestBacklog() throws Exception {
        doTestNextReceiverCompletesWhenCalledBeforeReceiverCreate(NextReceiverPolicy.SMALLEST_BACKLOG);
    }

    @Test
    public void testNextReceiverCompletesWhenCalledBeforeReceiverCreateFirstAvailable() throws Exception {
        doTestNextReceiverCompletesWhenCalledBeforeReceiverCreate(NextReceiverPolicy.FIRST_AVAILABLE);
    }

    public void doTestNextReceiverCompletesWhenCalledBeforeReceiverCreate(NextReceiverPolicy policy) throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(policy);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    started.countDown();
                    Receiver receiver = connection.nextReceiver();
                    Delivery delivery = receiver.receive();
                    LOG.info("Next receiver returned delivery with body: {}", delivery.message().body());
                    done.countDown();
                } catch (ClientException e) {
                    e.printStackTrace();
                }
            });

            assertTrue(started.await(10, TimeUnit.SECONDS));

            connection.openFuture().get();

            peer.waitForScriptToComplete();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue().afterDelay(10);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            connection.openReceiver("test-receiver1", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            assertTrue(done.await(10, TimeUnit.SECONDS));

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverRoundRobinReturnsNextReceiverAfterLast() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.ROUND_ROBIN);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            assertEquals(0, receiver1.queuedDeliveries());

            Receiver next = session.nextReceiver();
            assertSame(next, receiver2);
            next = session.nextReceiver();
            assertSame(next, receiver3);

            peer.waitForScriptToComplete();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverRoundRobinPolicyWrapsAround() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.ROUND_ROBIN);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            assertEquals(0, receiver1.queuedDeliveries());

            Receiver next = session.nextReceiver();
            assertSame(next, receiver2);
            next = session.nextReceiver();
            assertSame(next, receiver3);

            peer.waitForScriptToComplete();

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(3)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 1);

            next = session.nextReceiver();
            assertSame(next, receiver1);

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverRoundRobinPolicyRestartsWhenLastReceiverClosed() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(1)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(2)
                                 .withDeliveryId(2)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.ROUND_ROBIN);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();
            peer.expectDetach().respond();

            Wait.waitFor(() -> receiver2.queuedDeliveries() == 2);
            Wait.waitFor(() -> receiver3.queuedDeliveries() == 1);

            assertEquals(0, receiver1.queuedDeliveries());

            Receiver next = session.nextReceiver();
            assertSame(next, receiver2);
            next.close();

            peer.waitForScriptToComplete();

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(3)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 1);

            next = session.nextReceiver();
            assertSame(next, receiver1);

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testNextReceiverRoundRobinPolicySkipsEmptyReceivers() throws Exception {
        byte[] payload = createEncodedMessage(new AmqpValue<String>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteTransfer().withHandle(3)
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.RANDOM);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOptions = new SessionOptions().defaultNextReceiverPolicy(NextReceiverPolicy.ROUND_ROBIN);
            Session session = connection.openSession(sessionOptions);

            ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10).autoAccept(false);
            Receiver receiver1 = session.openReceiver("test-receiver1", receiverOptions).openFuture().get();
            Receiver receiver2 = session.openReceiver("test-receiver2", receiverOptions).openFuture().get();
            Receiver receiver3 = session.openReceiver("test-receiver3", receiverOptions).openFuture().get();
            Receiver receiver4 = session.openReceiver("test-receiver4", receiverOptions).openFuture().get();

            peer.waitForScriptToComplete();

            Wait.waitFor(() -> receiver1.queuedDeliveries() == 1);
            Wait.waitFor(() -> receiver4.queuedDeliveries() == 1);

            assertEquals(0, receiver2.queuedDeliveries());
            assertEquals(0, receiver3.queuedDeliveries());

            Receiver next = session.nextReceiver();
            assertSame(next, receiver1);
            next = session.nextReceiver();
            assertSame(next, receiver4);

            peer.waitForScriptToComplete();
            peer.expectClose().respond();

            connection.close();

            peer.waitForScriptToComplete();
        }
    }
}
