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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.AcceptedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.ModifiedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.RejectedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.ReleasedMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.TransactionalStateMatcher;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test the {@link ProtonSender}
 */
@Timeout(20)
public class ProtonSenderTest extends ProtonEngineTestSupport {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonSenderTest.class);

    @Test
    public void testLocalLinkStateCannotBeChangedAfterOpen() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");

        sender.setProperties(new HashMap<>());

        sender.open();

        try {
            sender.setProperties(new HashMap<>());
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            sender.setDesiredCapabilities(new Symbol[] { AmqpError.DECODE_ERROR });
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            sender.setOfferedCapabilities(new Symbol[] { AmqpError.DECODE_ERROR });
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            sender.setSenderSettleMode(SenderSettleMode.MIXED);
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            sender.setSource(new Source());
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            sender.setTarget(new Target());
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            sender.setMaxMessageSize(UnsignedLong.ZERO);
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        sender.detach();
        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderEmitsOpenAndCloseEvents() throws Exception {
        doTestSenderEmitsEvents(false);
    }

    @Test
    public void testSenderEmitsOpenAndDetachEvents() throws Exception {
        doTestSenderEmitsEvents(true);
    }

    private void doTestSenderEmitsEvents(boolean detach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean senderLocalOpen = new AtomicBoolean();
        final AtomicBoolean senderLocalClose = new AtomicBoolean();
        final AtomicBoolean senderLocalDetach = new AtomicBoolean();
        final AtomicBoolean senderRemoteOpen = new AtomicBoolean();
        final AtomicBoolean senderRemoteClose = new AtomicBoolean();
        final AtomicBoolean senderRemoteDetach = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.localOpenHandler(result -> senderLocalOpen.set(true))
              .localCloseHandler(result -> senderLocalClose.set(true))
              .localDetachHandler(result -> senderLocalDetach.set(true))
              .openHandler(result -> senderRemoteOpen.set(true))
              .detachHandler(result -> senderRemoteDetach.set(true))
              .closeHandler(result -> senderRemoteClose.set(true));

        sender.open();

        assertNull(sender.getDeliveryTagGenerator());

        if (detach) {
            sender.detach();
        } else {
            sender.close();
        }

        assertTrue(senderLocalOpen.get(), "Sender should have reported local open");
        assertTrue(senderRemoteOpen.get(), "Sender should have reported remote open");

        if (detach) {
            assertFalse(senderLocalClose.get(), "Sender should not have reported local close");
            assertTrue(senderLocalDetach.get(), "Sender should have reported local detach");
            assertFalse(senderRemoteClose.get(), "Sender should not have reported remote close");
            assertTrue(senderRemoteDetach.get(), "Sender should have reported remote close");
        } else {
            assertTrue(senderLocalClose.get(), "Sender should have reported local close");
            assertFalse(senderLocalDetach.get(), "Sender should not have reported local detach");
            assertTrue(senderRemoteClose.get(), "Sender should have reported remote close");
            assertFalse(senderRemoteDetach.get(), "Sender should not have reported remote close");
        }

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderRoutesDetachEventToCloseHandlerIfNonSset() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean senderLocalOpen = new AtomicBoolean();
        final AtomicBoolean senderLocalClose = new AtomicBoolean();
        final AtomicBoolean senderRemoteOpen = new AtomicBoolean();
        final AtomicBoolean senderRemoteClose = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.localOpenHandler(result -> senderLocalOpen.set(true))
              .localCloseHandler(result -> senderLocalClose.set(true))
              .openHandler(result -> senderRemoteOpen.set(true))
              .closeHandler(result -> senderRemoteClose.set(true));

        sender.open();
        sender.detach();

        assertTrue(senderLocalOpen.get(), "Sender should have reported local open");
        assertTrue(senderRemoteOpen.get(), "Sender should have reported remote open");
        assertTrue(senderLocalClose.get(), "Sender should have reported local detach");
        assertTrue(senderRemoteClose.get(), "Sender should have reported remote detach");

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderEnforcesOneActiveDeliveryAtNextAPI() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test").open();

        assertNotNull(sender.next());

        assertThrows(IllegalStateException.class, () -> sender.next());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderReceivesParentSessionClosedEvent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean parentClosed = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.parentEndpointClosedHandler(result -> parentClosed.set(true));

        sender.open();

        session.close();

        assertTrue(parentClosed.get(), "Sender should have reported parent session closed");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderReceivesParentConnectionClosedEvent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean parentClosed = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.parentEndpointClosedHandler(result -> parentClosed.set(true));

        sender.open();

        connection.close();

        assertTrue(parentClosed.get(), "Sender should have reported parent connection closed");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineShutdownEventNeitherEndClosed() throws Exception {
        doTestEngineShutdownEvent(false, false);
    }

    @Test
    public void testEngineShutdownEventLocallyClosed() throws Exception {
        doTestEngineShutdownEvent(true, false);
    }

    @Test
    public void testEngineShutdownEventRemotelyClosed() throws Exception {
        doTestEngineShutdownEvent(false, true);
    }

    @Test
    public void testEngineShutdownEventBothEndsClosed() throws Exception {
        doTestEngineShutdownEvent(true, true);
    }

    private void doTestEngineShutdownEvent(boolean locallyClosed, boolean remotelyClosed) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean engineShutdown = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        connection.open();

        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();
        sender.engineShutdownHandler(result -> engineShutdown.set(true));

        if (locallyClosed) {
            if (remotelyClosed) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            sender.close();
        }

        if (remotelyClosed && !locallyClosed) {
            peer.remoteDetach();
        }

        engine.shutdown();

        if (locallyClosed && remotelyClosed) {
            assertFalse(engineShutdown.get(), "Should not have reported engine shutdown");
        } else {
            assertTrue(engineShutdown.get(), "Should have reported engine shutdown");
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderOpenWithNoSenderOrReceiverSettleModes() throws Exception {
        doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(null, null);
    }

    @Test
    public void testSenderOpenWithSettledAndFirst() throws Exception {
        doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.SETTLED, ReceiverSettleMode.FIRST);
    }

    @Test
    public void testSenderOpenWithUnsettledAndSecond() throws Exception {
        doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.UNSETTLED, ReceiverSettleMode.SECOND);
    }

    private void doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode senderMode, ReceiverSettleMode receiverMode) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withSndSettleMode(senderMode == null ? null : senderMode.byteValue())
                           .withRcvSettleMode(receiverMode == null ? null : receiverMode.byteValue())
                           .respond()
                           .withSndSettleMode(senderMode == null ? null : senderMode.byteValue())
                           .withRcvSettleMode(receiverMode == null ? null : receiverMode.byteValue());

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender");
        sender.setSenderSettleMode(senderMode);
        sender.setReceiverSettleMode(receiverMode);
        sender.open();

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();

        if (senderMode != null) {
            assertEquals(senderMode, sender.getSenderSettleMode());
        } else {
            assertEquals(SenderSettleMode.MIXED, sender.getSenderSettleMode());
        }
        if (receiverMode != null) {
            assertEquals(receiverMode, sender.getReceiverSettleMode());
        } else {
            assertEquals(ReceiverSettleMode.FIRST, sender.getReceiverSettleMode());
        }

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderOpenAndCloseAreIdempotent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();

        // Should not emit another attach frame
        sender.open();

        sender.close();

        // Should not emit another detach frame
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCreateSenderAndClose() throws Exception {
        doTestCreateSenderAndCloseOrDetachLink(true);
    }

    @Test
    public void testCreateSenderAndDetach() throws Exception {
        doTestCreateSenderAndCloseOrDetachLink(false);
    }

    private void doTestCreateSenderAndCloseOrDetachLink(boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.expectDetach().withClosed(close).respond();
        peer.expectClose().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();

        assertTrue(sender.isSender());
        assertFalse(sender.isReceiver());

        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAttachAfterLocalSenderOpened() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenBeginAttachBeforeRemoteResponds() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();
        peer.expectAttach();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.open();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrivesWithNullTarget() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond().withNullTarget();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.setSource(new Source());
        sender.setTarget(new Target());
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.open();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");
        assertNull(sender.getRemoteTarget());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleSenders() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).respond();
        peer.expectAttach().withHandle(1).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender1 = session.sender("sender-1");
        sender1.open();
        Sender sender2 = session.sender("sender-2");
        sender2.open();

        // Close in reverse order
        sender2.close();
        sender1.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireClosedEventAfterRemoteDetachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.closeHandler(result -> {
            senderRemotelyClosed.set(true);
        });
        sender.open();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");

        sender.close();

        assertTrue(senderRemotelyClosed.get(), "Sender remote closed event did not fire");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireClosedEventAfterRemoteDetachArrivesBeforeLocalClose() throws Exception {
        doTestSenderFireEventAfterRemoteDetachArrivesBeforeLocalClose(true);
    }

    @Test
    public void testSenderFireDetachEventAfterRemoteDetachArrivesBeforeLocalClose() throws Exception {
        doTestSenderFireEventAfterRemoteDetachArrivesBeforeLocalClose(false);
    }

    private void doTestSenderFireEventAfterRemoteDetachArrivesBeforeLocalClose(boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteDetach().withClosed(close).queue();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();
        final AtomicBoolean senderRemotelyDetached = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.openHandler(result -> senderRemotelyOpened.set(true));
        sender.closeHandler(result -> senderRemotelyClosed.set(true));
        sender.detachHandler(result -> senderRemotelyDetached.set(true));
        sender.open();

        peer.waitForScriptToComplete();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");
        if (close) {
            assertTrue(senderRemotelyClosed.get(), "Sender remote closed event did not fire");
            assertFalse(senderRemotelyDetached.get(), "Sender remote detached event fired");
        } else {
            assertFalse(senderRemotelyClosed.get(), "Sender remote closed event fired");
            assertTrue(senderRemotelyDetached.get(), "Sender remote closed event did not fire");
        }

        peer.expectDetach().withClosed(close);
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testRemotelyCloseSenderAndOpenNewSenderImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, false);
    }

    @Test
    public void testRemotelyDetachSenderAndOpenNewSenderImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(false, false);
    }

    @Test
    public void testRemotelyCloseSenderAndOpenNewSenderImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, true);
    }

    @Test
    public void testRemotelyDetachSenderAndOpenNewSenderImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(false, true);
    }

    private void doTestRemotelyTerminateLinkAndThenCreateNewLink(boolean close, boolean sameLinkName) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        String firstLinkName = "test-link-1";
        String secondLinkName = sameLinkName ? firstLinkName : "test-link-2";

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withRole(Role.SENDER.getValue()).respond();
        peer.remoteDetach().withClosed(close).queue();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();
        final AtomicBoolean senderRemotelyDetached = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender(firstLinkName);
        sender.openHandler(result -> senderRemotelyOpened.set(true));
        sender.closeHandler(result -> senderRemotelyClosed.set(true));
        sender.detachHandler(result -> senderRemotelyDetached.set(true));
        sender.open();

        peer.waitForScriptToComplete();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");
        if (close) {
            assertTrue(senderRemotelyClosed.get(), "Sender remote closed event did not fire");
            assertFalse(senderRemotelyDetached.get(), "Sender remote detached event fired");
        } else {
            assertFalse(senderRemotelyClosed.get(), "Sender remote closed event fired");
            assertTrue(senderRemotelyDetached.get(), "Sender remote closed event did not fire");
        }

        peer.expectDetach().withClosed(close);
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();
        peer.expectAttach().withHandle(0).withRole(Role.SENDER.getValue()).respond();
        peer.expectDetach().withClosed(close).respond();

        // Reset trackers
        senderRemotelyOpened.set(false);
        senderRemotelyClosed.set(false);
        senderRemotelyDetached.set(false);

        sender = session.sender(secondLinkName);
        sender.openHandler(result -> senderRemotelyOpened.set(true));
        sender.closeHandler(result -> senderRemotelyClosed.set(true));
        sender.detachHandler(result -> senderRemotelyDetached.set(true));
        sender.open();

        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");
        if (close) {
            assertTrue(senderRemotelyClosed.get(), "Sender remote closed event did not fire");
            assertFalse(senderRemotelyDetached.get(), "Sender remote detached event fired");
        } else {
            assertFalse(senderRemotelyClosed.get(), "Sender remote closed event fired");
            assertTrue(senderRemotelyDetached.get(), "Sender remote closed event did not fire");
        }

        assertNull(failure);
    }

    @Test
    public void testConnectionSignalsRemoteSenderOpen() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteAttach().withName("receiver")
                           .withHandle(0)
                           .withRole(Role.RECEIVER.getValue())
                           .withInitialDeliveryCount(0)
                           .onChannel(0).queue();
        peer.expectAttach();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Sender> sender = new AtomicReference<>();

        Connection connection = engine.start();

        connection.senderOpenHandler(result -> {
            senderRemotelyOpened.set(true);
            sender.set(result);
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");

        sender.get().open();
        sender.get().close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenSenderAfterSessionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");

        session.close();

        try {
            sender.open();
            fail("Should not be able to open a link from a closed session.");
        } catch (IllegalStateException ise) {}

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenSenderAfterSessionRemotelyClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteEnd().queue();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        Sender sender = session.sender("test");
        session.open();

        try {
            sender.open();
            fail("Should not be able to open a link from a remotely closed session.");
        } catch (IllegalStateException ise) {}

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testGetCurrentDeliveryFromSender() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).respond();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        sender.open();

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        assertFalse(delivery.isAborted());
        assertTrue(delivery.isPartial());
        assertFalse(delivery.isSettled());
        assertFalse(delivery.isRemotelySettled());

        // Always return same delivery until completed.
        assertSame(delivery, sender.current());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderGetsCreditOnIncomingFlow() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.open();

        assertEquals(10, sender.getCredit());
        assertTrue(sender.isSendable());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSendSmallPayloadWhenCreditAvailable() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payloadBuffer);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                handler.next().setTag(new byte[] {0}).writeBytes(payload);
            }
        });

        sender.open();
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSendTransferWithNonDefaultMessageFormat() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withMessageFormat(17).withPayload(payloadBuffer);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                handler.next().setTag(new byte[] {0}).setMessageFormat(17).writeBytes(payload);
            }
        });

        sender.open();
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderSignalsDeliveryUpdatedOnSettledThenSettleFromLinkAPI() throws Exception {
        doTestSenderSignalsDeliveryUpdatedOnSettled(true);
    }

    @Test
    public void testSenderSignalsDeliveryUpdatedOnSettledThenSettleDelivery() throws Exception {
        doTestSenderSignalsDeliveryUpdatedOnSettled(false);
    }

    private void doTestSenderSignalsDeliveryUpdatedOnSettled(boolean settleFromLink) {

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted()
                                .withFirst(0).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliveryUpdatedAndSettled = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> updatedDelivery = new AtomicReference<>();
        sender.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        assertFalse(sender.isSendable());

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                handler.next().setTag(new byte[] {0}).writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
            }
        });

        sender.open();

        assertTrue(deliveryUpdatedAndSettled.get(), "Delivery should have been updated and state settled");
        assertEquals(Accepted.getInstance(), updatedDelivery.get().getRemoteState());
        assertTrue(sender.hasUnsettled());
        assertFalse(sender.unsettled().isEmpty());

        if (settleFromLink) {
            sender.settle(delivery -> true);
        } else {
            updatedDelivery.get().settle();
        }

        assertFalse(sender.hasUnsettled());
        assertTrue(sender.unsettled().isEmpty());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderBeforeOpenConnection() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Create the connection but don't open, then open a session and a sender and
        // the session begin and sender attach shouldn't go out until the connection
        // is opened locally.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER.getValue()).respond();

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderBeforeOpenSession() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its attach until the session
        // is opened.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        Sender sender = session.sender("sender");
        sender.open();

        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER.getValue()).respond();

        // Now open the session, expect the Begin, and Attach frames
        session.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderDetachAfterEndSent() {
        doTestSenderClosedOrDetachedAfterEndSent(false);
    }

    @Test
    public void testSenderCloseAfterEndSent() {
        doTestSenderClosedOrDetachedAfterEndSent(true);
    }

    public void doTestSenderClosedOrDetachedAfterEndSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER.getValue()).respond();
        peer.expectEnd().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its detach if the session has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        // Causes the End frame to be sent
        session.close();

        // The sender should not emit an end as the session was closed which implicitly
        // detached the link.
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderDetachAfterCloseSent() {
        doTestSenderClosedOrDetachedAfterCloseSent(false);
    }

    @Test
    public void testSenderCloseAfterCloseSent() {
        doTestSenderClosedOrDetachedAfterCloseSent(true);
    }

    public void doTestSenderClosedOrDetachedAfterCloseSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER.getValue()).respond();
        peer.expectClose().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its detach if the connection has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        // Cause an Close frame to be sent
        connection.close();

        // The sender should not emit an detach as the connection was closed which implicitly
        // detached the link.
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testNoDispositionSentAfterDeliverySettledForSender() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0});
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSendable = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> sent = new AtomicReference<>();

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                sent.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
                deliverySentAfterSendable.set(true);
            }
        });

        sender.open();

        assertTrue(deliverySentAfterSendable.get(), "Delivery should have been sent after credit arrived");

        assertNull(sender.current());

        sent.get().disposition(Accepted.getInstance(), true);

        OutgoingDelivery delivery2 = sender.next();
        assertNotSame(delivery2, sent.get());
        delivery2.disposition(Released.getInstance(), true);

        assertFalse(sender.hasUnsettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderCannotSendAfterConnectionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        sender.open();

        assertEquals(10, sender.getCredit());
        assertTrue(sender.isSendable());

        connection.close();

        assertFalse(sender.isSendable());
        try {
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1 }));
            fail("Should not be able to write to delivery after connection closed.");
        } catch (IllegalStateException ise) {
            // Should not allow writes on past delivery instances after connection closed
        }

        try {
            sender.next();
            fail("Should not be able get next after connection closed");
        } catch (IllegalStateException ise) {
            // Should not allow next message after close of connection
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderCannotSendAfterSessionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        sender.open();

        assertEquals(10, sender.getCredit());
        assertTrue(sender.isSendable());

        session.close();

        assertFalse(sender.isSendable());
        try {
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1 }));
            fail("Should not be able to write to delivery after session closed.");
        } catch (IllegalStateException ise) {
            // Should not allow writes on past delivery instances after session closed
        }

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderWriteBytesThrowsEngineFailedAfterConnectionDropped() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.dropAfterLastHandler();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();
        OutgoingDelivery delivery = sender.next();

        assertNotNull(delivery);
        assertTrue(sender.isSendable());

        try {
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1 }));
            fail("Should not be able to write to delivery afters simulated connection drop.");
        } catch (EngineFailedException efe) {
            // Should not allow writes on past delivery instances after connection dropped
            assertTrue(efe.getCause() instanceof UncheckedIOException);
            LOG.debug("Caught expected IO exception from write to broken connection", efe);
        }

        peer.waitForScriptToComplete();

        assertNotNull(failure);
    }

    @Test
    public void testSendMultiFrameDeliveryAndSingleFrameDeliveryOnSingleSessionFromDifferentSenders() {
        doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(false);
    }

    @Test
    public void testMultipleMultiFrameDeliveriesOnSingleSessionFromDifferentSenders() {
        doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(true);
    }

    private void doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(boolean bothDeliveriesMultiFrame) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        int contentLength1 = 6000;
        int frameSizeLimit = 4000;
        int contentLength2 = 2000;
        if (bothDeliveriesMultiFrame) {
            contentLength2 = 6000;
        }

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(frameSizeLimit).respond().withContainerId("driver").withMaxFrameSize(frameSizeLimit);
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();

        Connection connection = engine.start();
        connection.setMaxFrameSize(frameSizeLimit);
        connection.open();
        Session session = connection.session();
        session.open();

        String linkName1 = "Sender1";
        Sender sender1 = session.sender(linkName1);
        sender1.open();

        String linkName2 = "Sender2";
        Sender sender2 = session.sender(linkName2);
        sender2.open();

        final AtomicBoolean sender1MarkedSendable = new AtomicBoolean();
        sender1.creditStateUpdateHandler(handler -> {
            sender1MarkedSendable.set(handler.isSendable());
        });

        final AtomicBoolean sender2MarkedSendable = new AtomicBoolean();
        sender2.creditStateUpdateHandler(handler -> {
            sender2MarkedSendable.set(handler.isSendable());
        });

        peer.remoteFlow().withHandle(0)
                         .withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).now();
        peer.remoteFlow().withHandle(1)
                         .withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).now();

        assertTrue(sender1MarkedSendable.get(), "Sender 1 should now be sendable");
        assertTrue(sender2MarkedSendable.get(), "Sender 2 should now be sendable");

        // Frames are not multiplexed for large deliveries as we write the full
        // writable portion out when a write is called.

        peer.expectTransfer().withHandle(0)
                             .withSettled(true)
                             .withState().accepted()
                             .withDeliveryId(0)
                             .withMore(true)
                             .withDeliveryTag(new byte[] {1});
        peer.expectTransfer().withHandle(0)
                             .withSettled(true)
                             .withState().accepted()
                             .withDeliveryId(0)
                             .withMore(false)
                             .withDeliveryTag(nullValue());
        peer.expectTransfer().withHandle(1)
                             .withSettled(true)
                             .withState().accepted()
                             .withDeliveryId(1)
                             .withMore(bothDeliveriesMultiFrame)
                             .withDeliveryTag(new byte[] {2});
        if (bothDeliveriesMultiFrame) {
            peer.expectTransfer().withHandle(1)
                                 .withSettled(true)
                                 .withState().accepted()
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withDeliveryTag(nullValue());
        }

        ProtonBuffer messageContent1 = createContentBuffer(contentLength1);
        OutgoingDelivery delivery1 = sender1.next();
        delivery1.setTag(new byte[] { 1 });
        delivery1.disposition(Accepted.getInstance(), true);
        delivery1.writeBytes(messageContent1);

        ProtonBuffer messageContent2 = createContentBuffer(contentLength2);
        OutgoingDelivery delivery2 = sender2.next();
        delivery2.setTag(new byte[] { 2 });
        delivery2.disposition(Accepted.getInstance(), true);
        delivery2.writeBytes(messageContent2);

        peer.expectClose().respond();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testMaxFrameSizeOfPeerHasEffect() {
        doMaxFrameSizeTestImpl(0, 0, 5700, 1);
        doMaxFrameSizeTestImpl(1024, 0, 5700, 6);
    }

    @Test
    public void testMaxFrameSizeOutgoingFrameSizeLimitHasEffect() {
        doMaxFrameSizeTestImpl(0, 512, 5700, 12);
        doMaxFrameSizeTestImpl(1024, 512, 5700, 12);
        doMaxFrameSizeTestImpl(1024, 2048, 5700, 6);
    }

    void doMaxFrameSizeTestImpl(int remoteMaxFrameSize, int outboundFrameSizeLimit, int contentLength, int expectedNumFrames) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        if (outboundFrameSizeLimit == 0) {
            if (remoteMaxFrameSize == 0) {
                peer.expectOpen().respond();
            } else {
                peer.expectOpen().respond().withMaxFrameSize(remoteMaxFrameSize);
            }
        } else {
            if (remoteMaxFrameSize == 0) {
                peer.expectOpen().withMaxFrameSize(outboundFrameSizeLimit).respond();
            } else {
                peer.expectOpen().withMaxFrameSize(outboundFrameSizeLimit)
                                 .respond()
                                 .withMaxFrameSize(remoteMaxFrameSize);
            }
        }
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();

        Connection connection = engine.start();
        if (outboundFrameSizeLimit != 0) {
            connection.setMaxFrameSize(outboundFrameSizeLimit);
        }
        connection.open();
        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(handler.isSendable());
        });

        peer.remoteFlow().withHandle(0)
                         .withDeliveryCount(0)
                         .withLinkCredit(50)
                         .withIncomingWindow(65535)
                         .withOutgoingWindow(65535)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).now();

        assertTrue(senderMarkedSendable.get(), "Sender should now be sendable");

        // This calculation isn't entirely precise, there is some added performative/frame overhead not
        // accounted for...but values are chosen to work, and verified here.
        final int frameCount;
        if (remoteMaxFrameSize == 0 && outboundFrameSizeLimit == 0) {
            frameCount = 1;
        } else if(remoteMaxFrameSize == 0 && outboundFrameSizeLimit != 0) {
            frameCount = (int) Math.ceil((double)contentLength / (double) outboundFrameSizeLimit);
        } else {
            int effectiveMaxFrameSize;
            if (outboundFrameSizeLimit != 0) {
                effectiveMaxFrameSize = Math.min(outboundFrameSizeLimit, remoteMaxFrameSize);
            } else {
                effectiveMaxFrameSize = remoteMaxFrameSize;
            }

            frameCount = (int) Math.ceil((double)contentLength / (double) effectiveMaxFrameSize);
        }

        assertEquals(expectedNumFrames, frameCount, "Unexpected number of frames calculated");

        for (int i = 1; i <= expectedNumFrames; ++i) {
            peer.expectTransfer().withHandle(0)
                                 .withSettled(true)
                                 .withState().accepted()
                                 .withDeliveryId(0)
                                 .withMore(i != expectedNumFrames ? true : false)
                                 .withDeliveryTag(i == 1 ? notNullValue() : nullValue())
                                 .withNonNullPayload();
        }

        ProtonBuffer messageContent = createContentBuffer(contentLength);
        OutgoingDelivery delivery = sender.next();
        delivery.setTag(new byte[] { 1 });
        delivery.disposition(Accepted.getInstance(), true);
        delivery.writeBytes(messageContent);

        peer.expectClose().respond();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCompleteInProgressDeliveryWithFinalEmptyTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withAborted(anyOf(nullValue(), is(false)))
                             .withSettled(false)
                             .withMore(anyOf(nullValue(), is(false)))
                             .withNullPayload();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), false);
        delivery.streamBytes(null, true);

        assertFalse(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertFalse(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAbortInProgressDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withAborted(true)
                             .withSettled(true)
                             .withMore(anyOf(nullValue(), is(false)))
                             .withNullPayload();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        delivery.abort();

        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAbortAlreadyAbortedDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withAborted(true)
                             .withSettled(true)
                             .withMore(anyOf(nullValue(), is(false)))
                             .withNullPayload();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        assertTrue(sender.hasUnsettled());

        delivery.abort();

        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        // Second abort attempt should not error out or trigger additional frames
        delivery.abort();

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAbortOnDeliveryThatHasNoWritesIsNoOp() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.abort();

        assertNull(sender.current());
        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAbortOnDeliveryThatHasNoWritesIsNoOpThenSendUsingCurrent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.abort();

        assertNull(sender.current());
        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        try {
            sender.next();
        } catch (IllegalStateException ise) {
            fail("Should not be able to next as current was not aborted since nothing was ever written.");
        }

        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withPayload(payload);
        peer.expectDisposition().withFirst(0).withSettled(true).withState().accepted();
        peer.expectDetach().withHandle(0).respond();

        delivery = sender.current();
        delivery.setTag(new byte[] {1}).writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        delivery.disposition(Accepted.getInstance(), true);

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSettleTransferWithNullDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(null, nullValue(), true);
    }

    @Test
    public void testSettleTransferWithAcceptedDisposition() throws Exception {
        DeliveryState state = Accepted.getInstance();
        AcceptedMatcher matcher = new AcceptedMatcher();
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, true);
    }

    @Test
    public void testUnsettledDispositionOfTransferWithAcceptedOutcome() throws Exception {
        DeliveryState state = Accepted.getInstance();
        AcceptedMatcher matcher = new AcceptedMatcher();
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, false);
    }

    @Test
    public void testSettleTransferWithReleasedDisposition() throws Exception {
        DeliveryState state = Released.getInstance();
        ReleasedMatcher matcher = new ReleasedMatcher();
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, true);
    }

    @Test
    public void testSettleTransferWithRejectedDisposition() throws Exception {
        DeliveryState state = new Rejected();
        RejectedMatcher matcher = new RejectedMatcher();
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, true);
    }

    @Test
    public void testSettleTransferWithRejectedWithErrorDisposition() throws Exception {
        DeliveryState state = new Rejected().setError(new ErrorCondition(AmqpError.DECODE_ERROR, "test"));
        RejectedMatcher matcher = new RejectedMatcher().withError(AmqpError.DECODE_ERROR.toString(), "test");
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, true);
    }

    @Test
    public void testSettleTransferWithModifiedDisposition() throws Exception {
        DeliveryState state = new Modified().setDeliveryFailed(true).setUndeliverableHere(true);
        ModifiedMatcher matcher = new ModifiedMatcher().withDeliveryFailed(true).withUndeliverableHere(true);
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, true);
    }

    @Test
    public void testSettleTransferWithTransactionalDisposition() throws Exception {
        DeliveryState state = new TransactionalState().setTxnId(new Binary(new byte[] {1})).setOutcome(Accepted.getInstance());
        TransactionalStateMatcher matcher =
            new TransactionalStateMatcher().withTxnId(new byte[] {1}).withOutcome(new AcceptedMatcher());
        doTestSettleTransferWithSpecifiedOutcome(state, matcher, true);
    }

    private void doTestSettleTransferWithSpecifiedOutcome(DeliveryState state, Matcher<?> stateMatcher, boolean settled) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0});
        peer.expectDisposition().withFirst(0)
                                .withSettled(settled)
                                .withState(stateMatcher);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSendable = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> sentDelivery = new AtomicReference<>();
        sender.creditStateUpdateHandler(handler -> {
            sentDelivery.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
            deliverySentAfterSendable.set(sender.isSendable());
        });

        sender.open();

        assertTrue(deliverySentAfterSendable.get(), "Delivery should have been sent after credit arrived");

        OutgoingDelivery delivery = sender.current();
        assertNull(delivery);
        sentDelivery.get().disposition(state, settled);

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAttemptedSecondDispositionOnAlreadySettledDeliveryNull() throws Exception {
        doTestAttemptedSecondDispositionOnAlreadySettledDelivery(Accepted.getInstance(), null);
    }

    @Test
    public void testAttemptedSecondDispositionOnAlreadySettledDeliveryReleased() throws Exception {
        doTestAttemptedSecondDispositionOnAlreadySettledDelivery(Accepted.getInstance(), Released.getInstance());
    }

    @Test
    public void testAttemptedSecondDispositionOnAlreadySettledDeliveryModified() throws Exception {
        doTestAttemptedSecondDispositionOnAlreadySettledDelivery(Released.getInstance(), new Modified().setDeliveryFailed(true));
    }

    @Test
    public void testAttemptedSecondDispositionOnAlreadySettledDeliveryRejected() throws Exception {
        doTestAttemptedSecondDispositionOnAlreadySettledDelivery(Released.getInstance(), new Rejected());
    }

    @Test
    public void testAttemptedSecondDispositionOnAlreadySettledDeliveryTransactional() throws Exception {
        doTestAttemptedSecondDispositionOnAlreadySettledDelivery(Released.getInstance(), new TransactionalState().setOutcome(Accepted.getInstance()));
    }

    private void doTestAttemptedSecondDispositionOnAlreadySettledDelivery(DeliveryState first, DeliveryState second) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0});
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState(notNullValue());
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");
        final AtomicReference<OutgoingDelivery> sentDelivery = new AtomicReference<>();

        final AtomicBoolean deliverySentAfterSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            sentDelivery.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
            deliverySentAfterSendable.set(sender.isSendable());
        });

        sender.open();

        assertTrue(deliverySentAfterSendable.get(), "Delivery should have been sent after credit arrived");

        OutgoingDelivery delivery = sender.current();
        assertNull(delivery);
        sentDelivery.get().disposition(first, true);

        // A second attempt at the same outcome should result in no action.
        sentDelivery.get().disposition(first, true);

        try {
            sentDelivery.get().disposition(second, true);
            fail("Should not be able to update outcome on already settled delivery");
        } catch (IllegalStateException ise) {
            // Expected
        }

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSettleSentDeliveryAfterRemoteSettles() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .accept();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSendable = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> sentDelivery = new AtomicReference<>();
        sender.creditStateUpdateHandler(handler -> {
            sentDelivery.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
            deliverySentAfterSendable.set(sender.isSendable());
        });

        sender.deliveryStateUpdatedHandler((delivery) -> {
            if (delivery.isRemotelySettled()) {
                delivery.settle();
            }
        });

        sender.open();

        assertTrue(deliverySentAfterSendable.get(), "Delivery should have been sent after credit arrived");

        assertNull(sender.current());

        assertTrue(sentDelivery.get().isRemotelySettled());
        assertSame(Accepted.getInstance(), sentDelivery.get().getRemoteState());
        assertNull(sentDelivery.get().getState());
        assertTrue(sentDelivery.get().isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderHandlesDeferredOpenAndBeginAttachResponses() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();
        peer.expectAttach().withRole(Role.SENDER.getValue())
                           .withTarget().withDynamic(true).withAddress((String) null);

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.setTarget(new Target().setDynamic(true).setAddress(null));
        sender.openHandler(result -> senderRemotelyOpened.set(true)).open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        // This should happen after we inject the held open and attach
        peer.expectClose().respond();

        // Inject held responses to get the ball rolling again
        peer.remoteOpen().withOfferedCapabilities("ANONYMOUS_RELAY").now();
        peer.respondToLastBegin().now();
        peer.respondToLastAttach().now();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");
        assertNotNull(sender.<Target>getRemoteTarget().getAddress());

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, true, true);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, true, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponseBeginWrittenAndNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, false, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, false, false, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenNotWritten() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(false, false, false, false);
    }

    private void testCloseAfterShutdownNoOutputAndNoException(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin, boolean respondToAttach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                if (respondToBegin) {
                    peer.expectBegin().respond();
                    if (respondToAttach) {
                        peer.expectAttach().respond();
                    } else {
                        peer.expectAttach();
                    }
                } else {
                    peer.expectBegin();
                    peer.expectAttach();
                }
            } else {
                peer.expectOpen();
                peer.expectBegin();
                peer.expectAttach();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();

        engine.shutdown();

        // Should clean up and not throw as we knowingly shutdown engine operations.
        sender.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true, true);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponseBeginWrittenAndNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, false, false, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenNotWritten() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(false, false, false, false);
    }

    private void testCloseAfterEngineFailedThrowsAndNoOutputWritten(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin, boolean respondToAttach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                if (respondToBegin) {
                    peer.expectBegin().respond();
                    if (respondToAttach) {
                        peer.expectAttach().respond();
                    } else {
                        peer.expectAttach();
                    }
                } else {
                    peer.expectBegin();
                    peer.expectAttach();
                }
                peer.expectClose();
            } else {
                peer.expectOpen();
                peer.expectBegin();
                peer.expectAttach();
                peer.expectClose();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();

        engine.engineFailed(new IOException());

        try {
            sender.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        try {
            session.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        try {
            connection.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        engine.shutdown();  // Explicit shutdown now allows local close to complete

        // Should clean up and not throw as we knowingly shutdown engine operations.
        sender.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
    }

    @Test
    public void testCloseReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(true);
    }

    @Test
    public void testDetachReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(false);
    }

    private void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
        final String condition = "amqp:link:detach-forced";
        final String description = "something bad happened.";

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().withClosed(close)
                           .withError(condition, description)
                           .respond();
        peer.expectClose();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();
        sender.setCondition(new ErrorCondition(Symbol.valueOf(condition), description));

        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderDrainedWhenNotDraining() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(10).withDrain(false).queue();
        peer.expectDetach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.creditStateUpdateHandler(link -> link.drained());
        sender.open();

        assertEquals(10, sender.getCredit());

        sender.close();

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderDrainedWhenDrainSignaledButNoCreditGiven() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(0).withDrain(false).queue();
        peer.expectDetach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.creditStateUpdateHandler(link -> link.drained());
        sender.open();

        assertEquals(0, sender.getCredit());

        sender.close();

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderSignalsDrainedWhenCreditOutstanding() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(10).withDrain(true).queue();
        peer.expectFlow().withDeliveryCount(10).withLinkCredit(0).withDrain(true);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.creditStateUpdateHandler(link -> link.drained());
        sender.open();
        sender.close();

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderOmitsFlowWhenDrainedCreditIsSatisfied() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(1).withDrain(true).queue();

        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .accept();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSendable = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> sentDelivery = new AtomicReference<>();
        sender.creditStateUpdateHandler(link -> {
            if (link.isSendable()) {
                sentDelivery.set(link.next().setTag(new byte[] {0}).writeBytes(payload));
                deliverySentAfterSendable.set(true);
            }
        });

        sender.deliveryStateUpdatedHandler((delivery) -> {
            if (delivery.isRemotelySettled()) {
                delivery.settle();
            }
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        // Should not send a flow as the send fulfilled the requested drain amount.
        sender.drained();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderAppliesDeliveryTagGeneratorToNextDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withIncomingWindow(10).withLinkCredit(10).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.SEQUENTIAL.createGenerator());
        sender.deliveryStateUpdatedHandler((delivery) -> {
            delivery.settle();
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload()
                             .withDeliveryTag(new byte[] {0}).accept();
        peer.expectTransfer().withNonNullPayload()
                             .withDeliveryTag(new byte[] {1}).accept();
        peer.expectTransfer().withNonNullPayload()
                             .withDeliveryTag(new byte[] {2}).accept();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(payload.duplicate());
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(payload.duplicate());
        OutgoingDelivery delivery3 = sender.next();
        delivery3.writeBytes(payload.duplicate());

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        assertNotNull(delivery1);
        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertNotNull(delivery2);
        assertTrue(delivery2.isSettled());
        assertTrue(delivery2.isRemotelySettled());
        assertNotNull(delivery3);
        assertTrue(delivery3.isSettled());
        assertTrue(delivery3.isRemotelySettled());

        peer.expectDetach().respond();
        peer.expectClose().respond();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderAppliedGeneratedDeliveryTagCanBeOverridden() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        sender.setDeliveryTagGenerator(generator);
        sender.open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {127})
                             .withPayload(payloadBuffer);
        peer.expectDetach().withHandle(0).respond();

        OutgoingDelivery delivery = sender.next();

        DeliveryTag oldTag = delivery.getTag();

        delivery.setTag(new byte[] {127});

        // Pooled tag should be reused.
        assertSame(oldTag, generator.nextTag());

        delivery.writeBytes(payload);

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderReleasesPooledDeliveryTagsAfterSettledByBoth() throws Exception {
        doTestSenderReleasesPooledDeliveryTags(false, true);
    }

    @Test
    public void testSenderReleasesPooledDeliveryTagsAfterSettledAfterDispositionUpdate() throws Exception {
        doTestSenderReleasesPooledDeliveryTags(false, false);
    }

    @Test
    public void testSenderReleasesPooledDeliveryTagsSenderSettlesFirst() throws Exception {
        doTestSenderReleasesPooledDeliveryTags(true, false);
    }

    private void doTestSenderReleasesPooledDeliveryTags(boolean sendSettled, boolean receiverSettles) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(10).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator());
        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        final int toSend = sender.getCredit();
        final byte[] expectedTag = new byte[] {0};

        List<OutgoingDelivery> sent = new ArrayList<>(toSend);

        for (int i = 0; i < toSend; ++i) {
            peer.expectTransfer().withHandle(0)
                                 .withSettled(sendSettled)
                                 .withState(nullValue())
                                 .withDeliveryId(i)
                                 .withMore(false)
                                 .withDeliveryTag(expectedTag)
                                 .respond()
                                 .withSettled(receiverSettles)
                                 .withState().accepted();
            if (!sendSettled && !receiverSettles) {
                peer.expectDisposition().withFirst(i)
                                        .withSettled(true)
                                        .withState(nullValue());
            }
        }

        for (int i = 0; i < toSend; ++i) {
            OutgoingDelivery delivery = sender.next();

            if (sendSettled) {
                delivery.settle();
            }

            delivery.writeBytes(payload.duplicate());

            if (!sendSettled) {
                delivery.settle();
            }
        }

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        sent.forEach(delivery -> assertEquals(delivery.getTag().tagBytes() , expectedTag));

        peer.expectDetach().respond();
        peer.expectClose().respond();

        // Should not send a flow as the send fulfilled the requested drain amount.
        sender.drained();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderHandlesDelayedDispositionsForSentTransfers() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withLinkCredit(10).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.SEQUENTIAL.createGenerator());
        sender.deliveryStateUpdatedHandler((delivery) -> {
            delivery.settle();
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload()
                             .withDeliveryTag(new byte[] {0});
        peer.expectTransfer().withNonNullPayload()
                             .withDeliveryTag(new byte[] {1});
        peer.expectTransfer().withNonNullPayload()
                             .withDeliveryTag(new byte[] {2});

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(payload.duplicate());
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(payload.duplicate());
        OutgoingDelivery delivery3 = sender.next();
        delivery3.writeBytes(payload.duplicate());

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        assertNotNull(delivery1);
        assertNotNull(delivery2);
        assertNotNull(delivery3);

        peer.remoteDisposition().withRole(Role.RECEIVER.getValue())
                                .withFirst(0)
                                .withSettled(true)
                                .withState().accepted().now();

        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertFalse(delivery2.isSettled());
        assertFalse(delivery2.isRemotelySettled());
        assertFalse(delivery3.isSettled());
        assertFalse(delivery3.isRemotelySettled());

        peer.remoteDisposition().withRole(Role.RECEIVER.getValue())
                                .withFirst(1)
                                .withSettled(true)
                                .withState().accepted().now();

        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertTrue(delivery2.isSettled());
        assertTrue(delivery2.isRemotelySettled());
        assertFalse(delivery3.isSettled());
        assertFalse(delivery3.isRemotelySettled());

        peer.remoteDisposition().withRole(Role.RECEIVER.getValue())
                                .withFirst(2)
                                .withSettled(true)
                                .withState().accepted().now();

        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertTrue(delivery2.isSettled());
        assertTrue(delivery2.isRemotelySettled());
        assertTrue(delivery3.isSettled());
        assertTrue(delivery3.isRemotelySettled());

        peer.expectDetach().respond();
        peer.expectClose().respond();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testNoDispositionSentWhenNoStateOrSettlementRequested() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender").open();

        final AtomicBoolean sender1MarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            sender1MarkedSendable.set(handler.isSendable());
        });

        peer.remoteFlow().withHandle(0)
                         .withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(1)
                         .withNextOutgoingId(1).now();

        assertTrue(sender1MarkedSendable.get(), "Sender 1 should now be sendable");

        // Frames are not multiplexed for large deliveries as we write the full
        // writable portion out when a write is called.

        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withNullState()
                             .withDeliveryId(0)
                             .withMore(false)
                             .withDeliveryTag(new byte[] {1});

        ProtonBuffer messageContent1 = createContentBuffer(32);
        OutgoingDelivery delivery1 = sender.next();
        delivery1.setTag(new byte[] { 1 });
        delivery1.writeBytes(messageContent1);

        // No action requested so no frame should be emitted.
        delivery1.disposition(null, false);

        peer.waitForScriptToComplete();
        peer.expectDisposition().withState().accepted();

        delivery1.disposition(Accepted.getInstance(), true);

        peer.expectClose().respond();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCannotAlterMessageFormatAfterInitialBytesWritten() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState(nullValue())
                             .withMessageFormat(42)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withMessageFormat(42)
                             .withAborted(anyOf(nullValue(), is(false)))
                             .withSettled(false)
                             .withMore(anyOf(nullValue(), is(false)))
                             .withPayload(payload);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        final OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.setMessageFormat(42);
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), false);

        assertThrows(IllegalStateException.class, () -> delivery.setMessageFormat(43));
        assertDoesNotThrow(() -> delivery.setMessageFormat(42));

        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), true);

        assertFalse(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertFalse(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCanUpdateAcceptedStateAfterInitialTransferAndSettle() throws Exception {
        doTestCanUpdateStateAndOrSettleAfterInitialTransfer(true);
    }

    @Test
    public void testCanUpdateAcceptedStateAfterInitialTransferDoNotSettle() throws Exception {
        doTestCanUpdateStateAndOrSettleAfterInitialTransfer(false);
    }

    private void doTestCanUpdateStateAndOrSettleAfterInitialTransfer(boolean settle) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState(nullValue())
                             .withMessageFormat(42)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withState().accepted()
                             .withDeliveryId(0)
                             .withMessageFormat(42)
                             .withAborted(anyOf(nullValue(), is(false)))
                             .withSettled(settle)
                             .withMore(anyOf(nullValue(), is(false)))
                             .withPayload(payload);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        final OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.setMessageFormat(42);
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), false);

        assertThrows(IllegalStateException.class, () -> delivery.setMessageFormat(43));
        assertDoesNotThrow(() -> delivery.setMessageFormat(42));

        delivery.disposition(Accepted.getInstance(), settle);
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), true);

        assertFalse(delivery.isAborted());
        assertFalse(delivery.isPartial());
        if (settle) {
            assertTrue(delivery.isSettled());
        } else {
            assertFalse(delivery.isSettled());
        }

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderNotSendableWhenRemoteIncomingWindowIsZero() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(0)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();

        final OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), false);

        assertFalse(sender.isSendable());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderBecomesSendableAfterRemoteIncomingWindowExpanded() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(0)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        {
            // Not expecting an update as we weren't yet able to send and still aren't
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            sender.open();

            assertTrue(senderCreditUpdated.await(5, TimeUnit.MILLISECONDS));
            assertFalse(sender.isSendable());
        }

        final OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        // Shouldn't generate any frames as there's no session capacity
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), false);

        {
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(0).now();

            assertTrue(senderCreditUpdated.await(10, TimeUnit.SECONDS));
            assertTrue(sender.isSendable());
        }

        peer.expectTransfer().withHandle(0)
                             .withMore(false)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectDetach().withHandle(0).respond();

        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        assertFalse(sender.isSendable());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderBecomesSendableAfterRemoteIncomingWindowExpandedSessionFlowSentBeforeAttach() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteFlow().withNullHandle()
                         .withIncomingWindow(0)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(0).queue();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();

        assertFalse(sender.isSendable());

        final OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        // Shouldn't generate any frames as there's no session capacity
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload), false);

        {
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(0).now();

            assertTrue(senderCreditUpdated.await(10, TimeUnit.SECONDS));
            assertTrue(sender.isSendable());
        }

        peer.expectTransfer().withHandle(0)
                             .withMore(false)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectDetach().withHandle(0).respond();

        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        assertFalse(sender.isSendable());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionRevokesIncomingWindowSetsSenderStateToNotSendableViaDirectLinkFlow() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteFlow().withIncomingWindow(1)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(0).queue();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();

        assertFalse(sender.isSendable());

        {
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(1)
                             .withIncomingWindow(1)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(0).now();

            assertTrue(senderCreditUpdated.await(10, TimeUnit.SECONDS));
            assertTrue(sender.isSendable());
        }

        {
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(1)
                             .withIncomingWindow(0)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(0).now();

            assertTrue(senderCreditUpdated.await(10, TimeUnit.SECONDS));
            assertFalse(sender.isSendable());
        }

        peer.expectDetach().withHandle(0).respond();

        // Should not generate any outgoing transfers as the delivery is not sendable
        final OutgoingDelivery delivery = sender.next();
        delivery.setTag(new byte[] {0});
        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionRevokesIncomingWindowSetsSenderStateToNotSendableViaSessionFlow() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteFlow().withIncomingWindow(1)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(0).queue();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();

        assertFalse(sender.isSendable());

        {
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(1)
                             .withIncomingWindow(1)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(0).now();

            assertTrue(senderCreditUpdated.await(10, TimeUnit.SECONDS));
            assertTrue(sender.isSendable());
        }

        {
            final CountDownLatch senderCreditUpdated = new CountDownLatch(1);
            sender.creditStateUpdateHandler(handler -> {
                senderCreditUpdated.countDown();
            });

            // Arrives at session level but impacts the links in the session.
            peer.remoteFlow().withNullHandle()
                             .withIncomingWindow(0)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(0).now();

            assertTrue(senderCreditUpdated.await(10, TimeUnit.SECONDS));
            assertFalse(sender.isSendable());
        }

        peer.expectDetach().withHandle(0).respond();

        // Should not generate any outgoing transfers as the delivery is not sendable
        final OutgoingDelivery delivery = sender.next();
        delivery.setTag(new byte[] {0});
        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderOnlyWritesToSessionRemoteIncomingLimitWriteBytes() throws Exception {
        doTestSenderOnlyWritesToSessionRemoteIncomingLimit(false);
    }

    @Test
    public void testSenderOnlyWritesToSessionRemoteIncomingLimitStreamBytes() throws Exception {
        doTestSenderOnlyWritesToSessionRemoteIncomingLimit(true);
    }

    private void doTestSenderOnlyWritesToSessionRemoteIncomingLimit(boolean streamBytes) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[1536];
        Arrays.fill(payload, (byte) 64);
        ProtonBuffer payloadBuffer = ProtonByteBufferAllocator.DEFAULT.wrap(payload);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver").withMaxFrameSize(1024);
        peer.expectBegin().respond().withIncomingWindow(1);
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(1)
                         .withIncomingWindow(1)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(0).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withNonNullPayload();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();

        final OutgoingDelivery delivery = sender.next();

        delivery.setTag(new byte[] {0});
        if (streamBytes) {
            delivery.streamBytes(payloadBuffer, true);
        } else {
            delivery.writeBytes(payloadBuffer);
        }

        assertTrue(delivery.isPartial());
        assertTrue(payloadBuffer.isReadable());
        assertNotEquals(payload.length, payloadBuffer.getReadableBytes());
        assertFalse(sender.isSendable());

        peer.remoteFlow().withIncomingWindow(1)
                         .withNextIncomingId(1)
                         .withLinkCredit(10).now();

        assertTrue(sender.isSendable());

        peer.expectTransfer().withHandle(0)
                             .withMore(false)
                             .withDeliveryId(0)
                             .withNonNullPayload();
        peer.expectDetach().withHandle(0).respond();

        if (streamBytes) {
            delivery.streamBytes(payloadBuffer, true);
        } else {
            delivery.writeBytes(payloadBuffer);
        }

        assertFalse(delivery.isPartial());
        assertFalse(payloadBuffer.isReadable());
        assertFalse(sender.isSendable());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderUpdateDeliveryUpdatedEventHandlerInDelivery() throws InterruptedException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().ofSender().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .respond()
                             .withSettled(true)
                             .withState().accepted();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        final CountDownLatch stateUpdated = new CountDownLatch(1);
        sender.creditStateUpdateHandler(link -> {
            if (link.isSendable()) {
                OutgoingDelivery delivery = sender.next();
                delivery.deliveryStateUpdatedHandler((outgoing) -> {
                    stateUpdated.countDown();
                });

                delivery.setTag(new byte[] {0});
                delivery.writeBytes(payload);
            }
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        assertTrue(stateUpdated.await(5, TimeUnit.SECONDS));

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTransferCountTracksOutgoingDeliveryLifecycle() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(10).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withDeliveryId(0)
                             .withDeliveryTag(anyOf(nullValue(), is(new byte[] {0})))
                             .withPayload(payload);
        peer.expectTransfer().withHandle(0)
                             .withState(nullValue())
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withAborted(true)
                             .withMore(anyOf(nullValue(), is(false)))
                             .withNullPayload();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1").open();

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        assertEquals(0, delivery.getTransferCount());

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        assertEquals(1, delivery.getTransferCount());

        delivery.streamBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        assertEquals(2, delivery.getTransferCount());

        delivery.abort();

        assertEquals(2, delivery.getTransferCount());

        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testDispositionFilterAppliesToOnlySubsetOfUnsettledMapSettledAndAccepted() {
        testDispositionFilterAppliesToOnlySubsetOfUnsettledMap(true, true);
    }

    @Test
    public void testDispositionFilterAppliesToOnlySubsetOfUnsettledMapSettledOnly() {
        testDispositionFilterAppliesToOnlySubsetOfUnsettledMap(true, false);
    }

    @Test
    public void testDispositionFilterAppliesToOnlySubsetOfUnsettledMapAcceptedOnly() {
        testDispositionFilterAppliesToOnlySubsetOfUnsettledMap(false, true);
    }

    private void testDispositionFilterAppliesToOnlySubsetOfUnsettledMap(boolean settled, boolean accepted) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);
        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(10).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(false)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withNonNullPayload();
        peer.expectTransfer().withHandle(0)
                             .withMore(false)
                             .withDeliveryId(1)
                             .withDeliveryTag(new byte[] {1})
                             .withNonNullPayload();
        peer.expectTransfer().withHandle(0)
                             .withMore(false)
                             .withDeliveryId(2)
                             .withDeliveryTag(new byte[] {2})
                             .withNonNullPayload();
        if (!accepted) {
            peer.expectDisposition().withFirst(1).withSettled(settled).withState(nullValue());
        } else {
            peer.expectDisposition().withFirst(1).withSettled(settled).withState().accepted();
        }
        peer.expectDetach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.creditStateUpdateHandler(link -> link.drained());
        sender.open();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.setTag(new byte[] { 0 });
        delivery1.writeBytes(payload.duplicate());

        OutgoingDelivery delivery2 = sender.next();
        delivery2.setTag(new byte[] { 1 });
        delivery2.writeBytes(payload.duplicate());

        OutgoingDelivery delivery3 = sender.next();
        delivery3.setTag(new byte[] { 2 });
        delivery3.writeBytes(payload.duplicate());

        sender.disposition((delivery) -> {
            if (delivery.getTag().tagBuffer().equals(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {1}))) {
                return true;
            } else {
                return false;
            }
        }, accepted ? Accepted.getInstance() : null, settled);

        assertEquals(7, sender.getCredit());

        sender.close();

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderReportsDeliveryUpdatedOnDispositionForMultipleTransfers() throws Exception {
        final Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        final ProtonTestConnector peer = createTestPeer(engine);
        final byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(2).queue();
        peer.expectTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withPayload(payload);
        peer.expectTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withPayload(payload);
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted()
                                .withFirst(0)
                                .withLast(1).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test");

        final AtomicInteger dispositionCounter = new AtomicInteger();

        final ArrayList<OutgoingDelivery> deliveries = new ArrayList<>();

        sender.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                dispositionCounter.incrementAndGet();
                deliveries.add(delivery);
            }
        });

        sender.open();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.setTag(new byte[] { 0 });
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        OutgoingDelivery delivery2 = sender.next();
        delivery2.setTag(new byte[] { 1 });
        delivery2.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();

        sender.close();

        assertEquals(2, deliveries.size(), "Not all deliveries received dispositions");

        byte deliveryTag = 0;

        for (OutgoingDelivery delivery : deliveries) {
            assertEquals(deliveryTag++, delivery.getTag().tagBuffer().getByte(0), "Delivery not updated in correct order");
            assertTrue(delivery.isRemotelySettled(), "Delivery should be marked as remotely settled");
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderReportsIsSendableAfterOpenedIfRemoteSendsFlowBeforeLocallyOpened() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteAttach().withName("receiver")
                           .withHandle(0)
                           .withRole(Role.RECEIVER.getValue())
                           .withInitialDeliveryCount(0)
                           .onChannel(0).queue();
        peer.remoteFlow().withLinkCredit(1).queue();
        peer.expectAttach();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Sender> sender = new AtomicReference<>();

        Connection connection = engine.start();

        connection.senderOpenHandler(result -> {
            senderRemotelyOpened.set(true);
            sender.set(result);
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        connection.session().open();

        assertTrue(senderRemotelyOpened.get(), "Sender remote opened event did not fire");

        assertFalse(sender.get().isSendable());

        sender.get().open();

        assertTrue(sender.get().isSendable());

        sender.get().close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testWriteThatExceedConfiguredSessionIncomingCreditLimitOnTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().withNextOutgoingId(0).respond();
        peer.expectAttach().ofSender().respond();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().open();
        Sender sender = session.sender("test").open();

        int payloadOutstanding = 4800;
        final byte[] bytes = new byte[payloadOutstanding];
        Arrays.fill(bytes, (byte) 1);
        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(bytes);

        OutgoingDelivery delivery = sender.next().setTag(new byte[] { 0 });
        assertEquals(payload.getReadableBytes(), payloadOutstanding);
        delivery.writeBytes(payload);
        assertEquals(payload.getReadableBytes(), payloadOutstanding);

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(0).withLinkCredit(10).now();
        peer.expectTransfer().withNonNullPayload().withMore(true);

        delivery.writeBytes(payload);
        assertTrue(payload.getReadableBytes() < payloadOutstanding);  // Leave space for Transfer
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(1).withLinkCredit(10).now();
        peer.expectTransfer().withNonNullPayload().withMore(true);

        delivery.writeBytes(payload);
        assertTrue(payload.getReadableBytes() < payloadOutstanding, "Expected < " + payloadOutstanding + " but was: " + payload.getReadableBytes());
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(2).withLinkCredit(10).now();
        peer.expectTransfer().withNonNullPayload().withMore(true);

        delivery.writeBytes(payload);
        assertTrue(payload.getReadableBytes() < payloadOutstanding, "Expected < " + payloadOutstanding + " but was: " + payload.getReadableBytes());
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(3).withLinkCredit(10).now();
        peer.expectTransfer().withNonNullPayload().withMore(true);

        delivery.writeBytes(payload);
        assertTrue(payload.getReadableBytes() < payloadOutstanding, "Expected < " + payloadOutstanding + " but was: " + payload.getReadableBytes());
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(4).withLinkCredit(10).now();
        peer.expectTransfer().withNonNullPayload().withMore(false).accept();

        delivery.writeBytes(payload);
        assertEquals(0 , payload.getReadableBytes());

        peer.waitForScriptToComplete(500, TimeUnit.SECONDS);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testWriteThatExceedsConfiguredSessionIncomingCreditLimitOnTransferFromCreditUpdatedhandler() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().withNextOutgoingId(0).respond();
        peer.expectAttach().ofSender().respond();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().open();
        Sender sender = session.sender("test").open();

        int payloadOutstanding = 4800;
        final byte[] bytes = new byte[payloadOutstanding];
        Arrays.fill(bytes, (byte) 1);
        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(bytes);

        final OutgoingDelivery delivery = sender.next().setTag(new byte[] { 0 });
        assertEquals(payload.getReadableBytes(), payloadOutstanding);
        delivery.writeBytes(payload);
        assertEquals(payload.getReadableBytes(), payloadOutstanding);

        sender.creditStateUpdateHandler((theSender) -> {
            delivery.writeBytes(payload);
        });

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload().withMore(true);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(0).withLinkCredit(10).now();

        assertTrue(payload.getReadableBytes() < payloadOutstanding);  // Leave space for Transfer
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload().withMore(true);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(1).withLinkCredit(10).now();

        assertTrue(payload.getReadableBytes() < payloadOutstanding, "Expected < " + payloadOutstanding + " but was: " + payload.getReadableBytes());
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload().withMore(true);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(2).withLinkCredit(10).now();

        assertTrue(payload.getReadableBytes() < payloadOutstanding, "Expected < " + payloadOutstanding + " but was: " + payload.getReadableBytes());
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload().withMore(true);
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(3).withLinkCredit(10).now();

        assertTrue(payload.getReadableBytes() < payloadOutstanding, "Expected < " + payloadOutstanding + " but was: " + payload.getReadableBytes());
        payloadOutstanding = payload.getReadableBytes();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withNonNullPayload().withMore(false).accept();
        peer.remoteFlow().withIncomingWindow(1).withNextIncomingId(4).withLinkCredit(10).now();

        assertEquals(0 , payload.getReadableBytes());

        peer.waitForScriptToComplete(500, TimeUnit.SECONDS);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }
}
