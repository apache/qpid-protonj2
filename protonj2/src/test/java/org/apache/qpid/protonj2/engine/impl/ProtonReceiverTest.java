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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.LinkState;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineShutdownException;
import org.apache.qpid.protonj2.engine.util.SimplePojo;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test the {@link ProtonReceiver}
 */
@Timeout(20)
public class ProtonReceiverTest extends ProtonEngineTestSupport {

    public static final Symbol[] SUPPORTED_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                                     Rejected.DESCRIPTOR_SYMBOL,
                                                                     Released.DESCRIPTOR_SYMBOL,
                                                                     Modified.DESCRIPTOR_SYMBOL };

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

        Receiver receiver = session.receiver("test");

        receiver.setProperties(new HashMap<>());

        receiver.open();

        try {
            receiver.setProperties(new HashMap<>());
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            receiver.setDesiredCapabilities(new Symbol[] { AmqpError.DECODE_ERROR });
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            receiver.setOfferedCapabilities(new Symbol[] { AmqpError.DECODE_ERROR });
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            receiver.setSenderSettleMode(SenderSettleMode.MIXED);
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            receiver.setSource(new Source());
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            receiver.setTarget(new Target());
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        try {
            receiver.setMaxMessageSize(UnsignedLong.ZERO);
            fail("Cannot alter local link initial state data after sender opened.");
        } catch (IllegalStateException ise) {
            // Expected
        }

        receiver.detach();
        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverEmitsOpenAndCloseEvents() throws Exception {
        doTestReceiverEmitsEvents(false);
    }

    @Test
    public void testReceiverEmitsOpenAndDetachEvents() throws Exception {
        doTestReceiverEmitsEvents(true);
    }

    private void doTestReceiverEmitsEvents(boolean detach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean receiverLocalOpen = new AtomicBoolean();
        final AtomicBoolean receiverLocalClose = new AtomicBoolean();
        final AtomicBoolean receiverLocalDetach = new AtomicBoolean();
        final AtomicBoolean receiverRemoteOpen = new AtomicBoolean();
        final AtomicBoolean receiverRemoteClose = new AtomicBoolean();
        final AtomicBoolean receiverRemoteDetach = new AtomicBoolean();

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

        Receiver receiver = session.receiver("test");
        receiver.localOpenHandler(result -> receiverLocalOpen.set(true))
                .localCloseHandler(result -> receiverLocalClose.set(true))
                .localDetachHandler(result -> receiverLocalDetach.set(true))
                .openHandler(result -> receiverRemoteOpen.set(true))
                .detachHandler(result -> receiverRemoteDetach.set(true))
                .closeHandler(result -> receiverRemoteClose.set(true));

        receiver.open();

        if (detach) {
            receiver.detach();
        } else {
            receiver.close();
        }

        assertTrue(receiverLocalOpen.get(), "Receiver should have reported local open");
        assertTrue(receiverRemoteOpen.get(), "Receiver should have reported remote open");

        if (detach) {
            assertFalse(receiverLocalClose.get(), "Receiver should not have reported local close");
            assertTrue(receiverLocalDetach.get(), "Receiver should have reported local detach");
            assertFalse(receiverRemoteClose.get(), "Receiver should not have reported remote close");
            assertTrue(receiverRemoteDetach.get(), "Receiver should have reported remote close");
        } else {
            assertTrue(receiverLocalClose.get(), "Receiver should have reported local close");
            assertFalse(receiverLocalDetach.get(), "Receiver should not have reported local detach");
            assertTrue(receiverRemoteClose.get(), "Receiver should have reported remote close");
            assertFalse(receiverRemoteDetach.get(), "Receiver should not have reported remote close");
        }

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverRoutesDetachEventToCloseHandlerIfNoneSet() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean receiverLocalOpen = new AtomicBoolean();
        final AtomicBoolean receiverLocalClose = new AtomicBoolean();
        final AtomicBoolean receiverRemoteOpen = new AtomicBoolean();
        final AtomicBoolean receiverRemoteClose = new AtomicBoolean();

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

        Receiver receiver = session.receiver("test");
        receiver.localOpenHandler(result -> receiverLocalOpen.set(true))
                .localCloseHandler(result -> receiverLocalClose.set(true))
                .openHandler(result -> receiverRemoteOpen.set(true))
                .closeHandler(result -> receiverRemoteClose.set(true));

        receiver.open();
        receiver.detach();

        assertTrue(receiverLocalOpen.get(), "Receiver should have reported local open");
        assertTrue(receiverRemoteOpen.get(), "Receiver should have reported remote open");
        assertTrue(receiverLocalClose.get(), "Receiver should have reported local detach");
        assertTrue(receiverRemoteClose.get(), "Receiver should have reported remote detach");

        session.close();

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

        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.engineShutdownHandler(result -> engineShutdown.set(true));

        if (locallyClosed) {
            if (remotelyClosed) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            receiver.close();
        }

        if (remotelyClosed && !locallyClosed) {
            peer.remoteDetach().now();
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
    public void testReceiverOpenWithNoSenderOrReceiverSettleModes() throws Exception {
        doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(null, null);
    }

    @Test
    public void testReceiverOpenWithSettledAndFirst() throws Exception {
        doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.SETTLED, ReceiverSettleMode.FIRST);
    }

    @Test
    public void testReceiverOpenWithUnsettledAndSecond() throws Exception {
        doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.UNSETTLED, ReceiverSettleMode.SECOND);
    }

    private void doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode senderMode, ReceiverSettleMode receiverMode) {
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

        Receiver receiver = session.receiver("test");
        receiver.setSenderSettleMode(senderMode);
        receiver.setReceiverSettleMode(receiverMode);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();

        if (senderMode != null) {
            assertEquals(senderMode, receiver.getSenderSettleMode());
        } else {
            assertEquals(SenderSettleMode.MIXED, receiver.getSenderSettleMode());
        }
        if (receiverMode != null) {
            assertEquals(receiverMode, receiver.getReceiverSettleMode());
        } else {
            assertEquals(ReceiverSettleMode.FIRST, receiver.getReceiverSettleMode());
        }

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCreateReceiverAndInspectRemoteEndpoint() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource(notNullValue())
                           .withTarget(notNullValue())
                           .respond();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        final Modified defaultOutcome = new Modified().setDeliveryFailed(true);
        final String sourceAddress = UUID.randomUUID().toString() + ":1";

        Source source = new Source();
        source.setAddress(sourceAddress);
        source.setOutcomes(SUPPORTED_OUTCOMES);
        source.setDefaultOutcome(defaultOutcome);

        Receiver receiver = session.receiver("test");
        receiver.setSource(source);
        receiver.setTarget(new Target());
        receiver.open();

        assertTrue(receiver.getRemoteState().equals(LinkState.ACTIVE));
        assertNotNull(receiver.getRemoteSource());
        assertNotNull(receiver.getRemoteTarget());
        assertArrayEquals(SUPPORTED_OUTCOMES, receiver.getRemoteSource().getOutcomes());
        assertTrue(receiver.getRemoteSource().getDefaultOutcome() instanceof Modified);
        assertEquals(sourceAddress, receiver.getRemoteSource().getAddress());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCreateReceiverAndClose() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(true);
    }

    @Test
    public void testCreateReceiverAndDetach() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachLink(boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
        peer.expectDetach().withClosed(close).respond();
        peer.expectClose().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("test");
        receiver.open();

        assertTrue(receiver.isReceiver());
        assertFalse(receiver.isSender());

        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverOpenAndCloseAreIdempotent() throws Exception {
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
        Receiver receiver = session.receiver("test");
        receiver.open();

        // Should not emit another attach frame
        receiver.open();

        receiver.close();

        // Should not emit another detach frame
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAttachAfterLocalReceiverOpened() throws Exception {
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
        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.close();

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
        Receiver receiver = session.receiver("test");
        receiver.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.openHandler(result -> {
            receiverRemotelyOpened.set(true);
        });
        receiver.open();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireClosedEventAfterRemoteDetachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyClosed = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.openHandler(result -> {
            receiverRemotelyOpened.set(true);
        });
        receiver.closeHandler(result -> {
            receiverRemotelyClosed.set(true);
        });
        receiver.open();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");

        receiver.close();

        assertTrue(receiverRemotelyClosed.get(), "Receiver remote closed event did not fire");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testRemotelyCloseReceiverAndOpenNewReceiverImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, false);
    }

    @Test
    public void testRemotelyDetachReceiverAndOpenNewReceiverImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(false, false);
    }

    @Test
    public void testRemotelyCloseReceiverAndOpenNewReceiverImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, true);
    }

    @Test
    public void testRemotelyDetachReceiverAndOpenNewReceiverImmediatelyAfterWithSameLinkName() throws Exception {
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
        peer.expectAttach().withHandle(0).withRole(Role.RECEIVER.getValue()).respond();
        peer.remoteDetach().withClosed(close).queue();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyClosed = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyDetached = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver(firstLinkName);
        receiver.openHandler(result -> receiverRemotelyOpened.set(true));
        receiver.closeHandler(result -> receiverRemotelyClosed.set(true));
        receiver.detachHandler(result -> receiverRemotelyDetached.set(true));
        receiver.open();

        peer.waitForScriptToComplete();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");

        if (close) {
            assertTrue(receiverRemotelyClosed.get(), "Receiver remote closed event did not fire");
            assertFalse(receiverRemotelyDetached.get(), "Receiver remote detached event fired");
        } else {
            assertFalse(receiverRemotelyClosed.get(), "Receiver remote closed event fired");
            assertTrue(receiverRemotelyDetached.get(), "Receiver remote closed event did not fire");
        }

        peer.expectDetach().withClosed(close);
        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();
        peer.expectAttach().withHandle(0).withRole(Role.RECEIVER.getValue()).respond();
        peer.expectDetach().withClosed(close).respond();

        // Reset trackers
        receiverRemotelyOpened.set(false);
        receiverRemotelyClosed.set(false);
        receiverRemotelyDetached.set(false);

        receiver = session.receiver(secondLinkName);
        receiver.openHandler(result -> receiverRemotelyOpened.set(true));
        receiver.closeHandler(result -> receiverRemotelyClosed.set(true));
        receiver.detachHandler(result -> receiverRemotelyDetached.set(true));
        receiver.open();

        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");

        if (close) {
            assertTrue(receiverRemotelyClosed.get(), "Receiver remote closed event did not fire");
            assertFalse(receiverRemotelyDetached.get(), "Receiver remote detached event fired");
        } else {
            assertFalse(receiverRemotelyClosed.get(), "Receiver remote closed event fired");
            assertTrue(receiverRemotelyDetached.get(), "Receiver remote closed event did not fire");
        }

        assertNull(failure);
    }

    @Test
    public void testReceiverFireOpenedEventAfterRemoteAttachArrivesWithNullTarget() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond().withNullSource();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.setSource(new Source());
        receiver.setTarget(new Target());
        receiver.openHandler(result -> {
            receiverRemotelyOpened.set(true);
        });
        receiver.open();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");
        assertNull(receiver.getRemoteSource());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleReceivers() throws Exception {
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

        Receiver receiver1 = session.receiver("receiver-1");
        receiver1.open();
        Receiver receiver2 = session.receiver("receiver-2");
        receiver2.open();

        // Close in reverse order
        receiver2.close();
        receiver1.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionSignalsRemoteReceiverOpen() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteAttach().withName("sender")
                           .withHandle(0)
                           .withRole(Role.SENDER.getValue())
                           .withInitialDeliveryCount(0).queue();
        peer.expectAttach();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Receiver> receiver = new AtomicReference<>();

        Connection connection = engine.start();

        connection.receiverOpenHandler(result -> {
            receiverRemotelyOpened.set(true);
            receiver.set(result);
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");

        receiver.get().open();
        receiver.get().close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenReceiverAfterSessionClosed() throws Exception {
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

        Receiver receiver = session.receiver("test");

        session.close();

        try {
            receiver.open();
            fail("Should not be able to open a link from a closed session.");
        } catch (IllegalStateException ise) {}

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenReceiverAfterSessionRemotelyClosed() throws Exception {
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
        Receiver receiver = session.receiver("test");
        session.open();

        try {
            receiver.open();
            fail("Should not be able to open a link from a remotely closed session.");
        } catch (IllegalStateException ise) {}

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenReceiverBeforeOpenConnection() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Create the connection but don't open, then open a session and a receiver and
        // the session begin and receiver attach shouldn't go out until the connection
        // is opened locally.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER.getValue()).respond();

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenReceiverBeforeOpenSession() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its attach until the session
        // is opened.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER.getValue()).respond();

        // Now open the session, expect the Begin, and Attach frames
        session.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDetachAfterEndSent() {
        doTestReceiverCloseOrDetachAfterEndSent(false);
    }

    @Test
    public void testReceiverCloseAfterEndSent() {
        doTestReceiverCloseOrDetachAfterEndSent(true);
    }

    public void doTestReceiverCloseOrDetachAfterEndSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER.getValue()).respond();
        peer.expectEnd().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its detach if the session has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        // Cause an End frame to be sent
        session.close();

        // The sender should not emit an end as the session was closed which implicitly
        // detached the link.
        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDetachAfterCloseSent() {
        doTestReceiverClosedOrDetachedAfterCloseSent(false);
    }

    @Test
    public void testReceiverCloseAfterCloseSent() {
        doTestReceiverClosedOrDetachedAfterCloseSent(true);
    }

    public void doTestReceiverClosedOrDetachedAfterCloseSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER.getValue()).respond();
        peer.expectClose().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its detach if the connection has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        // Cause an Close frame to be sent
        connection.close();

        // The receiver should not emit an detach as the connection was closed which implicitly
        // detached the link.
        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowWhenCreditSet() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond().withNextOutgoingId(42);
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100).withNextIncomingId(42);
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.addCredit(100);
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowWithNoIncomingIdWhenRemoteBeginHasNotArrivedYet() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin();
        peer.expectAttach();
        peer.expectFlow().withLinkCredit(100).withNextIncomingId(nullValue());

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open();

        receiver.addCredit(100);

        final CountDownLatch opened = new CountDownLatch(1);
        receiver.openHandler((self) -> {
            opened.countDown();
        });

        peer.waitForScriptToComplete();
        peer.respondToLastBegin().withNextOutgoingId(42).now();
        peer.respondToLastAttach().now();
        peer.expectFlow().withLinkCredit(101).withNextIncomingId(42);
        peer.expectDetach().respond();

        assertTrue(opened.await(10, TimeUnit.SECONDS));

        receiver.addCredit(1);

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowAfterOpenedWhenCreditSetBeforeOpened() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.addCredit(100);
        receiver.open();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowAfterConnectionOpenFinallySent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");

        // Create and open all resources except don't open the connection and then
        // we will observe that the receiver flow doesn't fire until it has sent its
        // attach following the session send its Begin.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.addCredit(1);
        receiver.open();

        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);

        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test //TODO: questionable. If its going to no-op the credit then it should perhaps not do this (open before parent) to begin with, as strange to send the attaches but not credit?
    public void testReceiverOmitsFlowAfterConnectionOpenFinallySentWhenAfterDetached() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Create and open all resources except don't open the connection and then
        // we will observe that the receiver flow doesn't fire since the link was
        // detached prior to being able to send any state updates.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.addCredit(1);
        receiver.open();
        receiver.detach();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        connection.open();

        peer.waitForScriptToComplete();

        assertEquals(0, receiver.getCredit());
        assertNull(failure);
    }

    @Test
    public void testReceiverDrainAllOutstanding() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        int creditWindow = 100;

        // Add some credit, verify not draining
        Matcher<Boolean> notDrainingMatcher = anyOf(equalTo(false), nullValue());
        peer.expectFlow().withDrain(notDrainingMatcher).withLinkCredit(creditWindow).withDeliveryCount(0);
        receiver.addCredit(creditWindow);

        peer.waitForScriptToComplete();

        // Check that calling drain sends flow, and calls handler on response draining all credit
        AtomicBoolean handlerCalled = new AtomicBoolean();
        receiver.creditStateUpdateHandler(x -> {
            handlerCalled.set(true);
        });

        peer.expectFlow().withDrain(true).withLinkCredit(creditWindow).withDeliveryCount(0)
                         .respond()
                         .withDrain(true).withLinkCredit(0).withDeliveryCount(creditWindow);

        receiver.drain();

        peer.waitForScriptToComplete();
        assertTrue(handlerCalled.get(), "Handler was not called");

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDrainWithNoCreditResultInNoOutput() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open();

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();

        receiver.drain();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDrainAllowsOnlyOnePendingDrain() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        int creditWindow = 100;

        // Add some credit, verify not draining
        Matcher<Boolean> notDrainingMatcher = anyOf(equalTo(false), nullValue());
        peer.expectFlow().withDrain(notDrainingMatcher).withLinkCredit(creditWindow).withDeliveryCount(0);
        receiver.addCredit(creditWindow);

        peer.waitForScriptToComplete();

        peer.expectFlow().withDrain(true).withLinkCredit(creditWindow).withDeliveryCount(0);

        receiver.drain();

        assertThrows(IllegalStateException.class, () -> receiver.drain());

        peer.waitForScriptToComplete();

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDrainWithCreditsAllowsOnlyOnePendingDrain() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open();

        final int creditWindow = 100;

        peer.waitForScriptToComplete();

        peer.expectFlow().withDrain(true).withLinkCredit(creditWindow).withDeliveryCount(0);

        receiver.drain(creditWindow);

        assertThrows(IllegalStateException.class, () -> receiver.drain(creditWindow));

        peer.waitForScriptToComplete();

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverThrowsOnAddCreditAfterConnectionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        connection.close();

        try {
            receiver.addCredit(100);
            fail("Should not be able to add credit after connection was closed");
        } catch (IllegalStateException ise) {
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverThrowsOnAddCreditAfterSessionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        session.close();

        try {
            receiver.addCredit(100);
            fail("Should not be able to add credit after session was closed");
        } catch (IllegalStateException ise) {
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDispatchesIncomingDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });
        receiver.open();
        receiver.addCredit(100);
        receiver.close();

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Deliver should not be partial");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsDispositionForTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Deliver should not be partial");
        assertFalse(receiver.hasUnsettled());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsDispositionOnlyOnceForTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Deliver should not be partial");

        // Already settled so this should trigger error
        try {
            receivedDelivery.get().disposition(Released.getInstance(), true);
            fail("Should not be able to set a second disposition");
        } catch (IllegalStateException ise) {
            // Expected that we can't settle twice.
        }

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsUpdatedDispositionsForTransferBeforeSettlement() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(false)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().released();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), false);
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Deliver should not be partial");
        assertTrue(receiver.hasUnsettled());

        // Second disposition should be sent as we didn't settle previously.
        receivedDelivery.get().disposition(Released.getInstance(), true);

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsUpdatedDispositionsForTransferBeforeSettlementThenSettles() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(false)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted();
        peer.expectDisposition().withFirst(0)
                                .withSettled(false)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().released();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().released();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance());
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Deliver should not be partial");
        assertTrue(receiver.hasUnsettled());

        // Second disposition should be sent as we didn't settle previously.
        receivedDelivery.get().disposition(Released.getInstance());
        receivedDelivery.get().settle();

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    /**
     * Verify that no Disposition frame is emitted by the Transport should a Delivery
     * have disposition applied after the Close frame was sent.
     */
    @Test
    public void testDispositionNoAllowedAfterCloseSent() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectClose();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        receiver.open();
        receiver.addCredit(1);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Deliver should not be partial");

        connection.close();

        try {
            receivedDelivery.get().disposition(Released.getInstance());
            fail("Should not be able to set a disposition after the connection was closed");
        } catch (IllegalStateException ise) {}

        try {
            receivedDelivery.get().disposition(Released.getInstance(), true);
            fail("Should not be able to update a disposition after the connection was closed");
        } catch (IllegalStateException ise) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDisposition() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.SENDER.getValue())
                                .withState().accepted()
                                .withFirst(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        final AtomicBoolean deliveryUpdatedAndSettled = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> updatedDelivery = new AtomicReference<>();
        receiver.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        receiver.open();
        receiver.addCredit(100);
        receiver.close();

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Delivery should not be partial");
        assertFalse(updatedDelivery.get().isPartial(), "Delivery should not be partial");
        assertTrue(deliveryUpdatedAndSettled.get(), "Delivery should have been updated to settled");
        assertSame(receivedDelivery.get(), updatedDelivery.get(), "Delivery should be same object as first received");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfers() throws Exception {
        doTestReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfers(0);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfersDeliveryIdOverflows() throws Exception {
        doTestReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfers(Integer.MAX_VALUE);
    }

    private void doTestReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfers(int firstDeliveryId) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(firstDeliveryId)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteTransfer().withDeliveryId(firstDeliveryId + 1)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.SENDER.getValue())
                                .withState().accepted()
                                .withFirst(firstDeliveryId)
                                .withLast(firstDeliveryId + 1).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicInteger dispositionCounter = new AtomicInteger();

        final ArrayList<IncomingDelivery> deliveries = new ArrayList<>();

        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
        });

        receiver.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                dispositionCounter.incrementAndGet();
                deliveries.add(delivery);
            }
        });

        receiver.open();
        receiver.addCredit(2);
        receiver.close();

        assertEquals(2, deliveryCounter.get(), "Not all deliveries arrived");
        assertEquals(2, deliveries.size(), "Not all deliveries received dispositions");

        byte deliveryTag = 0;

        for (IncomingDelivery delivery : deliveries) {
            assertEquals(deliveryTag++, delivery.getTag().tagBuffer().getByte(0), "Delivery not updated in correct order");
            assertTrue(delivery.isRemotelySettled(), "Delivery should be marked as remotely settled");
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedNextFrameForMultiFrameTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        String text = "test-string-for-split-frame-delivery";
        byte[] encoded = text.getBytes(StandardCharsets.UTF_8);
        byte[] first = Arrays.copyOfRange(encoded, 0, encoded.length / 2);
        byte[] second = Arrays.copyOfRange(encoded, encoded.length / 2, encoded.length);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withBody().withData(first).also().queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withBody().withData(second).also().queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicInteger deliverReads = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
            deliverReads.incrementAndGet();
        });

        receiver.open();
        receiver.addCredit(2);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Delivery should not be partial");
        assertEquals(2, deliverReads.get(), "Deliver should have been read twice for two transfers");
        assertSame(receivedDelivery.get(), receivedDelivery.get(), "Delivery should be same object as first received");

        ProtonBuffer payload = receivedDelivery.get().readAll();

        assertNotNull(payload);

        // We are cheating a bit here as this isn't how the encoding would normally work.
        Data section1 = decoder.readObject(payload, decoderState, Data.class);
        Data section2 = decoder.readObject(payload, decoderState, Data.class);

        Binary data1 = section1.getBinary();
        Binary data2 = section2.getBinary();

        ProtonBuffer combined = ProtonByteBufferAllocator.DEFAULT.allocate(encoded.length);

        combined.writeBytes(data1.asByteBuffer());
        combined.writeBytes(data2.asByteBuffer());

        assertEquals(text, combined.toString(StandardCharsets.UTF_8), "Encoded and Decoded strings don't match");

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsUpdateWhenLastFrameOfMultiFrameTransferHasNoPayload() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        String text = "test-string-for-split-frame-delivery";
        byte[] encoded = text.getBytes(StandardCharsets.UTF_8);
        byte[] first = Arrays.copyOfRange(encoded, 0, encoded.length / 2);
        byte[] second = Arrays.copyOfRange(encoded, encoded.length / 2, encoded.length);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withBody().withData(first).also().queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withBody().withData(second).also().queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withMore(true)
                             .withMessageFormat(0)
                             .queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withMore(false)
                             .withMessageFormat(0)
                             .queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicInteger deliverReads = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
            deliverReads.incrementAndGet();
        });

        receiver.open();
        receiver.addCredit(1);

        assertTrue(deliveryArrived.get(), "Delivery did not arrive at the receiver");
        assertFalse(receivedDelivery.get().isPartial(), "Delivery should not be partial");
        assertEquals(4, deliverReads.get(), "Deliver should have been read twice for two transfers");
        assertSame(receivedDelivery.get(), receivedDelivery.get(), "Delivery should be same object as first received");

        ProtonBuffer payload = receivedDelivery.get().readAll();

        assertNotNull(payload);

        // We are cheating a bit here as this isn't how the encoding would normally work.
        Data section1 = decoder.readObject(payload, decoderState, Data.class);
        Data section2 = decoder.readObject(payload, decoderState, Data.class);

        Binary data1 = section1.getBinary();
        Binary data2 = section2.getBinary();

        ProtonBuffer combined = ProtonByteBufferAllocator.DEFAULT.allocate(encoded.length);

        combined.writeBytes(data1.asByteBuffer());
        combined.writeBytes(data2.asByteBuffer());

        assertEquals(text, combined.toString(StandardCharsets.UTF_8), "Encoded and Decoded strings don't match");

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testMultiplexMultiFrameDeliveriesOnSingleSessionIncoming() throws Exception {
        doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(true);
    }

    @Test
    public void testMultiplexMultiFrameDeliveryOnSingleSessionIncoming() throws Exception {
        doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(false);
    }

    private void doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(boolean bothDeliveriesMultiFrame) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver-1").respond();
        peer.expectAttach().withHandle(1).withName("receiver-2").respond();
        peer.expectFlow().withHandle(0).withLinkCredit(5);
        peer.expectFlow().withHandle(1).withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver1 = session.receiver("receiver-1");
        Receiver receiver2 = session.receiver("receiver-2");

        final String delivery1LinkedResource = "Delivery1";
        final String delivery2LinkedResource = "Delivery2";

        final AtomicReference<IncomingDelivery> receivedDelivery1 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery2 = new AtomicReference<>();

        final AtomicBoolean delivery1Updated = new AtomicBoolean();
        final AtomicBoolean delivery2Updated = new AtomicBoolean();

        final String deliveryTag1 = "tag1";
        final String deliveryTag2 = "tag2";

        final byte[] payload1 = new byte[] { 1, 1 };
        final byte[] payload2 = new byte[] { 2, 2 };

        // Receiver 1 handlers for delivery processing.
        receiver1.deliveryReadHandler(delivery -> {
            receivedDelivery1.set(delivery);
            delivery.setLinkedResource(delivery1LinkedResource);
        });
        receiver1.deliveryStateUpdatedHandler(delivery -> {
            delivery1Updated.set(true);
            assertEquals(delivery1LinkedResource, delivery.getLinkedResource());
            assertEquals(delivery1LinkedResource, delivery.getLinkedResource(String.class));
            final String autoCasted = delivery.getLinkedResource();
            assertEquals(delivery1LinkedResource, autoCasted);
        });

        // Receiver 2 handlers for delivery processing.
        receiver2.deliveryReadHandler(delivery -> {
            receivedDelivery2.set(delivery);
            delivery.setLinkedResource(delivery2LinkedResource);
        });
        receiver2.deliveryStateUpdatedHandler(delivery -> {
            delivery2Updated.set(true);
            assertEquals(delivery2LinkedResource, delivery.getLinkedResource());
            assertEquals(delivery2LinkedResource, delivery.getLinkedResource(String.class));
            final String autoCasted = delivery.getLinkedResource();
            assertEquals(delivery2LinkedResource, autoCasted);
        });

        receiver1.open();
        receiver2.open();

        receiver1.addCredit(5);
        receiver2.addCredit(5);

        assertNull(receivedDelivery1.get(), "Should not have any delivery data yet on receiver 1");
        assertNull(receivedDelivery2.get(), "Should not have any delivery date yet on receiver 2");

        peer.remoteTransfer().withDeliveryId(0)
                             .withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(payload1).now();
        peer.remoteTransfer().withDeliveryId(1)
                             .withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(bothDeliveriesMultiFrame)
                             .withMessageFormat(0)
                             .withPayload(payload2).now();

        assertNotNull(receivedDelivery1.get(), "Should have a delivery event on receiver 1");
        assertNotNull(receivedDelivery2.get(), "Should have a delivery event on receiver 2");

        assertTrue(receivedDelivery1.get().isPartial(), "Delivery on Receiver 1 Should not be complete");
        if (bothDeliveriesMultiFrame) {
            assertTrue(receivedDelivery2.get().isPartial(), "Delivery on Receiver 2 Should be complete");
        } else {
            assertFalse(receivedDelivery2.get().isPartial(), "Delivery on Receiver 2 Should not be complete");
        }

        peer.remoteTransfer().withDeliveryId(0)
                             .withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload1).now();
        if (bothDeliveriesMultiFrame) {
            peer.remoteTransfer().withDeliveryId(1)
                                 .withHandle(1)
                                 .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).now();
        }

        assertFalse(receivedDelivery1.get().isPartial(), "Delivery on Receiver 1 Should be complete");
        assertFalse(receivedDelivery2.get().isPartial(), "Delivery on Receiver 2 Should be complete");

        peer.expectDisposition().withFirst(1)
                                .withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER.getValue())
                                .withState().accepted();

        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag().tagBuffer().getArray());

        ProtonBuffer payloadBuffer1 = receivedDelivery1.get().readAll();
        ProtonBuffer payloadBuffer2 = receivedDelivery2.get().readAll();

        assertEquals(payload1.length * 2, payloadBuffer1.getReadableBytes(), "Received 1 payload size is wrong");
        assertEquals(payload2.length * (bothDeliveriesMultiFrame ? 2 : 1), payloadBuffer2.getReadableBytes(), "Received 2 payload size is wrong");

        receivedDelivery2.get().disposition(Accepted.getInstance(), true);
        receivedDelivery1.get().disposition(Accepted.getInstance(), true);

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverDeliveryIdTrackingHandlesAbortedDelivery() {
        // Check aborted=true, more=false, settled=true.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(false, true);
        // Check aborted=true, more=false, settled=unset(false)
        // Aborted overrides settled not being set.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(false, null);
        // Check aborted=true, more=false, settled=false
        // Aborted overrides settled being explicitly false.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(false, false);
        // Check aborted=true, more=true, settled=true
        // Aborted overrides the more=true.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(true, true);
        // Check aborted=true, more=true, settled=unset(false)
        // Aborted overrides the more=true, and settled being unset.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(true, null);
        // Check aborted=true, more=true, settled=false
        // Aborted overrides the more=true, and settled explicitly false.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(true, false);
    }

    private void doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(boolean setMoreOnAbortedTransfer, Boolean setSettledOnAbortedTransfer) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver");
        receiver.addCredit(2);

        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> abortedDelivery = new AtomicReference<>();
        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicBoolean deliveryUpdated = new AtomicBoolean();
        final byte[] payload = new byte[] { 1 };

        // Receiver 1 handlers for delivery processing.
        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            if (delivery.isAborted()) {
                abortedDelivery.set(delivery);
            } else {
                receivedDelivery.set(delivery);
            }
        });
        receiver.deliveryStateUpdatedHandler(delivery -> {
            deliveryUpdated.set(true);
        });

        receiver.open();

        assertNull(receivedDelivery.get(), "Should not have any delivery data yet on receiver 1");
        assertEquals(0, deliveryCounter.get(), "Should not have any delivery data yet on receiver 1");
        assertFalse(deliveryUpdated.get(), "Should not have any delivery data yet on receiver 1");

        // First chunk indicates more to come.
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull(receivedDelivery.get(), "Should have delivery data on receiver");
        assertEquals(1, deliveryCounter.get(), "Should have delivery data on receiver");
        assertFalse(deliveryUpdated.get(), "Should not have any delivery updates yet on receiver");

        // Second chunk indicates more to come as a twist but also signals aborted.
        peer.remoteTransfer().withDeliveryId(0)
                             .withSettled(setSettledOnAbortedTransfer)
                             .withMore(setMoreOnAbortedTransfer)
                             .withAborted(true)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull(receivedDelivery.get(), "Should have delivery data on receiver");
        assertEquals(2, deliveryCounter.get(), "Should have delivery data on receiver");
        assertFalse(deliveryUpdated.get(), "Should not have a delivery updates on receiver");
        assertTrue(receivedDelivery.get().isAborted(), "Should now show that delivery is aborted");
        assertTrue(receivedDelivery.get().isRemotelySettled(), "Should now show that delivery is remotely settled");
        assertNull(receivedDelivery.get().readAll(), "Aborted Delivery should discard read bytes");

        // Another delivery now which should arrive just fine, no further frames on this one.
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull(abortedDelivery.get(), "Should have one aborted delivery");
        assertNotNull(receivedDelivery.get(), "Should have delivery data on receiver");
        assertNotSame(abortedDelivery.get(), receivedDelivery.get(), "Should have a final non-aborted delivery");
        assertEquals(3, deliveryCounter.get(), "Should have delivery data on receiver");
        assertFalse(deliveryUpdated.get(), "Should not have a delivery updates on receiver");
        assertFalse(receivedDelivery.get().isAborted(), "Should now show that delivery is not aborted");
        assertEquals(2, receivedDelivery.get().getTag().tagBuffer().getByte(0), "Should have delivery tagged as two");

        // Test that delivery count updates correctly on next flow
        peer.expectFlow().withLinkCredit(10).withDeliveryCount(2);

        receiver.addCredit(10);

        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testAbortedTransferRemovedFromUnsettledListOnceSettledRemoteSettles() {
        doTestAbortedTransferRemovedFromUnsettledListOnceSettled(true);
    }

    @Test
    public void testAbortedTransferRemovedFromUnsettledListOnceSettledRemoteDoesNotSettle() {
        doTestAbortedTransferRemovedFromUnsettledListOnceSettled(false);
    }

    private void doTestAbortedTransferRemovedFromUnsettledListOnceSettled(boolean remoteSettled) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver");
        receiver.addCredit(1);

        final AtomicReference<IncomingDelivery> abortedDelivery = new AtomicReference<>();
        final byte[] payload = new byte[] { 1 };

        // Receiver 1 handlers for delivery processing.
        receiver.deliveryReadHandler(delivery -> {
            if (delivery.isAborted()) {
                abortedDelivery.set(delivery);
            }
        });

        receiver.open();

        // Send one chunk then abort to check that local side can settle and clear
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(payload).now();
        peer.remoteTransfer().withDeliveryId(0)
                             .withSettled(remoteSettled)
                             .withMore(false)
                             .withAborted(true)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull(abortedDelivery.get(), "should have one aborted delivery");

        assertTrue(receiver.hasUnsettled());
        assertEquals(1, receiver.unsettled().size());
        abortedDelivery.get().settle();
        assertFalse(receiver.hasUnsettled());
        assertEquals(0, receiver.unsettled().size());

        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testDeliveryWithIdOmittedOnContinuationTransfers() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver-1").respond();
        peer.expectAttach().withHandle(1).withName("receiver-2").respond();
        peer.expectFlow().withHandle(0).withLinkCredit(5);
        peer.expectFlow().withHandle(1).withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver1 = session.receiver("receiver-1");
        Receiver receiver2 = session.receiver("receiver-2");

        final AtomicReference<IncomingDelivery> receivedDelivery1 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery2 = new AtomicReference<>();

        final AtomicInteger receiver1Transfers = new AtomicInteger();
        final AtomicInteger receiver2Transfers = new AtomicInteger();

        final AtomicBoolean delivery1Updated = new AtomicBoolean();
        final AtomicBoolean delivery2Updated = new AtomicBoolean();

        final String deliveryTag1 = "tag1";
        final String deliveryTag2 = "tag2";

        // Receiver 1 handlers for delivery processing.
        receiver1.deliveryReadHandler(delivery -> {
            receivedDelivery1.set(delivery);
            receiver1Transfers.incrementAndGet();
        });
        receiver1.deliveryStateUpdatedHandler(delivery -> {
            delivery1Updated.set(true);
            receiver1Transfers.incrementAndGet();
        });

        // Receiver 2 handlers for delivery processing.
        receiver2.deliveryReadHandler(delivery -> {
            receivedDelivery2.set(delivery);
            receiver2Transfers.incrementAndGet();
        });
        receiver2.deliveryStateUpdatedHandler(delivery -> {
            delivery2Updated.set(true);
            receiver2Transfers.incrementAndGet();
        });

        receiver1.open();
        receiver2.open();

        receiver1.addCredit(5);
        receiver2.addCredit(5);

        assertNull(receivedDelivery1.get(), "Should not have any delivery data yet on receiver 1");
        assertNull(receivedDelivery2.get(), "Should not have any delivery date yet on receiver 2");
        assertEquals(0, receiver1Transfers.get(), "Receiver 1 should not have any transfers yet");
        assertEquals(0, receiver2Transfers.get(), "Receiver 2 should not have any transfers yet");

        peer.remoteTransfer().withDeliveryId(0)
                             .withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {1}).now();
        peer.remoteTransfer().withDeliveryId(1)
                             .withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {10}).now();

        assertNotNull(receivedDelivery1.get(), "Should have a delivery event on receiver 1");
        assertNotNull(receivedDelivery2.get(), "Should have a delivery event on receiver 2");
        assertEquals(1, receiver1Transfers.get(), "Receiver 1 should have 1 transfers");
        assertEquals(1, receiver2Transfers.get(), "Receiver 2 should have 1 transfers");
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        peer.remoteTransfer().withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {11}).now();
        peer.remoteTransfer().withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {2}).now();

        assertNotNull(receivedDelivery1.get(), "Should have a delivery event on receiver 1");
        assertNotNull(receivedDelivery2.get(), "Should have a delivery event on receiver 2");
        assertEquals(2, receiver1Transfers.get(), "Receiver 1 should have 2 transfers");
        assertEquals(2, receiver2Transfers.get(), "Receiver 2 should have 2 transfers");
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        peer.remoteTransfer().withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {3}).now();
        peer.remoteTransfer().withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {12}).now();

        assertNotNull(receivedDelivery1.get(), "Should have a delivery event on receiver 1");
        assertNotNull(receivedDelivery2.get(), "Should have a delivery event on receiver 2");
        assertEquals(3, receiver1Transfers.get(), "Receiver 1 should have 3 transfers");
        assertEquals(3, receiver2Transfers.get(), "Receiver 2 should have 3 transfers");
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        peer.remoteTransfer().withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {13}).now();

        assertNotNull(receivedDelivery1.get(), "Should have a delivery event on receiver 1");
        assertNotNull(receivedDelivery2.get(), "Should have a delivery event on receiver 2");
        assertEquals(3, receiver1Transfers.get(), "Receiver 1 should have 3 transfers");
        assertEquals(4, receiver2Transfers.get(), "Receiver 2 should have 4 transfers");
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());
        assertFalse(receivedDelivery1.get().isPartial(), "Delivery on Receiver 1 Should be complete");
        assertFalse(receivedDelivery2.get().isPartial(), "Delivery on Receiver 2 Should be complete");

        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag().tagBuffer().getArray());

        ProtonBuffer delivery1Buffer = receivedDelivery1.get().readAll();
        ProtonBuffer delivery2Buffer = receivedDelivery2.get().readAll();

        for (int i = 1; i < 4; ++i) {
            assertEquals(i, delivery1Buffer.readByte());
        }

        for (int i = 10; i < 14; ++i) {
            assertEquals(i, delivery2Buffer.readByte());
        }

        assertNull(receivedDelivery1.get().readAll());
        assertNull(receivedDelivery2.get().readAll());

        peer.expectDetach().withHandle(0).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver1.close();
        receiver2.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testDeliveryIdThresholdsAndWraps() {
        // Check start from 0
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.valueOf(2));
        // Check run up to max-int (interesting boundary for underlying impl)
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(Integer.MAX_VALUE - 2), UnsignedInteger.valueOf(Integer.MAX_VALUE -1), UnsignedInteger.valueOf(Integer.MAX_VALUE));
        // Check crossing from signed range value into unsigned range value (interesting boundary for underlying impl)
        long maxIntAsLong = Integer.MAX_VALUE;
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(maxIntAsLong), UnsignedInteger.valueOf(maxIntAsLong + 1L), UnsignedInteger.valueOf(maxIntAsLong + 2L));
        // Check run up to max-uint
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(0xFFFFFFFFL - 2), UnsignedInteger.valueOf(0xFFFFFFFFL - 1), UnsignedInteger.MAX_VALUE);
        // Check wrapping from max unsigned value back to min(/0).
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.MAX_VALUE, UnsignedInteger.ZERO, UnsignedInteger.ONE);
    }

    private void doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger deliveryId1, UnsignedInteger deliveryId2, UnsignedInteger deliveryId3) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond().withNextOutgoingId(deliveryId1.intValue());
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver");

        final AtomicReference<IncomingDelivery> receivedDelivery1 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery2 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery3 = new AtomicReference<>();

        final AtomicInteger deliveryCounter = new AtomicInteger();

        final String deliveryTag1 = "tag1";
        final String deliveryTag2 = "tag2";
        final String deliveryTag3 = "tag3";

        // Receiver handlers for delivery processing.
        receiver.deliveryReadHandler(delivery -> {
            switch (deliveryCounter.get()) {
                case 0:
                    receivedDelivery1.set(delivery);
                    break;
                case 1:
                    receivedDelivery2.set(delivery);
                    break;
                case 2:
                    receivedDelivery3.set(delivery);
                    break;
                default:
                    break;
            }
            deliveryCounter.incrementAndGet();
        });
        receiver.deliveryStateUpdatedHandler(delivery -> {
            deliveryCounter.incrementAndGet();
        });

        receiver.open();
        receiver.addCredit(5);

        assertNull(receivedDelivery1.get(), "Should not have received delivery 1");
        assertNull(receivedDelivery2.get(), "Should not have received delivery 2");
        assertNull(receivedDelivery3.get(), "Should not have received delivery 3");
        assertEquals(0, deliveryCounter.get(), "Receiver should not have any deliveries yet");

        peer.remoteTransfer().withDeliveryId(deliveryId1.intValue())
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMessageFormat(0)
                             .withPayload(new byte[] {1}).now();

        assertNotNull(receivedDelivery1.get(), "Should have received delivery 1");
        assertNull(receivedDelivery2.get(), "Should not have received delivery 2");
        assertNull(receivedDelivery3.get(), "Should not have received delivery 3");
        assertEquals(1, deliveryCounter.get(), "Receiver should have 1 deliveries now");

        peer.remoteTransfer().withDeliveryId(deliveryId2.intValue())
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMessageFormat(0)
                             .withPayload(new byte[] {2}).now();

        assertNotNull(receivedDelivery1.get(), "Should have received delivery 1");
        assertNotNull(receivedDelivery2.get(), "Should have received delivery 2");
        assertNull(receivedDelivery3.get(), "Should not have received delivery 3");
        assertEquals(2, deliveryCounter.get(), "Receiver should have 2 deliveries now");

        peer.remoteTransfer().withDeliveryId(deliveryId3.intValue())
                             .withDeliveryTag(deliveryTag3.getBytes(StandardCharsets.UTF_8))
                             .withMessageFormat(0)
                             .withPayload(new byte[] {3}).now();

        assertNotNull(receivedDelivery1.get(), "Should have received delivery 1");
        assertNotNull(receivedDelivery2.get(), "Should have received delivery 2");
        assertNotNull(receivedDelivery3.get(), "Should have received delivery 3");
        assertEquals(3, deliveryCounter.get(), "Receiver should have 3 deliveries now");

        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get(), "delivery duplicate detected");
        assertNotSame(receivedDelivery2.get(), receivedDelivery3.get(), "delivery duplicate detected");
        assertNotSame(receivedDelivery1.get(), receivedDelivery3.get(), "delivery duplicate detected");

        // Verify deliveries arrived with expected payload
        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag3.getBytes(StandardCharsets.UTF_8), receivedDelivery3.get().getTag().tagBuffer().getArray());

        ProtonBuffer delivery1Buffer = receivedDelivery1.get().readAll();
        ProtonBuffer delivery2Buffer = receivedDelivery2.get().readAll();
        ProtonBuffer delivery3Buffer = receivedDelivery3.get().readAll();

        assertEquals(1, delivery1Buffer.readByte(), "Delivery 1 payload not as expected");
        assertEquals(2, delivery2Buffer.readByte(), "Delivery 2 payload not as expected");
        assertEquals(3, delivery3Buffer.readByte(), "Delivery 3 payload not as expected");

        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverFlowSentAfterAttachWrittenWhenCreditPrefilled() throws Exception {
        doTestReceiverFlowSentAfterAttachWritten(true);
    }

    @Test
    public void testReceiverFlowSentAfterAttachWrittenWhenCreditAddedBeforeAttachResponse() throws Exception {
        doTestReceiverFlowSentAfterAttachWritten(false);
    }

    private void doTestReceiverFlowSentAfterAttachWritten(boolean creditBeforeOpen) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();

        Receiver receiver = session.receiver("receiver");

        if (creditBeforeOpen) {
            // Add credit before open, no frame should be written until opened.
            receiver.addCredit(5);
        }

        // Expect attach but don't respond to observe that flow is sent regardless.
        peer.waitForScriptToComplete();
        peer.expectAttach();
        peer.expectFlow().withLinkCredit(5).withDeliveryCount(nullValue());

        receiver.open();

        if (!creditBeforeOpen) {
            // Add credit after open, frame should be written regardless of no attach response
            receiver.addCredit(5);
        }

        peer.respondToLastAttach().now();
        peer.waitForScriptToComplete();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.detach();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverHandlesDeferredOpenAndBeginAttachResponses() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();
        peer.expectAttach().withRole(Role.RECEIVER.getValue())
                           .withSource().withDynamic(true)
                           .withAddress((String) null);

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver-1");
        receiver.setSource(new Source().setDynamic(true).setAddress(null));
        receiver.openHandler(result -> receiverRemotelyOpened.set(true)).open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        // This should happen after we inject the held open and attach
        peer.expectClose().respond();

        // Inject held responses to get the ball rolling again
        peer.remoteOpen().withOfferedCapabilities("ANONYMOUS_RELAY").now();
        peer.respondToLastBegin().now();
        peer.respondToLastAttach().now();

        assertTrue(receiverRemotelyOpened.get(), "Receiver remote opened event did not fire");
        assertNotNull(receiver.getRemoteSource().getAddress());

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

        Receiver receiver = session.receiver("test");
        receiver.open();

        engine.shutdown();

        // Should clean up and not throw as we knowingly shutdown engine operations.
        receiver.close();
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

        Receiver receiver = session.receiver("test");
        receiver.open();

        engine.engineFailed(new IOException());

        try {
            receiver.close();
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
        receiver.close();
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

    public void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
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

        Receiver receiver = session.receiver("receiver-1");
        receiver.open();
        receiver.setCondition(new ErrorCondition(Symbol.valueOf(condition), description));

        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testReceiverAddCreditFailsAfterReceiverLocallyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, false, false);
    }

    @Test
    public void testReceiverAddCreditFailsAfterReceiverLocallyDetached() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, false, true);
    }

    @Test
    public void testReceiverAddCreditFailsAfterReceiverRemotelyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(false, true, false);
    }

    @Test
    public void testReceiverAddCreditFailsAfterReceiverRemotelyDetached() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(false, true, true);
    }

    @Test
    public void testReceiverAddCreditFailsAfterReceiverFullyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, true, false);
    }

    @Test
    public void testReceiverAddCreditFailsAfterReceiverFullyDetached() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, true, true);
    }

    private void doTestReceiverAddCreditFailsWhenLinkIsNotOperable(boolean localClose, boolean remoteClose, boolean detach) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        if (localClose) {
            if (remoteClose) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            if (detach) {
                receiver.detach();
            } else {
                receiver.close();
            }
        } else if (remoteClose) {
            peer.remoteDetach().withClosed(!detach).now();
        }

        try {
            receiver.addCredit(2);
            fail("Receiver should not allow addCredit to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverAddCreditFailsAfterSessionLocallyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenSessionNotOperable(true, false);
    }

    @Test
    public void testReceiverAddCreditFailsAfterSessionRemotelyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenSessionNotOperable(false, true);
    }

    @Test
    public void testReceiverAddCreditFailsAfterSessionFullyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenSessionNotOperable(true, true);
    }

    private void doTestReceiverAddCreditFailsWhenSessionNotOperable(boolean localClose, boolean remoteClose) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        if (localClose) {
            if (remoteClose) {
                peer.expectEnd().respond();
            } else {
                peer.expectEnd();
            }

            session.close();
        } else if (remoteClose) {
            peer.remoteEnd().now();
        }

        try {
            receiver.addCredit(2);
            fail("Receiver should not allow addCredit to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverAddCreditFailsAfterConnectionLocallyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenConnectionNotOperable(true, false);
    }

    @Test
    public void testReceiverAddCreditFailsAfterConnectionRemotelyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenConnectionNotOperable(false, true);
    }

    @Test
    public void testReceiverAddCreditFailsAfterConnectionFullyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenConnectionNotOperable(true, true);
    }

    private void doTestReceiverAddCreditFailsWhenConnectionNotOperable(boolean localClose, boolean remoteClose) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        if (localClose) {
            if (remoteClose) {
                peer.expectClose().respond();
            } else {
                peer.expectClose();
            }

            connection.close();
        } else if (remoteClose) {
            peer.remoteClose().now();
        }

        try {
            receiver.addCredit(2);
            fail("Receiver should not allow addCredit to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDispositionFailsAfterReceiverLocallyClosed() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, false, false);
    }

    @Test
    public void testReceiverDispositionFailsAfterReceiverLocallyDetached() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, false, true);
    }

    @Test
    public void testReceiverDispositionFailsAfterReceiverRemotelyClosed() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(false, true, false);
    }

    @Test
    public void testReceiverDispositionFailsAfterReceiverRemotelyDetached() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(false, true, true);
    }

    @Test
    public void testReceiverDispositionFailsAfterReceiverFullyClosed() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, true, false);
    }

    @Test
    public void testReceiverDispositionFailsAfterReceiverFullyDetached() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, true, true);
    }

    private void doTestReceiverDispositionFailsWhenLinkIsNotOperable(boolean localClose, boolean remoteClose, boolean detach) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        // Should no-op with no deliveries
        receiver.disposition(delivery -> true, Accepted.getInstance(), true);

        if (localClose) {
            if (remoteClose) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            if (detach) {
                receiver.detach();
            } else {
                receiver.close();
            }
        } else if (remoteClose) {
            peer.remoteDetach().withClosed(!detach).now();
        }

        try {
            receiver.disposition(delivery -> true, Accepted.getInstance(), true);
            fail("Receiver should not allow disposition to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testDrainCreditAmountLessThanCurrentCreditThrowsIAE() throws Exception {
        doTestReceiverDrainThrowsIAEForCertainDrainAmountScenarios(10, 1);
    }

    @Test
    public void testDrainOfNegativeCreditAmountThrowsIAEWhenCreditIsZero() throws Exception {
        doTestReceiverDrainThrowsIAEForCertainDrainAmountScenarios(0, -1);
    }

    @Test
    public void testDrainOfNegativeCreditAmountThrowsIAEWhenCreditIsNotZero() throws Exception {
        doTestReceiverDrainThrowsIAEForCertainDrainAmountScenarios(10, -1);
    }

    private void doTestReceiverDrainThrowsIAEForCertainDrainAmountScenarios(int credit, int drain) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        if (credit > 0) {
            peer.expectFlow().withDrain(false).withLinkCredit(credit);
        }

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open().addCredit(credit);

        peer.waitForScriptToComplete();

        // Check that calling drain sends flow, and calls handler on response draining all credit
        AtomicBoolean handlerCalled = new AtomicBoolean();
        receiver.creditStateUpdateHandler(x -> {
            handlerCalled.set(true);
        });

        try {
            receiver.drain(drain);
            fail("Should not be able to drain given amount");
        } catch (IllegalArgumentException iae) {}

        peer.waitForScriptToComplete();
        assertFalse(handlerCalled.get(), "Handler was called when no flow expected");

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testDrainRequestWithNoCreditPendingAndAmountRequestedAsZero() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        peer.waitForScriptToComplete();

        // Check that calling drain sends flow, and calls handler on response draining all credit
        AtomicBoolean handlerCalled = new AtomicBoolean();
        receiver.creditStateUpdateHandler(x -> {
            handlerCalled.set(true);
        });

        assertFalse(receiver.drain(0));

        peer.waitForScriptToComplete();
        assertFalse(handlerCalled.get(), "Handler was not called");

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDrainWithCreditsWhenNoCreditOutstanding() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        peer.waitForScriptToComplete();

        final int drainAmount = 100;

        // Check that calling drain sends flow, and calls handler on response draining all credit
        AtomicBoolean handlerCalled = new AtomicBoolean();
        receiver.creditStateUpdateHandler(x -> {
            handlerCalled.set(true);
        });

        peer.expectFlow().withDrain(true).withLinkCredit(drainAmount).withDeliveryCount(0)
                         .respond()
                         .withDrain(true).withLinkCredit(0).withDeliveryCount(drainAmount);

        receiver.drain(drainAmount);

        peer.waitForScriptToComplete();
        assertTrue(handlerCalled.get(), "Handler was not called");

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiveComplexEncodedAMQPMessageAndDecode() throws IOException {
        final String SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = "application/x-java-serialized-object";
        final String JMS_MSG_TYPE = "x-opt-jms-msg-type";

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withDrain(false).withLinkCredit(1);

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open().addCredit(1);

        peer.waitForScriptToComplete();

        final AtomicReference<IncomingDelivery> received = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            received.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });

        SimplePojo expectedContent = new SimplePojo(UUID.randomUUID());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(expectedContent);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        peer.expectDisposition().withState().accepted().withSettled(true);
        peer.remoteTransfer().withDeliveryTag(new byte[] {0})
                             .withDeliveryId(0)
                             .withProperties().withContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE).also()
                             .withMessageAnnotations().withAnnotation("x-opt-jms-msg-type", (byte) 1).also()
                             .withBody().withData(bytes).also()
                             .now();

        peer.waitForScriptToComplete();

        assertNotNull(received.get());

        ProtonBuffer buffer = received.get().readAll();

        MessageAnnotations annotations;
        Properties properties;
        Section<?> body;

        try {
            annotations = (MessageAnnotations) decoder.readObject(buffer, decoderState);
            assertNotNull(annotations);
            assertTrue(annotations.getValue().containsKey(Symbol.valueOf(JMS_MSG_TYPE)));
        } catch (Exception ex) {
            fail("Should not encounter error on decode of MessageAnnotations: " + ex);
        } finally {
            decoderState.reset();
        }

        try {
            properties = (Properties) decoder.readObject(buffer, decoderState);
            assertNotNull(properties);
            assertEquals(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, properties.getContentType());
        } catch (Exception ex) {
            fail("Should not encounter error on decode of Properties: " + ex);
        } finally {
            decoderState.reset();
        }

        try {
            body = (Section<?>) decoder.readObject(buffer, decoderState);
            assertNotNull(body);
            assertTrue(body instanceof Data);
            Data payload = (Data) body;
            assertEquals(bytes.length, payload.getBinary().getLength());
        } catch (Exception ex) {
            fail("Should not encounter error on decode of Body section: " + ex);
        } finally {
            decoderState.reset();
        }

        peer.expectClose().respond();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverCreditNotClearedUntilClosedAfterRemoteClosed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(10);
        peer.remoteDetach().queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open().addCredit(10);

        peer.waitForScriptToComplete();
        peer.expectDetach();

        assertEquals(10, receiver.getCredit());
        receiver.close();
        assertEquals(0, receiver.getCredit());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverCreditNotClearedUntilClosedAfterSessionRemoteClosed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(10);
        peer.remoteEnd().queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open().addCredit(10);

        peer.waitForScriptToComplete();
        peer.expectDetach();

        assertEquals(10, receiver.getCredit());
        receiver.close();
        assertEquals(0, receiver.getCredit());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverCreditNotClearedUntilClosedAfterConnectionRemoteClosed() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(10);
        peer.remoteClose().queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open().addCredit(10);

        peer.waitForScriptToComplete();
        peer.expectDetach();

        assertEquals(10, receiver.getCredit());
        receiver.close();
        assertEquals(0, receiver.getCredit());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverCreditNotClearedUntilClosedAfterEngineShutdown() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(10);

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open().addCredit(10);

        peer.waitForScriptToComplete();

        engine.shutdown();

        assertEquals(10, receiver.getCredit());
        receiver.close();
        assertEquals(0, receiver.getCredit());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverHonorsDeliverySetEventHandlers() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0).queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.SENDER.getValue())
                                .withState().accepted()
                                .withFirst(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicInteger additionalDeliveryCounter = new AtomicInteger();
        final AtomicInteger dispositionCounter = new AtomicInteger();

        final ArrayList<IncomingDelivery> deliveries = new ArrayList<>();

        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            delivery.deliveryReadHandler((target) -> {
                additionalDeliveryCounter.incrementAndGet();
            });
            delivery.deliveryStateUpdatedHandler((target) -> {
                dispositionCounter.incrementAndGet();
                deliveries.add(delivery);
            });
        });

        receiver.deliveryStateUpdatedHandler((delivery) -> {
            fail("Should not have updated this handler.");
        });

        receiver.open();
        receiver.addCredit(2);
        receiver.close();

        assertEquals(1, deliveryCounter.get(), "Should only be one initial delivery");
        assertEquals(1, additionalDeliveryCounter.get(), "Should be a second delivery update at the delivery handler");
        assertEquals(1, dispositionCounter.get(), "Not all deliveries received dispositions");

        byte deliveryTag = 0;

        for (IncomingDelivery delivery : deliveries) {
            assertEquals(deliveryTag++, delivery.getTag().tagBuffer().getByte(0), "Delivery not updated in correct order");
            assertTrue(delivery.isRemotelySettled(), "Delivery should be marked as remotely settled");
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverAbortedHandlerCalledWhenSet() throws Exception {
        doTestReceiverReadHandlerOrAbortHandlerCalled(true);
    }

    @Test
    public void testReceiverReadHandlerCalledForAbortWhenAbortedNotSet() throws Exception {
        doTestReceiverReadHandlerOrAbortHandlerCalled(false);
    }

    private void doTestReceiverReadHandlerOrAbortHandlerCalled(boolean setAbortHandler) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0).queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withAborted(true)
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicInteger deliveryAbortedInReadEventCounter = new AtomicInteger();
        final AtomicInteger deliveryAbortedCounter = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            if (delivery.isAborted()) {
                deliveryAbortedInReadEventCounter.incrementAndGet();
            } else {
                deliveryCounter.incrementAndGet();
            }
        });

        if (setAbortHandler) {
            receiver.deliveryAbortedHandler(delivery -> {
                deliveryAbortedCounter.incrementAndGet();
            });
        }

        receiver.deliveryStateUpdatedHandler((delivery) -> {
            fail("Should not have updated this handler.");
        });

        receiver.open();
        receiver.addCredit(2);
        receiver.close();

        assertEquals(1, deliveryCounter.get(), "Should only be one initial delivery");
        if (setAbortHandler) {
            assertEquals(0, deliveryAbortedInReadEventCounter.get(), "Should be no aborted delivery in read event");
            assertEquals(1, deliveryAbortedCounter.get(), "Should only be one aborted delivery events");
        } else {
            assertEquals(1, deliveryAbortedInReadEventCounter.get(), "Should only be no aborted delivery in read event");
            assertEquals(0, deliveryAbortedCounter.get(), "Should be no aborted delivery events");
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testIncomingDeliveryReadEventSignaledWhenNoAbortedHandlerSet() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0).queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withAborted(true)
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicInteger deliveryAbortedCounter = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            delivery.deliveryReadHandler((target) -> {
                if (target.isAborted()) {
                    deliveryAbortedCounter.incrementAndGet();
                }
            });
        });

        receiver.deliveryStateUpdatedHandler((delivery) -> {
            fail("Should not have updated this handler.");
        });

        receiver.open();
        receiver.addCredit(2);
        receiver.close();

        assertEquals(1, deliveryCounter.get(), "Should only be one initial delivery");
        assertEquals(1, deliveryAbortedCounter.get(), "Should only be one aborted delivery");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionWindowOpenedAfterDeliveryRead() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().withIncomingWindow(1).respond();
        peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withPayload(payload)
                             .withMessageFormat(0).queue();
        peer.expectFlow().withLinkCredit(1).withIncomingWindow(1);
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withPayload(payload)
                             .withMessageFormat(0).queue();
        peer.expectFlow().withLinkCredit(0).withIncomingWindow(1);
        peer.expectDetach().respond();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setIncomingCapacity(1024).open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            delivery.readAll();
        });

        receiver.open();
        receiver.addCredit(2);
        receiver.close();

        assertEquals(2, deliveryCounter.get(), "Should only be one initial delivery");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionWindowOpenedAfterDeliveryReadFromSplitFramedTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().withIncomingWindow(1).respond();
        peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withPayload(payload)
                             .withMessageFormat(0).queue();
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(true)
                             .withPayload(payload)
                             .withMessageFormat(0).queue();
        peer.expectFlow().withLinkCredit(3).withIncomingWindow(0);
        peer.expectFlow().withLinkCredit(3).withIncomingWindow(1);
        peer.expectDetach().respond();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setIncomingCapacity(1024).open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicReference<IncomingDelivery> delivery = new AtomicReference<>();

        receiver.deliveryReadHandler(incoming -> {
            if (deliveryCounter.getAndIncrement() == 0) {
                delivery.set(incoming);
                delivery.get().readAll();
            }
        });

        receiver.open();
        receiver.addCredit(2);

        assertEquals(2, deliveryCounter.get(), "Should only be one initial delivery");
        assertTrue(delivery.get().available() > 0);

        receiver.addCredit(1);

        delivery.get().readAll();

        receiver.close();

        assertEquals(2, deliveryCounter.get(), "Should only be one initial delivery");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testIncomingDeliveryTracksTransferInCount() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(1);
        peer.expectDetach().respond();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setIncomingCapacity(1024).open();
        Receiver receiver = session.receiver("test");

        final AtomicReference<IncomingDelivery> received = new AtomicReference<>();

        receiver.deliveryReadHandler(delivery -> {
            received.compareAndSet(null, delivery);
        });

        receiver.open();
        receiver.addCredit(2);

        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withPayload(payload).now();

        assertNotNull(received.get());
        assertEquals(1, received.get().getTransferCount());

        peer.remoteTransfer().withDeliveryId(0)
                             .withMore(false)
                             .withPayload(payload).now();

        assertNotNull(received.get());
        assertEquals(2, received.get().getTransferCount());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSettleDeliveryAfterEngineShutdown() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final byte[] payload = new byte[] { 1 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("receiver");
        receiver.addCredit(1);

        // Receiver 1 handlers for delivery processing.
        receiver.deliveryReadHandler(delivery -> {
            receivedDelivery.set(delivery);
        });

        receiver.open();

        peer.waitForScriptToComplete();

        engine.shutdown();

        try {
            receivedDelivery.get().settle();
            fail("Should not allow for settlement since engine was manually shut down");
        } catch (EngineShutdownException ese) {}

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReadAllDeliveryDataWhenSessionWindowInForceAndLinkIsClosed() throws Exception {
        testReadAllDeliveryDataWhenSessionWindowInForceButLinkCannotWrite(true, false, false, false);
    }

    @Test
    public void testReadAllDeliveryDataWhenSessionWindowInForceAndSessionIsClosed() throws Exception {
        testReadAllDeliveryDataWhenSessionWindowInForceButLinkCannotWrite(false, true, false, false);
    }

    @Test
    public void testReadAllDeliveryDataWhenSessionWindowInForceAndConnectionIsClosed() throws Exception {
        testReadAllDeliveryDataWhenSessionWindowInForceButLinkCannotWrite(false, false, true, false);
    }

    @Test
    public void testReadAllDeliveryDataWhenSessionWindowInForceAndEngineIsShutdown() throws Exception {
        testReadAllDeliveryDataWhenSessionWindowInForceButLinkCannotWrite(false, false, false, true);
    }

    private void testReadAllDeliveryDataWhenSessionWindowInForceButLinkCannotWrite(boolean closeLink, boolean closeSession, boolean closeConnection, boolean shutdown) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().withIncomingWindow(1).respond();
        peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withPayload(payload)
                             .withMessageFormat(0).queue();
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withPayload(payload)
                             .withMessageFormat(0).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setIncomingCapacity(1024).open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicReference<IncomingDelivery> delivery = new AtomicReference<>();

        receiver.deliveryReadHandler(incoming -> {
            if (deliveryCounter.getAndAdd(1) == 0) {
                delivery.set(incoming);
                incoming.readAll();
            }
        });

        receiver.open();
        receiver.addCredit(2);

        peer.waitForScriptToComplete();

        if (closeLink) {
            peer.expectDetach().withClosed(true).respond();
            receiver.close();
        }
        if (closeSession) {
            peer.expectEnd().respond();
            session.close();
        }
        if (closeConnection) {
            peer.expectClose().respond();
            connection.close();
        }
        if (shutdown) {
            engine.shutdown();
        }

        assertNotNull(delivery.get());
        assertEquals(2, deliveryCounter.get(), "Should only be one initial delivery");

        delivery.get().readAll();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testWalkUnsettledAfterReceivingTransfersThatCrossSignedIntDeliveryIdRange() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte[] payload = new byte[] { 1 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond().withNextOutgoingId(Integer.MAX_VALUE);
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(Integer.MAX_VALUE)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.remoteTransfer().withDeliveryId(Integer.MAX_VALUE + 1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("receiver");

        receiver.addCredit(2);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDisposition().withFirst(Integer.MAX_VALUE)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDisposition().withFirst(Integer.MAX_VALUE + 1)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(receiver.hasUnsettled());
        assertEquals(2, receiver.unsettled().size());
        receiver.disposition((delivery) -> true, Accepted.getInstance(), true);

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testUnsettledCollectionDispositionsAfterReceivingTransfersThatCrossSignedIntDeliveryIdRange() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte[] payload = new byte[] { 1 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond().withNextOutgoingId(Integer.MAX_VALUE);
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(Integer.MAX_VALUE)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.remoteTransfer().withDeliveryId(Integer.MAX_VALUE + 1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("receiver");

        receiver.addCredit(2);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDisposition().withFirst(Integer.MAX_VALUE)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDisposition().withFirst(Integer.MAX_VALUE + 1)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(receiver.hasUnsettled());
        assertEquals(2, receiver.unsettled().size());
        receiver.unsettled().forEach((delivery) -> {
            delivery.disposition(Accepted.getInstance(), true);
        });

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testWalkUnsettledAfterReceivingTransfersThatCrossUnsignedIntDeliveryIdRange() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte[] payload = new byte[] { 1 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond().withNextOutgoingId(UnsignedInteger.MAX_VALUE.intValue());
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(3);
        peer.remoteTransfer().withDeliveryId(UnsignedInteger.MAX_VALUE.intValue())
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("receiver");

        receiver.addCredit(3);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDisposition().withFirst(UnsignedInteger.MAX_VALUE.intValue())
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDisposition().withFirst(1)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(receiver.hasUnsettled());
        assertEquals(3, receiver.unsettled().size());
        receiver.disposition((delivery) -> true, Accepted.getInstance(), true);

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testUnsettledCollectionDispositionAfterReceivingTransfersThatCrossUnsignedIntDeliveryIdRange() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte[] payload = new byte[] { 1 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond().withNextOutgoingId(UnsignedInteger.MAX_VALUE.intValue());
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(3);
        peer.remoteTransfer().withDeliveryId(UnsignedInteger.MAX_VALUE.intValue())
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("receiver");

        receiver.addCredit(3);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDisposition().withFirst(UnsignedInteger.MAX_VALUE.intValue())
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDisposition().withFirst(1)
                                .withSettled(true)
                                .withState().accepted();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        assertTrue(receiver.hasUnsettled());
        assertEquals(3, receiver.unsettled().size());
        receiver.unsettled().forEach((delivery) -> {
            delivery.disposition(Accepted.getInstance(), true);
        });

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testIncomingWindowRefilledWithBytesPreviouslyReadOnAbortedTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        byte[] payload = new byte[256];
        Arrays.fill(payload, (byte) 127);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().withIncomingWindow(2).respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2).withNextIncomingId(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();

        Connection connection = engine.start().open();
        Session session = connection.session();
        session.setIncomingCapacity((int) (connection.getMaxFrameSize() * 2));
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicInteger deliveryAbortedCounter = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            if (delivery.isAborted()) {
                deliveryAbortedCounter.incrementAndGet();
            }
        });

        receiver.deliveryStateUpdatedHandler((delivery) -> {
            fail("Should not have updated this handler.");
        });

        receiver.open();
        receiver.addCredit(2);

        peer.waitForScriptToComplete();
        peer.expectFlow().withLinkCredit(1).withIncomingWindow(2).withNextIncomingId(3);
        peer.expectDetach().respond();

        assertEquals((connection.getMaxFrameSize() * 2) - payload.length, session.getRemainingIncomingCapacity());

        peer.remoteTransfer().withDeliveryId(0)
                             .withAborted(true)
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertEquals(connection.getMaxFrameSize() * 2, session.getRemainingIncomingCapacity());

        receiver.close();

        assertEquals(2, deliveryCounter.get(), "Should have received two delivery read events");
        assertEquals(1, deliveryAbortedCounter.get(), "Should only be one aborted delivery event");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiveDeliveriesAndSendDispositionUponReceipt() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final byte[] payload = new byte[] { 1 };

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond().withNextOutgoingId(0);
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(3);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState().accepted();
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.expectDisposition().withFirst(1)
                                .withSettled(true)
                                .withState().accepted();
        peer.remoteTransfer().withDeliveryId(2)
                             .withDeliveryTag(new byte[] {3})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).queue();
        peer.expectDisposition().withFirst(2)
                                .withSettled(true)
                                .withState().accepted();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("receiver");
        receiver.deliveryReadHandler((delivery) -> {
            delivery.disposition(Accepted.getInstance(), true);
        });

        receiver.addCredit(3);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }
}
