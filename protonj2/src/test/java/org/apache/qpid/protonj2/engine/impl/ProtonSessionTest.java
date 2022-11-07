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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.ConnectionState;
import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Link;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.test.driver.matchers.types.UnsignedIntegerMatcher;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SessionError;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test behaviors of the ProtonSession implementation.
 */
@Timeout(20)
public class ProtonSessionTest extends ProtonEngineTestSupport {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonSessionTest.class);

    @Test
    public void testSessionEmitsOpenAndCloseEvents() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean sessionLocalOpen = new AtomicBoolean();
        final AtomicBoolean sessionLocalClose = new AtomicBoolean();
        final AtomicBoolean sessionRemoteOpen = new AtomicBoolean();
        final AtomicBoolean sessionRemoteClose = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();

        session.localOpenHandler(result -> sessionLocalOpen.set(true))
               .localCloseHandler(result -> sessionLocalClose.set(true))
               .openHandler(result -> sessionRemoteOpen.set(true))
               .closeHandler(result -> sessionRemoteClose.set(true));

        session.open();

        assertEquals(connection, session.getParent());

        session.close();

        assertTrue(sessionLocalOpen.get(), "Session should have reported local open");
        assertTrue(sessionLocalClose.get(), "Session should have reported local close");
        assertTrue(sessionRemoteOpen.get(), "Session should have reported remote open");
        assertTrue(sessionRemoteClose.get(), "Session should have reported remote close");

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

        Connection connection = engine.start();

        connection.open();

        Session session = connection.session();
        session.open();
        session.engineShutdownHandler(result -> engineShutdown.set(true));

        if (locallyClosed) {
            if (remotelyClosed) {
                peer.expectEnd().respond();
            } else {
                peer.expectEnd();
            }

            session.close();
        }

        if (remotelyClosed && !locallyClosed) {
            peer.remoteEnd().now();
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
    public void testSessionOpenAndCloseAreIdempotent() throws Exception {
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

        // Should not emit another begin frame
        session.open();

        session.close();

        // Should not emit another end frame
        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderCreateOnClosedSessionThrowsISE() throws Exception {
        testLinkCreateOnClosedSessionThrowsISE(false);
    }

    @Test
    public void testReceiverCreateOnClosedSessionThrowsISE() throws Exception {
        testLinkCreateOnClosedSessionThrowsISE(true);
    }

    private void testLinkCreateOnClosedSessionThrowsISE(boolean receiver) throws Exception {
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
        Session session = connection.session().open().close();

        if (receiver) {
            try {
                session.receiver("test");
                fail("Should not allow receiver create on closed session");
            } catch (IllegalStateException ise) {
                // Expected
            }
        } else {
            try {
                session.sender("test");
                fail("Should not allow sender create on closed session");
            } catch (IllegalStateException ise) {
                // Expected
            }
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSessionBeforeOpenConnection() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // An opened session shouldn't write its begin until the parent connection
        // is opened and once it is the begin should be automatically written.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();

        connection.open();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testEngineEmitsBeginAfterLocalSessionOpened() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();

        Session session = connection.session();
        session.openHandler((result) -> {
            remoteOpened.set(true);
        });
        session.open();

        peer.waitForScriptToComplete();

        assertTrue(remoteOpened.get());
        assertNull(failure);
    }

    @Test
    public void testSessionFiresOpenedEventAfterRemoteOpensLocallyOpenedSession() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();

        final AtomicBoolean connectionRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        connection.openHandler((result) -> {
            connectionRemotelyOpened.set(true);
        });
        connection.open();

        assertTrue(connectionRemotelyOpened.get(), "Connection remote opened event did not fire");

        Session session = connection.session();
        session.openHandler(result -> {
            sessionRemotelyOpened.set(true);
        });
        session.open();

        assertTrue(sessionRemotelyOpened.get(), "Session remote opened event did not fire");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testNoSessionPerformativesEmittedIfConnectionOpenedAndClosedBeforeAnyRemoteResponses() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // An opened session shouldn't write its begin until the parent connection
        // is opened and once it is the begin should be automatically written.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();

        peer.expectAMQPHeader();

        connection.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        connection.close();

        peer.expectOpen().respond();
        peer.expectClose().respond();
        peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseSessionWithNullSetsOnSessionOptions() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().onChannel(0).respond();
        peer.expectEnd().onChannel(0).respond();
        peer.expectClose();

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.setProperties(null);
        session.setOfferedCapabilities((Symbol[]) null);
        session.setDesiredCapabilities((Symbol[]) null);
        session.setCondition(null);
        session.open();

        assertNotNull(session.getAttachments());
        assertNull(session.getProperties());
        assertNull(session.getOfferedCapabilities());
        assertNull(session.getDesiredCapabilities());
        assertNull(session.getCondition());

        assertNull(session.getRemoteProperties());
        assertNull(session.getRemoteOfferedCapabilities());
        assertNull(session.getRemoteDesiredCapabilities());
        assertNull(session.getRemoteCondition());

        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleSessions() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().onChannel(0).respond();
        peer.expectBegin().onChannel(1).respond();
        peer.expectEnd().onChannel(1).respond();
        peer.expectEnd().onChannel(0).respond();
        peer.expectClose();

        Connection connection = engine.start();
        connection.open();

        Session session1 = connection.session();
        session1.open();
        Session session2 = connection.session();
        session2.open();

        session2.close();
        session1.close();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineFireRemotelyOpenedSessionEventWhenRemoteBeginArrives() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.remoteBegin().queue();

        final AtomicBoolean connectionRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        final AtomicReference<Session> remoteSession = new AtomicReference<>();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openHandler((result) -> {
            connectionRemotelyOpened.set(true);
        });
        connection.sessionOpenHandler(result -> {
            remoteSession.set(result);
            sessionRemotelyOpened.set(true);
        });
        connection.open();

        assertTrue(connectionRemotelyOpened.get(), "Connection remote opened event did not fire");
        assertTrue(sessionRemotelyOpened.get(), "Session remote opened event did not fire");
        assertNotNull(remoteSession.get(), "Connection did not create a local session for remote open");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionPopulatesBeginUsingDefaults() throws IOException {
        doTestSessionOpenPopulatesBegin(false, false);
    }

    @Test
    public void testSessionPopulatesBeginWithConfiguredMaxFrameSizeButNoIncomingCapacity() throws IOException {
        doTestSessionOpenPopulatesBegin(true, false);
    }

    @Test
    public void testSessionPopulatesBeginWithConfiguredMaxFrameSizeAndIncomingCapacity() throws IOException {
        doTestSessionOpenPopulatesBegin(true, true);
    }

    private void doTestSessionOpenPopulatesBegin(boolean setMaxFrameSize, boolean setIncomingCapacity) {
        final int MAX_FRAME_SIZE = 32767;
        final int SESSION_INCOMING_CAPACITY = Integer.MAX_VALUE;

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final Matcher<?> expectedMaxFrameSize;

        if (setMaxFrameSize) {
            expectedMaxFrameSize = new UnsignedIntegerMatcher(MAX_FRAME_SIZE);
        } else {
            expectedMaxFrameSize = new UnsignedIntegerMatcher(ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE);
        }

        int expectedIncomingWindow = Integer.MAX_VALUE;
        if (setIncomingCapacity) {
            expectedIncomingWindow = SESSION_INCOMING_CAPACITY / MAX_FRAME_SIZE;
        }

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(expectedMaxFrameSize).respond().withContainerId("driver");
        peer.expectBegin().withHandleMax(nullValue())
                          .withNextOutgoingId(0)
                          .withIncomingWindow(expectedIncomingWindow)
                          .withOutgoingWindow(Integer.MAX_VALUE)
                          .withOfferedCapabilities(nullValue())
                          .withDesiredCapabilities(nullValue())
                          .withProperties(nullValue())
                          .respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();
        if (setMaxFrameSize) {
            connection.setMaxFrameSize(MAX_FRAME_SIZE);
        }
        connection.open();

        Session session = connection.session();
        if (setIncomingCapacity) {
            session.setIncomingCapacity(SESSION_INCOMING_CAPACITY);
        }
        session.open();
        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOpenFailsWhenConnectionClosed() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectClose().respond();

        final AtomicBoolean connectionOpenedSignaled = new AtomicBoolean();
        final AtomicBoolean connectionClosedSignaled = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openHandler(result -> {
            connectionOpenedSignaled.set(true);
        });
        connection.closeHandler(result -> {
            connectionClosedSignaled.set(true);
        });

        Session session = connection.session();
        connection.open();
        connection.close();

        assertTrue(connectionOpenedSignaled.get(), "Connection remote opened event did not fire");
        assertTrue(connectionClosedSignaled.get(), "Connection remote closed event did not fire");

        try {
            session.open();
            fail("Should not be able to open a session when its Connection was already closed");
        } catch (IllegalStateException ise) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOpenFailsWhenConnectionRemotelyClosed() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.remoteClose().queue();

        final AtomicBoolean connectionOpenedSignaled = new AtomicBoolean();
        final AtomicBoolean connectionClosedSignaled = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openHandler(result -> {
            connectionOpenedSignaled.set(true);
        });
        connection.closeHandler(result -> {
            connectionClosedSignaled.set(true);
        });

        Session session = connection.session();
        connection.open();

        assertTrue(connectionOpenedSignaled.get(), "Connection remote opened event did not fire");
        assertTrue(connectionClosedSignaled.get(), "Connection remote closed event did not fire");

        try {
            session.open();
            fail("Should not be able to open a session when its Connection was already closed");
        } catch (IllegalStateException ise) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOpenFailsWhenWriteOfBeginFailsWithException() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.dropAfterLastHandler();

        Connection connection = engine.start().open();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);
        assertTrue(connection.getState() == ConnectionState.ACTIVE);
        assertTrue(connection.getRemoteState() == ConnectionState.ACTIVE);

        Session session = connection.session();

        try {
            session.open();
            fail("Should not be able to open a session when its Connection was already closed");
        } catch (EngineFailedException failure) {
            LOG.trace("Got expected engine failure from session Begin write.", failure);
        }

        peer.waitForScriptToComplete();

        assertNotNull(failure);
        assertTrue(engine.isFailed());
        assertFalse(engine.isShutdown());
        assertNotNull(engine.failureCause());
    }

    @Test
    public void testSessionOpenNotSentUntilConnectionOpened() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        Session session = connection.session();
        session.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectClose();

        connection.open();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionCloseNotSentUntilConnectionOpened() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean sessionOpenedSignaled = new AtomicBoolean();

        Connection connection = engine.start();
        Session session = connection.session();
        session.openHandler(result -> sessionOpenedSignaled.set(true));
        session.open();
        session.close();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectEnd().respond();
        peer.expectClose();

        assertFalse(sessionOpenedSignaled.get(), "Session opened handler should not have been called yet");

        connection.open();

        // Session was already closed so no open event should fire.
        assertFalse(sessionOpenedSignaled.get(), "Session opened handler should not have been called yet");

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionRemotelyClosedWithError() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();

        Connection connection = engine.start().open();
        Session session = connection.session();
        session.open();

        peer.waitForScriptToComplete();

        assertTrue(session.isLocallyOpen());
        assertFalse(session.isLocallyClosed());
        assertTrue(session.isRemotelyOpen());
        assertFalse(session.isRemotelyClosed());

        peer.expectEnd();
        peer.expectClose();
        peer.remoteEnd().withErrorCondition(AmqpError.INTERNAL_ERROR.toString(), "Error").now();

        assertTrue(session.isLocallyOpen());
        assertFalse(session.isLocallyClosed());
        assertFalse(session.isRemotelyOpen());
        assertTrue(session.isRemotelyClosed());

        assertEquals(AmqpError.INTERNAL_ERROR, session.getRemoteCondition().getCondition());
        assertEquals("Error", session.getRemoteCondition().getDescription());

        session.close();

        assertFalse(session.isLocallyOpen());
        assertTrue(session.isLocallyClosed());
        assertFalse(session.isRemotelyOpen());
        assertTrue(session.isRemotelyClosed());

        assertEquals(AmqpError.INTERNAL_ERROR, session.getRemoteCondition().getCondition());
        assertEquals("Error", session.getRemoteCondition().getDescription());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionCloseAfterConnectionRemotelyClosedWhenNoBeginResponseReceived() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        Session session = connection.session();
        session.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin();
        peer.remoteClose().withErrorCondition(AmqpError.NOT_ALLOWED.toString(), "Error").queue();

        connection.open();

        peer.waitForScriptToComplete();
        peer.expectEnd();
        peer.expectClose();

        // Connection not locally closed so end still written.
        session.close();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testHandleRemoteBeginWithInvalidRemoteChannelSet() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");

        final AtomicBoolean remoteOpened = new AtomicBoolean();
        final AtomicBoolean remoteSession = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openHandler((result) -> {
            remoteOpened.set(true);
        });
        connection.open();

        connection.sessionOpenHandler(session -> {
            remoteSession.set(true);
        });

        peer.waitForScriptToComplete();

        // Simulate asynchronous arrival of data as we always operate on one thread in these tests.
        peer.expectClose().withError(not(nullValue()));
        peer.remoteBegin().withRemoteChannel(3).now();

        peer.waitForScriptToComplete();

        assertTrue(remoteOpened.get(), "Remote connection should have occurred");
        assertFalse(remoteSession.get(), "Should not have seen a remote session open.");

        assertNotNull(failure);
    }

    @Test
    public void testCapabilitiesArePopulatedAndAccessible() throws Exception {
        final Symbol clientOfferedSymbol = Symbol.valueOf("clientOfferedCapability");
        final Symbol clientDesiredSymbol = Symbol.valueOf("clientDesiredCapability");
        final Symbol serverOfferedSymbol = Symbol.valueOf("serverOfferedCapability");
        final Symbol serverDesiredSymbol = Symbol.valueOf("serverDesiredCapability");

        Symbol[] clientOfferedCapabilities = new Symbol[] { clientOfferedSymbol };
        Symbol[] clientDesiredCapabilities = new Symbol[] { clientDesiredSymbol };

        Symbol[] serverOfferedCapabilities = new Symbol[] { serverOfferedSymbol };
        Symbol[] serverDesiredCapabilities = new Symbol[] { serverDesiredSymbol };

        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().withOfferedCapabilities(new String[] { clientOfferedSymbol.toString() })
                          .withDesiredCapabilities(new String[] { clientDesiredSymbol.toString() })
                          .respond()
                          .withDesiredCapabilities(new String[] { serverDesiredSymbol.toString() })
                          .withOfferedCapabilities(new String[] { serverOfferedSymbol.toString() });
        peer.expectEnd().respond();

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();

        session.setDesiredCapabilities(clientDesiredCapabilities);
        session.setOfferedCapabilities(clientOfferedCapabilities);
        session.openHandler(result -> {
            sessionRemotelyOpened.set(true);
        });
        session.open();

        assertTrue(sessionRemotelyOpened.get(), "Session remote opened event did not fire");

        assertArrayEquals(clientOfferedCapabilities, session.getOfferedCapabilities());
        assertArrayEquals(clientDesiredCapabilities, session.getDesiredCapabilities());
        assertArrayEquals(serverOfferedCapabilities, session.getRemoteOfferedCapabilities());
        assertArrayEquals(serverDesiredCapabilities, session.getRemoteDesiredCapabilities());

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testPropertiesArePopulatedAndAccessible() throws Exception {
        final Symbol clientPropertyName = Symbol.valueOf("ClientPropertyName");
        final Integer clientPropertyValue = 1234;
        final Symbol serverPropertyName = Symbol.valueOf("ServerPropertyName");
        final Integer serverPropertyValue = 5678;

        Map<String, Object> expectedClientProperties = new HashMap<>();
        expectedClientProperties.put(clientPropertyName.toString(), clientPropertyValue);
        Map<Symbol, Object> clientProperties = new HashMap<>();
        clientProperties.put(clientPropertyName, clientPropertyValue);

        Map<String, Object> expectedServerProperties = new HashMap<>();
        expectedServerProperties.put(serverPropertyName.toString(), serverPropertyValue);
        Map<Symbol, Object> serverProperties = new HashMap<>();
        serverProperties.put(serverPropertyName, serverPropertyValue);

        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().withProperties(expectedClientProperties)
                          .respond()
                          .withProperties(expectedServerProperties);
        peer.expectEnd().respond();

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();

        session.setProperties(clientProperties);
        session.openHandler(result -> {
            sessionRemotelyOpened.set(true);
        });
        session.open();

        assertTrue(sessionRemotelyOpened.get(), "Session remote opened event did not fire");

        assertNotNull(session.getProperties());
        assertNotNull(session.getRemoteProperties());

        assertEquals(clientPropertyValue, session.getProperties().get(clientPropertyName));
        assertEquals(serverPropertyValue, session.getRemoteProperties().get(serverPropertyName));

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEmittedSessionIncomingWindowOnFirstFlowNoFrameSizeOrSessionCapacitySet() {
        doSessionIncomingWindowTestImpl(false, false);
    }

    @Test
    public void testEmittedSessionIncomingWindowOnFirstFlowWithFrameSizeButNoSessionCapacitySet() {
        doSessionIncomingWindowTestImpl(true, false);
    }

    @Test
    public void testEmittedSessionIncomingWindowOnFirstFlowWithNoFrameSizeButWithSessionCapacitySet() {
        doSessionIncomingWindowTestImpl(false, true);
    }

    @Test
    public void testEmittedSessionIncomingWindowOnFirstFlowWithFrameSizeAndSessionCapacitySet() {
        doSessionIncomingWindowTestImpl(true, true);
    }

    private void doSessionIncomingWindowTestImpl(boolean setFrameSize, boolean setSessionCapacity) {
        final int TEST_MAX_FRAME_SIZE = 5 * 1024;
        final int TEST_SESSION_CAPACITY = 100 * 1024;

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final Matcher<?> expectedMaxFrameSize;
        if (setFrameSize) {
            expectedMaxFrameSize = new UnsignedIntegerMatcher(TEST_MAX_FRAME_SIZE);
        } else {
            expectedMaxFrameSize = new UnsignedIntegerMatcher(ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE);
        }

        long expectedWindowSize = 2147483647;
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = TEST_SESSION_CAPACITY / TEST_MAX_FRAME_SIZE;
        } else if (setSessionCapacity) {
            expectedWindowSize = TEST_SESSION_CAPACITY / engine.connection().getMaxFrameSize();
        }

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(expectedMaxFrameSize).respond();
        peer.expectBegin().withIncomingWindow(expectedWindowSize).respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        if (setFrameSize) {
            connection.setMaxFrameSize(TEST_MAX_FRAME_SIZE);
        }
        connection.open();

        Session session = connection.session();
        int sessionCapacity = 0;
        if (setSessionCapacity) {
            sessionCapacity = TEST_SESSION_CAPACITY;
            session.setIncomingCapacity(sessionCapacity);
        }

        // Open session and verify emitted incoming window
        session.open();

        if (setSessionCapacity) {
            assertEquals(sessionCapacity, session.getRemainingIncomingCapacity());
        } else {
            assertEquals(Integer.MAX_VALUE, session.getRemainingIncomingCapacity());
        }

        assertEquals(sessionCapacity, session.getIncomingCapacity(), "Unexpected session capacity");

        // Use a receiver to force more session window observations.
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        final AtomicInteger deliveryArrived = new AtomicInteger();
        final AtomicReference<IncomingDelivery> delivered = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.incrementAndGet();
            delivered.set(delivery);
        });

        // Expect that a flow will be emitted and the window should match either default window
        // size or computed value if max frame size and capacity are set
        peer.expectFlow().withLinkCredit(1)
                           .withIncomingWindow(expectedWindowSize);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withBody().withString("test-message").also().queue();

        receiver.addCredit(1);

        assertEquals(1, deliveryArrived.get(), "Unexpected delivery count");
        assertNotNull(delivered.get());

        // Flow more credit after receiving a message but not consuming it should result in a decrease in
        // the incoming window if the capacity and max frame size are configured.
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize - 1;
            assertTrue(TEST_SESSION_CAPACITY > session.getRemainingIncomingCapacity());
        }

        peer.expectFlow().withLinkCredit(1)
                         .withIncomingWindow(expectedWindowSize);

        receiver.addCredit(1);

        // Settle the transfer then flow more credit, verify the emitted incoming window
        // (it should increase 1 if capacity and frame size set) otherwise remains unchanged.
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize + 1;
        }
        peer.expectFlow().withLinkCredit(2).withIncomingWindow(expectedWindowSize);

        // This will consume the bytes and free them from the session window tracking.
        assertNotNull(delivered.get().readAll());

        receiver.addCredit(1);

        peer.expectDetach().respond();
        peer.expectEnd().respond();

        receiver.close();
        session.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSessionHandlesDeferredOpenAndBeginResponses() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicInteger sessionOpened = new AtomicInteger();
        final AtomicInteger sessionClosed = new AtomicInteger();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.openHandler(result -> sessionOpened.incrementAndGet());
        session.closeHandler(result -> sessionClosed.incrementAndGet());
        session.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        // This should happen after we inject the held open and attach
        peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        // Inject held responses to get the ball rolling again
        peer.remoteOpen().withOfferedCapabilities("ANONYMOUS_RELAY").now();
        peer.respondToLastBegin().now();

        Sender sender = session.sender("sender-1");

        sender.open();

        session.close();

        assertEquals(1, sessionOpened.get(), "Should get one opened event");
        assertEquals(1, sessionClosed.get(), "Should get one closed event");

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponseBeginWrittenAndResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, true);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponseBeginWrittenAndNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, false, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenNotWritten() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(false, false, false);
    }

    private void testCloseAfterShutdownNoOutputAndNoException(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                if (respondToBegin) {
                    peer.expectBegin().respond();
                } else {
                    peer.expectBegin();
                }
            } else {
                peer.expectOpen();
                peer.expectBegin();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.open();

        engine.shutdown();

        // Should clean up and not throw as we knowingly shutdown engine operations.
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponseBeginWrittenAndResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponseBeginWrittenAndNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, false, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenNotWritten() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(false, false, false);
    }

    private void testCloseAfterEngineFailedThrowsAndNoOutputWritten(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean engineFailedEvent = new AtomicBoolean();

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                if (respondToBegin) {
                    peer.expectBegin().respond();
                } else {
                    peer.expectBegin();
                }
                peer.expectClose();
            } else {
                peer.expectOpen();
                peer.expectBegin();
                peer.expectClose();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.engineShutdownHandler(event -> engineFailedEvent.lazySet(true));
        session.open();

        engine.engineFailed(new IOException());

        try {
            session.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        try {
            connection.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        assertFalse(engineFailedEvent.get(), "Session should not have signalled engine failure");

        engine.shutdown();  // Explicit shutdown now allows local close to complete

        assertTrue(engineFailedEvent.get(), "Session should have signalled engine failure");

        // Should clean up and not throw as we knowingly shutdown engine operations.
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
    }

    @Test
    public void testSessionStopTrackingClosedSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, false, true);
    }

    @Test
    public void testSessionStopTrackingDetachedSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, false, false);
    }

    @Test
    public void testSessionStopTrackingClosedSendersRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, true, true);
    }

    @Test
    public void testSessionStopTrackingDetachedSendersRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, true, false);
    }

    @Test
    public void testSessionTracksRemotelyOpenSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, false, false, false);
    }

    @Test
    public void testSessionTracksLocallyOpenSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, false, true, false, false);
    }

    @Test
    public void testSessionStopTrackingClosedReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, false, true);
    }

    @Test
    public void testSessionStopTrackingDetachedReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, false, false);
    }

    @Test
    public void testSessionStopTrackingClosedReceiversRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, true, true);
    }

    @Test
    public void testSessionStopTrackingDetachedReceiversRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, true, false);
    }

    @Test
    public void testSessionTracksRemotelyOpenReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, false, false, false);
    }

    @Test
    public void testSessionTracksLocallyOpenReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, false, true, false, false);
    }

    private void doTestSessionTrackingOfLinks(Role role, boolean localDetach, boolean remoteDetach, boolean remoteGoesFirst, boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();

        session.open();

        assertTrue(session.senders().isEmpty());

        peer.expectAttach().withRole(role.getValue()).respond();

        final Link<?> link;

        if (role == Role.RECEIVER) {
            link = session.receiver("test");
        } else {
            link = session.sender("test");
        }

        link.open();

        if (role == Role.RECEIVER) {
            assertFalse(session.receivers().isEmpty());
            assertEquals(1, session.receivers().size());
        } else {
            assertFalse(session.senders().isEmpty());
            assertEquals(1, session.senders().size());
        }
        assertFalse(session.links().isEmpty());
        assertEquals(1, session.links().size());

        if (remoteDetach && remoteGoesFirst) {
            peer.remoteDetach().withClosed(close).now();
        }

        if (localDetach) {
            peer.expectDetach().withClosed(close);
            if (close) {
                link.close();
            } else {
                link.detach();
            }
        }

        if (remoteDetach && !remoteGoesFirst) {
            peer.remoteDetach().withClosed(close).now();
        }

        if (remoteDetach && localDetach) {
            assertTrue(session.receivers().isEmpty());
            assertTrue(session.senders().isEmpty());
            assertTrue(session.links().isEmpty());
        } else {
            if (role == Role.RECEIVER) {
                assertFalse(session.receivers().isEmpty());
                assertEquals(1, session.receivers().size());
            } else {
                assertFalse(session.senders().isEmpty());
                assertEquals(1, session.senders().size());
            }
            assertFalse(session.links().isEmpty());
            assertEquals(1, session.links().size());
        }

        peer.expectEnd().respond();
        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testGetSenderFromSessionByName() throws Exception {
        doTestSessionLinkByName(Role.SENDER);
    }

    @Test
    public void testGetReceiverFromSessionByName() throws Exception {
        doTestSessionLinkByName(Role.RECEIVER);
    }

    private void doTestSessionLinkByName(Role role) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(role.getValue()).respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();

        session.open();

        assertTrue(session.senders().isEmpty());

        final Link<?> link;

        if (role == Role.RECEIVER) {
            link = session.receiver("test");
        } else {
            link = session.sender("test");
        }

        link.open();

        final Link<?> lookup;
        if (role == Role.RECEIVER) {
            lookup = session.receiver("test");
        } else {
            lookup = session.sender("test");
        }

        assertSame(link, lookup);

        link.close();

        final Link<?> newLink;
        if (role == Role.RECEIVER) {
            newLink = session.receiver("test");
        } else {
            newLink = session.sender("test");
        }

        assertNotSame(link, newLink);

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseOrDetachWithErrorCondition() throws Exception {
        final String condition = "amqp:session:window-violation";
        final String description = "something bad happened.";

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectEnd().withError(condition, description).respond();
        peer.expectClose();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session().open();

        session.setCondition(new ErrorCondition(Symbol.valueOf(condition), description));
        session.close();

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSessionNotifiedOfRemoteSenderOpened() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();

        session.senderOpenHandler(result -> senderRemotelyOpened.set(true));
        session.open();

        peer.remoteAttach().ofReceiver().withHandle(1)
                                        .withInitialDeliveryCount(1)
                                        .withName("remote-sender").now();

        session.close();

        assertTrue(senderRemotelyOpened.get(), "Session should have reported remote sender open");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderAndReceiverWithSameLinkNames() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().ofSender().withHandle(0).withName("link-name");
        peer.expectAttach().ofReceiver().withHandle(1).withName("link-name");
        peer.expectEnd().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("link-name").open();
        Receiver receiver = session.receiver("link-name").open();

        sender.openHandler(link -> senderRemotelyOpened.set(true));
        receiver.openHandler(link -> receiverRemotelyOpened.set(true));

        peer.remoteAttach().ofSender().withHandle(1)
                                      .withInitialDeliveryCount(1)
                                      .withName("link-name").now();
        peer.remoteAttach().ofReceiver().withHandle(0)
                                        .withInitialDeliveryCount(1)
                                        .withName("link-name").now();

        assertTrue(sender.isLocallyOpen());
        assertTrue(sender.isRemotelyOpen());
        assertTrue(receiver.isLocallyOpen());
        assertTrue(receiver.isRemotelyOpen());

        session.close();

        assertTrue(senderRemotelyOpened.get(), "Sender should have reported remote sender open");
        assertTrue(receiverRemotelyOpened.get(), "Receiver should have reported remote sender open");

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testBeginAndEndSessionBeforeRemoteBeginArrives() throws Exception {
        doTestBeginAndEndSessionBeforeRemoteBeginArrives(false);
    }

    @Test
    public void testBeginAndEndSessionBeforeRemoteBeginArrivesForceGC() throws Exception {
        doTestBeginAndEndSessionBeforeRemoteBeginArrives(true);
    }

    private void doTestBeginAndEndSessionBeforeRemoteBeginArrives(boolean trgForceGC) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin();
        peer.expectEnd();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();

        session.open();
        session.close();

        // Make an "effort" to test the what happens after GC removes references to old sessions
        // this likely won't work but we at least tried.
        if (trgForceGC) {
            System.gc();
        }

        peer.waitForScriptToComplete();
        peer.remoteBegin().withRemoteChannel(0).withNextOutgoingId(1).now();
        peer.remoteEnd().now();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testHalfClosedSessionChannelNotImmediatelyRecycled() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().onChannel(0);
        peer.expectEnd();

        Connection connection = engine.start();

        connection.open();
        connection.session().open().close();

        // Channel 0 should be skipped since we are still waiting for the being / end and
        // we have a free slot that can be used instead.
        peer.waitForScriptToComplete();
        peer.expectBegin().onChannel(1).respond();
        peer.expectEnd().onChannel(1).respond();

        connection.session().open().close();

        // Now channel 1 should reused since it was opened and closed properly
        peer.waitForScriptToComplete();
        peer.expectBegin().onChannel(1).respond();
        peer.expectBegin().onChannel(0).respond();
        peer.expectEnd().onChannel(0).respond();

        connection.session().open();

        // Close the original session now and its slot should be free to be reused.
        peer.remoteBegin().withRemoteChannel(0).withNextOutgoingId(1).now();
        peer.remoteEnd().now();

        connection.session().open().close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testHalfClosedSessionChannelRecycledIfNoOtherAvailableChannels() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withChannelMax(1).respond().withContainerId("driver");
        peer.expectBegin().onChannel(0);
        peer.expectEnd().onChannel(0);
        peer.expectBegin().onChannel(1);
        peer.expectBegin().onChannel(0);

        Connection connection = engine.start();

        connection.setChannelMax(1); // at most two channels
        connection.open();
        connection.session().open().close(); // Ch: 0
        connection.session().open(); // Ch: 1
        connection.session().open(); // Ch: 0 (recycled)

        peer.waitForScriptToComplete();
        // Answer to initial Begin / End of session on Ch: 0
        peer.remoteBegin().withRemoteChannel(0).onChannel(1).now();
        peer.remoteEnd().onChannel(1).now();
        // Answer to second session which should have begun on Ch: 1
        peer.remoteBegin().withRemoteChannel(1).onChannel(0).now();
        // Answer to third session which should have begun on Ch: 0 recycled
        peer.remoteBegin().withRemoteChannel(0).onChannel(1).now();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSessionEnforcesHandleMaxForLocalSenders() throws Exception {
        doTestSessionEnforcesHandleMaxForLocalEndpoints(false);
    }

    @Test
    public void testSessionEnforcesHandleMaxForLocalReceivers() throws Exception {
        doTestSessionEnforcesHandleMaxForLocalEndpoints(true);
    }

    private void doTestSessionEnforcesHandleMaxForLocalEndpoints(boolean receiver) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().withHandleMax(0).respond();
        peer.expectAttach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().setHandleMax(0).open();

        assertEquals(0, session.getHandleMax());

        if (receiver) {
            session.receiver("receiver1").open();
            try {
                session.receiver("receiver2").open();
                fail("Should not allow receiver create on session with one handle maximum");
            } catch (IllegalStateException ise) {
                // Expected
            }
            try {
                session.sender("sender1").open();
                fail("Should not allow additional sender create on session with one handle maximum");
            } catch (IllegalStateException ise) {
                // Expected
            }
        } else {
            session.sender("sender1").open();
            try {
                session.sender("sender2").open();
                fail("Should not allow second sender create on session with one handle maximum");
            } catch (IllegalStateException ise) {
                // Expected
            }
            try {
                session.receiver("receiver1").open();
                fail("Should not allow additional receiver create on session with one handle maximum");
            } catch (IllegalStateException ise) {
                // Expected
            }
        }

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionEnforcesHandleMaxFromRemoteAttachOfSender() throws Exception {
        doTestSessionEnforcesHandleMaxFromRemoteAttach(true);
    }

    @Test
    public void testSessionEnforcesHandleMaxFromRemoteAttachOfReceiver() throws Exception {
        doTestSessionEnforcesHandleMaxFromRemoteAttach(false);
    }

    public void doTestSessionEnforcesHandleMaxFromRemoteAttach(boolean sender) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().withHandleMax(0).respond().withHandleMax(42);
        if (sender) {
            peer.remoteAttach().ofSender().withHandle(1).withName("link-name").queue();
        } else {
            peer.remoteAttach().ofReceiver().withHandle(1).withName("link-name").queue();
        }
        peer.expectClose().withError(ConnectionError.FRAMING_ERROR.toString(), "Session handle-max exceeded").respond();

        Connection connection = engine.start().open();

        // Remote should attempt to attach a link and violate local handle max restrictions
        connection.session().setHandleMax(0).open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOutgoingSetEqualToMaxFrameSize() throws Exception {
        testSessionConfigureOutgoingCapacity(1024, 1024, 1024);
    }

    @Test
    public void testSessionOutgoingSetToTwiceMaxFrameSize() throws Exception {
        testSessionConfigureOutgoingCapacity(1024, 2048, 2048);
    }

    @Test
    public void testSessionOutgoingSetToSmallerThanMaxFrameSize() throws Exception {
        testSessionConfigureOutgoingCapacity(1024, 512, 1024);
    }

    @Test
    public void testSessionOutgoingSetToLargerThanMaxFrameSizeAndNotEven() throws Exception {
        testSessionConfigureOutgoingCapacity(1024, 8199, 8192);
    }

    @Test
    public void testSessionOutgoingSetToZeroToDisableOutput() throws Exception {
        testSessionConfigureOutgoingCapacity(1024, 0, 0);
    }

    private void testSessionConfigureOutgoingCapacity(int frameSize, int sessionCapacity, int remainingCapacity) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(frameSize).respond();
        peer.expectBegin().respond();

        Connection connection = engine.start().setMaxFrameSize(frameSize).open();
        Session session = connection.session().open();

        peer.waitForScriptToComplete();

        assertEquals(Integer.MAX_VALUE, session.getRemainingOutgoingCapacity());

        session.setOutgoingCapacity(sessionCapacity);

        assertEquals(sessionCapacity, session.getOutgoingCapacity());
        assertEquals(remainingCapacity, session.getRemainingOutgoingCapacity());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSessionNotWritableWhenOutgoingCapacitySetToZeroAlsoReflectsInSenders() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(2).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test").open();

        peer.waitForScriptToComplete();

        assertTrue(sender.isSendable());
        assertEquals(Integer.MAX_VALUE, session.getRemainingOutgoingCapacity());

        session.setOutgoingCapacity(0);

        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSenderCannotSendAfterUsingUpOutgoingCapacityLimit() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(2048).open();
        Sender sender = session.sender("test").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);
        peer.expectTransfer().withPayload(payload);

        assertTrue(sender.isSendable());
        assertEquals(2048, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(2, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSenderGetsUpdatedOnceSessionOutgoingWindowIsExpandedByWriteCallbacks() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(3072).open();
        Sender sender = session.sender("test").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);
        peer.expectTransfer().withPayload(payload);
        peer.expectTransfer().withPayload(payload);

        final AtomicInteger creditStateUpdated = new AtomicInteger();
        sender.creditStateUpdateHandler((self) -> {
            creditStateUpdated.incrementAndGet();
        });

        assertTrue(sender.isSendable());
        assertEquals(3072, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        OutgoingDelivery delivery3 = sender.next();
        delivery3.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(3, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        assertEquals(1, creditStateUpdated.get());
        assertTrue(sender.isSendable());
        assertEquals(3072, session.getRemainingOutgoingCapacity());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSetSameOutgoingWindowAfterBecomingNotWritableDoesNotTriggerWritable() throws Exception {
        // Should not become writable because two outstanding writes but low water mark remains one frame pending.
        testSessionOutgoingWindowExpandedAfterItBecomeNotWritable(2048, false);
    }

    @Test
    public void testExpandingOutgoingWindowAfterBecomingNotWritableUpdateSenderAsWritableOneFrameBigger() throws Exception {
        // Should not become writable because two outstanding writes but low water mark remains one frame pending.
        testSessionOutgoingWindowExpandedAfterItBecomeNotWritable(3072, false);
    }

    @Test
    public void testExpandingOutgoingWindowAfterBecomingNotWritableUpdateSenderAsWritableTwoFramesBuffer() throws Exception {
        // Should become writable since low water mark was one but becomes two and we have only two pending.
        testSessionOutgoingWindowExpandedAfterItBecomeNotWritable(4096, true);
    }

    @Test
    public void testDisableOutgoingWindowingAfterBecomingNotWritableUpdateSenderAsWritable() throws Exception {
        // Should become pending since we are lifting restrictions
        testSessionOutgoingWindowExpandedAfterItBecomeNotWritable(-1, true);
    }

    private void testSessionOutgoingWindowExpandedAfterItBecomeNotWritable(int updatedWindow, boolean becomesWritable) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final int maxFrameSize = 1024;
        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(maxFrameSize).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(maxFrameSize).open();
        Session session = connection.session().setOutgoingCapacity(2048).open();
        Sender sender = session.sender("test").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);
        peer.expectTransfer().withPayload(payload);

        final AtomicInteger creditStateUpdated = new AtomicInteger();
        sender.creditStateUpdateHandler((self) -> {
            creditStateUpdated.incrementAndGet();
        });

        assertTrue(sender.isSendable());
        assertEquals(2048, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(2, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        session.setOutgoingCapacity(updatedWindow);

        if (becomesWritable) {
            assertEquals(1, creditStateUpdated.get());
            assertTrue(sender.isSendable());
        } else {
            assertEquals(0, creditStateUpdated.get());
            assertFalse(sender.isSendable());
        }

        if (updatedWindow == -1) {
            assertEquals(Integer.MAX_VALUE, session.getRemainingOutgoingCapacity());
        } else {
            assertEquals(updatedWindow - (asyncIOCallbacks.size() * maxFrameSize), session.getRemainingOutgoingCapacity());
        }

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testMultiplySendersCannotSendAfterUsingUpOutgoingCapacityLimit() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(2048).open();
        Sender sender1 = session.sender("test1").setDeliveryTagGenerator(generator).open();
        Sender sender2 = session.sender("test2").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);
        peer.expectTransfer().withPayload(payload);

        assertTrue(sender1.isSendable());
        assertTrue(sender2.isSendable());
        assertEquals(2048, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach, Attach
        assertEquals(4, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender1.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        OutgoingDelivery delivery2 = sender2.next();
        delivery2.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(2, asyncIOCallbacks.size());
        assertFalse(sender1.isSendable());
        assertFalse(sender2.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testOnlyOneSenderNotifiedOfNewCapacityIfFirstOneUsesItUp() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(2048).open();
        Sender sender1 = session.sender("test1").setDeliveryTagGenerator(generator).open();
        Sender sender2 = session.sender("test2").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);
        peer.expectTransfer().withPayload(payload);

        // One of them should write to the high water mark again and stop the other getting called.
        final AtomicInteger creditStateUpdated = new AtomicInteger();
        sender1.creditStateUpdateHandler((self) -> {
            creditStateUpdated.incrementAndGet();
            OutgoingDelivery delivery = self.next();
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        });
        sender2.creditStateUpdateHandler((self) -> {
            creditStateUpdated.incrementAndGet();
            OutgoingDelivery delivery = self.next();
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        });

        assertTrue(sender1.isSendable());
        assertTrue(sender2.isSendable());
        assertEquals(2048, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach, Attach
        assertEquals(4, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender1.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        OutgoingDelivery delivery2 = sender2.next();
        delivery2.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        assertEquals(2, asyncIOCallbacks.size());
        assertFalse(sender1.isSendable());
        assertFalse(sender2.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        // Free a frame's worth of window which should allow a new write from one sender.
        asyncIOCallbacks.poll().run();

        assertEquals(2, asyncIOCallbacks.size());
        assertFalse(sender1.isSendable());
        assertFalse(sender2.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertEquals(1, creditStateUpdated.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReduceOutgoingWindowDoesNotStopSenderIfSomeWindowRemaining() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(4096).open();
        Sender sender = session.sender("test1").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        assertTrue(sender.isSendable());
        assertEquals(4096, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(1, asyncIOCallbacks.size());
        assertTrue(sender.isSendable());
        assertEquals(3072, session.getRemainingOutgoingCapacity());

        session.setOutgoingCapacity(2048);
        assertEquals(1024, session.getRemainingOutgoingCapacity());
        assertTrue(sender.isSendable());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testDisableOutgoingWindowMarksSenderAsNotSendableWhenWriteStillPending() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(4096).open();
        Sender sender = session.sender("test1").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        assertTrue(sender.isSendable());
        assertEquals(4096, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(1, asyncIOCallbacks.size());
        assertTrue(sender.isSendable());
        assertEquals(3072, session.getRemainingOutgoingCapacity());

        session.setOutgoingCapacity(0);
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertFalse(sender.isSendable());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReduceAndThenIncreaseOutgoingWindowRemembersPreviouslyPendingWrites() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(4096).open();
        Sender sender = session.sender("test1").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        assertTrue(sender.isSendable());
        assertEquals(4096, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(1, asyncIOCallbacks.size());
        assertTrue(sender.isSendable());
        assertEquals(3072, session.getRemainingOutgoingCapacity());

        session.setOutgoingCapacity(1024);
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertFalse(sender.isSendable());
        session.setOutgoingCapacity(4096);
        assertEquals(3072, session.getRemainingOutgoingCapacity());
        assertTrue(sender.isSendable());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSenderNotifiedAfterSessionRemoteWindowOpenedAfterLocalCapacityRestored() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().withNextOutgoingId(0).respond().withNextOutgoingId(0);
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).withNextIncomingId(0).withIncomingWindow(1).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(1024).open();
        Sender sender = session.sender("test1").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        // One of them should write to the high water mark again and stop the other getting called.
        final AtomicInteger creditStateUpdated = new AtomicInteger();
        sender.creditStateUpdateHandler((self) -> {
            creditStateUpdated.incrementAndGet();
            OutgoingDelivery delivery = self.next();
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        });

        assertTrue(sender.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery = sender.next();
        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(1, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        // Free a frame's worth of window which shouldn't signal writable as still no remote capacity.
        asyncIOCallbacks.poll().run();

        assertEquals(0, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());
        assertEquals(0, creditStateUpdated.get());

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);
        peer.remoteFlow().withLinkCredit(19).withNextIncomingId(1).withIncomingWindow(1).now();

        assertEquals(1, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertEquals(1, creditStateUpdated.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSenderNotifiedAfterSessionRemoteWindowOpenedBeforeLocalCapacityRestored() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().withNextOutgoingId(0).respond().withNextOutgoingId(0);
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).withNextIncomingId(0).withIncomingWindow(1).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(1024).open();
        Sender sender = session.sender("test1").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        // One of them should write to the high water mark again and stop the other getting called.
        final AtomicInteger creditStateUpdated = new AtomicInteger();
        sender.creditStateUpdateHandler((self) -> {
            creditStateUpdated.incrementAndGet();
            if (sender.isSendable()) {
                OutgoingDelivery delivery = self.next();
                delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
            }
        });

        assertTrue(sender.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach
        assertEquals(3, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery = sender.next();
        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(1, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        // Restore session remote incoming capacity but the sender should not send since
        // there should still be pending I/O work to be signaled.
        peer.remoteFlow().withLinkCredit(19).withNextIncomingId(1).withIncomingWindow(1).now();

        assertEquals(1, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertEquals(1, creditStateUpdated.get());  // For now all flow events create a signal.

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        // Now local outgoing capacity should be opened up.
        asyncIOCallbacks.poll().run();

        assertEquals(1, asyncIOCallbacks.size());
        assertFalse(sender.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertEquals(2, creditStateUpdated.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testBothSendersNotifiedAfterSessionOutgoingWindowOpenedWhenBothRequestedSendableState() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().withNextOutgoingId(0).respond().withNextOutgoingId(0);
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).withNextIncomingId(0).withIncomingWindow(8192).queue();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).withNextIncomingId(0).withIncomingWindow(8192).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(1024).open();
        Sender sender1 = session.sender("test1").setDeliveryTagGenerator(generator).open();
        Sender sender2 = session.sender("test2").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        final AtomicInteger sender1CreditStateUpdated = new AtomicInteger();
        sender1.creditStateUpdateHandler((self) -> {
            sender1CreditStateUpdated.incrementAndGet();
        });

        final AtomicInteger sender2CreditStateUpdated = new AtomicInteger();
        sender2.creditStateUpdateHandler((self) -> {
            sender2CreditStateUpdated.incrementAndGet();
        });

        assertTrue(sender1.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());
        assertTrue(sender2.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach, Attach
        assertEquals(4, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery = sender1.next();
        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();

        assertEquals(1, asyncIOCallbacks.size());

        assertFalse(sender1.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        // Sender 2 shouldn't be able to send since sender 1 consumed the outgoing window
        assertFalse(sender2.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        // Free a frame's worth of window which should trigger both senders sendable update event
        asyncIOCallbacks.poll().run();
        assertEquals(0, asyncIOCallbacks.size());

        assertTrue(sender1.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());
        assertEquals(1, sender1CreditStateUpdated.get());
        assertTrue(sender2.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());
        assertEquals(1, sender2CreditStateUpdated.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testSingleSenderUpdatedWhenOutgoingWindowOpenedForTwoIfFirstConsumesSessionOutgoingWindow() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Queue<Runnable> asyncIOCallbacks = new ArrayDeque<>();
        ProtonTestConnector peer = createTestPeer(engine, asyncIOCallbacks);

        final byte[] payload = new byte[] {0, 1, 2, 3, 4};
        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(1024).respond();
        peer.expectBegin().withNextOutgoingId(0).respond().withNextOutgoingId(0);
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).withNextIncomingId(0).withIncomingWindow(8192).queue();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(20).withNextIncomingId(0).withIncomingWindow(8192).queue();

        Connection connection = engine.start().setMaxFrameSize(1024).open();
        Session session = connection.session().setOutgoingCapacity(1024).open();
        Sender sender1 = session.sender("test1").setDeliveryTagGenerator(generator).open();
        Sender sender2 = session.sender("test2").setDeliveryTagGenerator(generator).open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        final AtomicInteger sender1CreditStateUpdated = new AtomicInteger();
        sender1.creditStateUpdateHandler((self) -> {
            sender1CreditStateUpdated.incrementAndGet();
            if (self.isSendable()) {
                OutgoingDelivery delivery = self.next();
                delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));
            }
        });

        final AtomicInteger sender2CreditStateUpdated = new AtomicInteger();
        sender2.creditStateUpdateHandler((self) -> {
            sender2CreditStateUpdated.incrementAndGet();
        });

        assertTrue(sender1.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());
        assertTrue(sender2.isSendable());
        assertEquals(1024, session.getRemainingOutgoingCapacity());

        // Open, Begin, Attach, Attach
        assertEquals(4, asyncIOCallbacks.size());
        asyncIOCallbacks.forEach(runner -> runner.run());
        asyncIOCallbacks.clear();

        OutgoingDelivery delivery = sender1.next();
        delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        peer.waitForScriptToComplete();
        peer.expectTransfer().withPayload(payload);

        assertEquals(1, asyncIOCallbacks.size());

        assertFalse(sender1.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        // Sender 2 shouldn't be able to send since sender 1 consumed the outgoing window
        assertFalse(sender2.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());

        // Should trigger sender 1 to send which should exhaust the outgoing credit
        asyncIOCallbacks.poll().run();
        assertEquals(1, asyncIOCallbacks.size()); // Sender one should have sent

        assertFalse(sender1.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        assertEquals(1, sender1CreditStateUpdated.get());
        assertFalse(sender2.isSendable());
        assertEquals(0, session.getRemainingOutgoingCapacity());
        // Should not have triggered an event for sender 2 being able to send since
        // sender one consumed the outgoing window already.
        assertEquals(0, sender2CreditStateUpdated.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testHandleInUseErrorReturnedIfAttachWithAlreadyBoundHandleArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).respond().withHandle(0);
        peer.expectAttach().withHandle(1).respond().withHandle(0);
        peer.expectEnd().withError(SessionError.HANDLE_IN_USE.toString(),  "Attach received with handle that is already in use");

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        session.sender("test1").open();
        session.sender("test2").open();

        peer.waitForScriptToComplete();
        peer.expectClose().respond();

        connection.close();

        assertNull(failure);
    }

    @Test
    public void testEngineFailedWhenSessionReceivesDetachForUnknownLink() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteDetach().withHandle(2).onChannel(0).queue();
        peer.expectClose().withError(notNullValue());

        Connection connection = engine.start().open();
        connection.session().open();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
        assertTrue(failure instanceof ProtocolViolationException);
    }

    @Test
    public void testEngineFailedWhenSessionReceivesTransferForUnknownLink() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().ofReceiver().respond();
        peer.remoteDetach().queue();
        peer.remoteTransfer().withHandle(0)
                             .withDeliveryId(1)
                             .withDeliveryTag(new byte[] {1})
                             .onChannel(0)
                             .queue();
        peer.expectClose().withError(notNullValue());

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        session.receiver("test").open();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
        assertTrue(failure instanceof ProtocolViolationException);
    }

    @Test
    public void testSessionWideDeliveryMonitoringHandler() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        final AtomicBoolean deliveryReadByReceiver = new AtomicBoolean();
        final AtomicBoolean deliveryReadBySession = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().ofReceiver().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteTransfer().withHandle(0)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .onChannel(0)
                             .queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();

        session.deliveryReadHandler((delivery) -> deliveryReadBySession.set(true));

        Receiver receiver = session.receiver("test");
        receiver.deliveryReadHandler((delivery) -> deliveryReadByReceiver.set(true));
        receiver.open().addCredit(1);

        peer.waitForScriptToComplete();

        assertTrue(deliveryReadByReceiver.get());
        assertTrue(deliveryReadBySession.get());

        assertNull(failure);
    }
}
