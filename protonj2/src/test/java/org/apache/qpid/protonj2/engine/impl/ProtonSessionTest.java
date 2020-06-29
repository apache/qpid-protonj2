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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.ConnectionState;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Link;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.test.driver.ProtonTestPeer;
import org.apache.qpid.protonj2.test.driver.matchers.types.UnsignedIntegerMatcher;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Role;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Test behaviors of the ProtonSession implementation.
 */
public class ProtonSessionTest extends ProtonEngineTestSupport {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonEngineTestSupport.class);

    @Test(timeout = 30000)
    public void testSessionEmitsOpenAndCloseEvents() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
        session.close();

        assertTrue("Session should have reported local open", sessionLocalOpen.get());
        assertTrue("Session should have reported local close", sessionLocalClose.get());
        assertTrue("Session should have reported remote open", sessionRemoteOpen.get());
        assertTrue("Session should have reported remote close", sessionRemoteClose.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventNeitherEndClosed() throws Exception {
        doTestEngineShutdownEvent(false, false);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventLocallyClosed() throws Exception {
        doTestEngineShutdownEvent(true, false);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventRemotelyClosed() throws Exception {
        doTestEngineShutdownEvent(false, true);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventBothEndsClosed() throws Exception {
        doTestEngineShutdownEvent(true, true);
    }

    private void doTestEngineShutdownEvent(boolean locallyClosed, boolean remotelyClosed) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
            peer.remoteEnd();
        }

        engine.shutdown();

        if (locallyClosed && remotelyClosed) {
            assertFalse("Should not have reported engine shutdown", engineShutdown.get());
        } else {
            assertTrue("Should have reported engine shutdown", engineShutdown.get());
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testSessionOpenAndCloseAreIdempotent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testSenderCreateOnClosedSessionThrowsISE() throws Exception {
        testLinkCreateOnClosedSessionThrowsISE(false);
    }

    @Test(timeout = 30000)
    public void testReceiverCreateOnClosedSessionThrowsISE() throws Exception {
        testLinkCreateOnClosedSessionThrowsISE(true);
    }

    private void testLinkCreateOnClosedSessionThrowsISE(boolean receiver) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testOpenSessionBeforeOpenConnection() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testEngineEmitsBeginAfterLocalSessionOpened() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        connection.openHandler((result) -> {
            remoteOpened.set(true);
        });

        Session session = connection.session();
        session.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testSessionFiresOpenedEventAfterRemoteOpensLocallyOpenedSession() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Connection remote opened event did not fire", connectionRemotelyOpened.get());

        Session session = connection.session();
        session.openHandler(result -> {
            sessionRemotelyOpened.set(true);
        });
        session.open();

        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testNoSessionPerformativesEmiitedIfConnectionOpenedAndClosedBeforeAnyRemoteResponses() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testOpenAndCloseSessionWithNullSetsOnSessionOptions() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testOpenAndCloseMultipleSessions() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testEngineFireRemotelyOpenedSessionEventWhenRemoteBeginArrives() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Connection remote opened event did not fire", connectionRemotelyOpened.get());
        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());
        assertNotNull("Connection did not create a local session for remote open", remoteSession.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testSessionPopulatesBeginUsingDefaults() throws IOException {
        doTestSessionOpenPopulatesBegin(false, false);
    }

    @Test(timeout = 30000)
    public void testSessionPopulatesBeginWithConfiguredMaxFrameSizeButNoIncomingCapacity() throws IOException {
        doTestSessionOpenPopulatesBegin(true, false);
    }

    @Test(timeout = 30000)
    public void testSessionPopulatesBeginWithConfiguredMaxFrameSizeAndIncomingCapacity() throws IOException {
        doTestSessionOpenPopulatesBegin(true, true);
    }

    private void doTestSessionOpenPopulatesBegin(boolean setMaxFrameSize, boolean setIncomingCapacity) {
        final int MAX_FRAME_SIZE = 32767;
        final int SESSION_INCOMING_CAPACITY = Integer.MAX_VALUE;

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
                          .withNextOutgoingId(1)
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

    @Test(timeout = 30000)
    public void testSessionOpenFailsWhenConnectionClosed() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Connection remote opened event did not fire", connectionOpenedSignaled.get());
        assertTrue("Connection remote closed event did not fire", connectionClosedSignaled.get());

        try {
            session.open();
            fail("Should not be able to open a session when its Connection was already closed");
        } catch (IllegalStateException ise) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testSessionOpenFailsWhenConnectionRemotelyClosed() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Connection remote opened event did not fire", connectionOpenedSignaled.get());
        assertTrue("Connection remote closed event did not fire", connectionClosedSignaled.get());

        try {
            session.open();
            fail("Should not be able to open a session when its Connection was already closed");
        } catch (IllegalStateException ise) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testSessionOpenFailsWhenWriteOfBeginFailsWithException() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.rejectDataAfterLastScriptedElement();

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

    @Test(timeout = 30000)
    public void testSessionOpenNotSentUntilConnectionOpened() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertFalse("Session opened handler should not have been called yet", sessionOpenedSignaled.get());

        connection.open();

        // Session was already closed so no open event should fire.
        assertFalse("Session opened handler should not have been called yet", sessionOpenedSignaled.get());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testSessionRemotelyClosedWithError() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testSessionCloseAfterConnectionRemotelyClosedWhenNoBeginResponseReceived() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Rmote connection should have occured", remoteOpened.get());
        assertFalse("Should not have seen a remote session open.", remoteSession.get());

        assertNotNull(failure);
    }

    @Test(timeout = 30000)
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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());

        assertArrayEquals(clientOfferedCapabilities, session.getOfferedCapabilities());
        assertArrayEquals(clientDesiredCapabilities, session.getDesiredCapabilities());
        assertArrayEquals(serverOfferedCapabilities, session.getRemoteOfferedCapabilities());
        assertArrayEquals(serverDesiredCapabilities, session.getRemoteDesiredCapabilities());

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());

        assertNotNull(session.getProperties());
        assertNotNull(session.getRemoteProperties());

        assertEquals(clientPropertyValue, session.getProperties().get(clientPropertyName));
        assertEquals(serverPropertyValue, session.getRemoteProperties().get(serverPropertyName));

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testEmittedSessionIncomingWindowOnFirstFlow() {
        doSessionIncomingWindowTestImpl(false, false);
        doSessionIncomingWindowTestImpl(true, false);
        doSessionIncomingWindowTestImpl(false, true);
        doSessionIncomingWindowTestImpl(true, true);
    }

    private void doSessionIncomingWindowTestImpl(boolean setFrameSize, boolean setSessionCapacity) {
        final int TEST_MAX_FRAME_SIZE = 5 * 1024;
        final int TEST_SESSION_CAPACITY = 100 * 1024;

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
            // TODO - Hack to get test passing with current implementation of session windowing
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

        assertEquals("Unexpected session capacity", sessionCapacity, session.getIncomingCapacity());

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

        assertEquals("Unexpected delivery count", 1, deliveryArrived.get());
        assertNotNull(delivered.get());

        // Flow more credit after receiving a message but not consuming it should result in a decrease in
        // the incoming window if the capacity and max frame size are configured.
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize - 1;
        }
        peer.expectFlow().withLinkCredit(1)
                           .withIncomingWindow(expectedWindowSize);

        receiver.addCredit(1);

        // Settle the transfer then flow more credit, verify the emitted incoming window
        // (it should increase 1 if capacity and frame size set) otherwise remains unchanged.
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize + 1;
        }
        peer.expectFlow().withLinkCredit(2)
                         .withIncomingWindow(expectedWindowSize);

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

    @Test(timeout = 30000)
    public void testSessionHandlesDeferredOpenAndBeginResponses() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
        peer.remoteOpen().withOfferedCapabilities("ANONYMOUS_REALY").now();
        peer.respondToLastBegin().now();

        Sender sender = session.sender("sender-1");

        sender.open();

        session.close();

        assertEquals("Should get one opened event", 1, sessionOpened.get());
        assertEquals("Should get one closed event", 1, sessionClosed.get());

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test(timeout = 30000)
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponseBeginWrittenAndRsponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, true);
    }

    @Test(timeout = 30000)
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponseBeginWrittenAndNoRsponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, false);
    }

    @Test(timeout = 30000)
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, false, false);
    }

    @Test(timeout = 30000)
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenNotWritten() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(false, false, false);
    }

    private void testCloseAfterShutdownNoOutputAndNoException(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponseBeginWrittenAndReponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true);
    }

    @Test(timeout = 30000)
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponseBeginWrittenAndNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, false);
    }

    @Test(timeout = 30000)
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, false, false);
    }

    @Test(timeout = 30000)
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenNotWritten() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(false, false, false);
    }

    private void testCloseAfterEngineFailedThrowsAndNoOutputWritten(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertFalse("Session should not have signalled engine failure", engineFailedEvent.get());

        engine.shutdown();  // Explicit shutdown now allows local close to complete

        assertTrue("Session should have signalled engine failure", engineFailedEvent.get());

        // Should clean up and not throw as we knowingly shutdown engine operations.
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingClosedSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, false, true);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingDetchedSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, false, false);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingClosedSendersRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, true, true);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingDetachedSendersRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, true, true, false);
    }

    @Test(timeout = 30000)
    public void testSessionTracksRemotelyOpenSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, true, false, false, false);
    }

    @Test(timeout = 30000)
    public void testSessionTracksLocallyOpenSenders() throws Exception {
        doTestSessionTrackingOfLinks(Role.SENDER, false, true, false, false);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingClosedReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, false, true);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingDetchedReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, false, false);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingClosedReceiversRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, true, true);
    }

    @Test(timeout = 30000)
    public void testSessionStopTrackingDetachedReceiversRemoteGoesFirst() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, true, true, false);
    }

    @Test(timeout = 30000)
    public void testSessionTracksRemotelyOpenReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, true, false, false, false);
    }

    @Test(timeout = 30000)
    public void testSessionTracksLocallyOpenReceivers() throws Exception {
        doTestSessionTrackingOfLinks(Role.RECEIVER, false, true, false, false);
    }

    private void doTestSessionTrackingOfLinks(Role role, boolean localDetach, boolean remoteDetach, boolean remoteGoesFirst, boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 30000)
    public void testGetSenderFromSessionByName() throws Exception {
        doTestSessionLinkByName(Role.SENDER);
    }

    @Test(timeout = 30000)
    public void testGetReceiverFromSessionByName() throws Exception {
        doTestSessionLinkByName(Role.RECEIVER);
    }

    private void doTestSessionLinkByName(Role role) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCloseOrDetachWithErrorCondition() throws Exception {
        final String condition = "amqp:session:window-violation";
        final String description = "something bad happened.";

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
}
