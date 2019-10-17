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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.exceptions.EngineShutdownException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Tests for behaviors of the ProtonConnection class
 */
public class ProtonConnectionTest extends ProtonEngineTestSupport {

    @Test
    public void testConnectionOpenAndCloseAreIdempotent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectClose().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();

        // Should not emit another open frame
        connection.open();

        connection.close();

        // Should not emit another close frame
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionRemoteOpenTriggeredWhenRemoteOpenArrives() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openHandler((result) -> {
            remoteOpened.set(true);
        });

        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionRemoteCloseTriggeredWhenRemoteCloseArrives() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

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

        connection.open();

        assertEquals(ConnectionState.ACTIVE, connection.getState());
        assertEquals(ConnectionState.ACTIVE, connection.getRemoteState());

        connection.close();

        peer.waitForScriptToComplete();

        assertEquals(ConnectionState.CLOSED, connection.getState());
        assertEquals(ConnectionState.CLOSED, connection.getRemoteState());

        assertNull(failure);
    }

    @Test
    public void testConnectionOpenCarriesAllSetValues() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(true, true, true, true, true);
    }

    @Test
    public void testConnectionOpenCarriesDefaultMaxFrameSize() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetMaxFrameSize() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(true, false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesDefaultContainerId() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetContainerId() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, true, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesDefaultChannelMax() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetChannelMax() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, true, false, false);
    }

    @Test
    public void testConnectionOpenCarriesNoHostname() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetHostname() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, true, false);
    }

    @Test
    public void testConnectionOpenCarriesNoidleTimeout() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetIdleTimeout() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false, true);
    }

    private void doTestConnectionOpenPopulatesOpenCorrectly(boolean setMaxFrameSize, boolean setContainerId, boolean setChannelMax,
                                                            boolean setHostname, boolean setIdleTimeout) {
        int expectedMaxFrameSize = 32767;
        Matcher<?> expectedMaxFrameSizeMatcher = nullValue();
        if (setMaxFrameSize) {
            expectedMaxFrameSizeMatcher = equalTo(UnsignedInteger.valueOf(expectedMaxFrameSize));
        }
        String expectedContainerId = "";
        if (setContainerId) {
            expectedContainerId = "test";
        }
        short expectedChannelMax = 512;
        Matcher<?> expectedChannelMaxMatcher = nullValue();
        if (setChannelMax) {
            expectedChannelMaxMatcher = equalTo(UnsignedShort.valueOf(expectedChannelMax));
        }
        String expectedHostname = null;
        if (setHostname) {
            expectedHostname = "localhost";
        }
        UnsignedInteger expectedIdleTimeout = null;
        if (setIdleTimeout) {
            expectedIdleTimeout = UnsignedInteger.valueOf(60000);
        }

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(expectedMaxFrameSizeMatcher)
                         .withChannelMax(expectedChannelMaxMatcher)
                         .withContainerId(expectedContainerId)
                         .withHostname(expectedHostname)
                         .withIdleTimeOut(expectedIdleTimeout)
                         .withIncomingLocales(nullValue())
                         .withOutgoingLocales(nullValue())
                         .withDesiredCapabilities(nullValue())
                         .withOfferedCapabilities(nullValue())
                         .withProperties(nullValue())
                         .respond().withContainerId("driver");
        peer.expectClose().respond();

        Connection connection = engine.start();

        if (setMaxFrameSize) {
            connection.setMaxFrameSize(expectedMaxFrameSize);
        }
        if (setContainerId) {
            connection.setContainerId(expectedContainerId);
        }
        if (setChannelMax) {
            connection.setChannelMax(expectedChannelMax);
        }
        if (setHostname) {
            connection.setHostname(expectedHostname);
        }
        if (setIdleTimeout) {
            connection.setIdleTimeout(expectedIdleTimeout.intValue());
        }

        connection.open();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
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

        final AtomicBoolean remotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withOfferedCapabilities(clientOfferedCapabilities)
                         .withDesiredCapabilities(clientDesiredCapabilities)
                         .respond()
                         .withDesiredCapabilities(serverDesiredCapabilities)
                         .withOfferedCapabilities(serverOfferedCapabilities);
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.setDesiredCapabilities(clientDesiredCapabilities);
        connection.setOfferedCapabilities(clientOfferedCapabilities);
        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());

        assertArrayEquals(clientOfferedCapabilities, connection.getOfferedCapabilities());
        assertArrayEquals(clientDesiredCapabilities, connection.getDesiredCapabilities());
        assertArrayEquals(serverOfferedCapabilities, connection.getRemoteOfferedCapabilities());
        assertArrayEquals(serverDesiredCapabilities, connection.getRemoteDesiredCapabilities());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testPropertiesArePopulatedAndAccessible() throws Exception {
        final Symbol clientPropertyName = Symbol.valueOf("ClientPropertyName");
        final Integer clientPropertyValue = 1234;
        final Symbol serverPropertyName = Symbol.valueOf("ServerPropertyName");
        final Integer serverPropertyValue = 5678;

        Map<Symbol, Object> clientProperties = new HashMap<>();
        clientProperties.put(clientPropertyName, clientPropertyValue);

        Map<Symbol, Object> serverProperties = new HashMap<>();
        serverProperties.put(serverPropertyName, serverPropertyValue);

        final AtomicBoolean remotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withProperties(clientProperties)
                         .respond()
                         .withProperties(serverProperties);
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.setProperties(clientProperties);
        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());

        assertNotNull(connection.getProperties());
        assertNotNull(connection.getRemoteProperties());

        assertEquals(clientPropertyValue, connection.getProperties().get(clientPropertyName));
        assertEquals(serverPropertyValue, connection.getRemoteProperties().get(serverPropertyName));

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenedCarriesRemoteErrorCondition() throws Exception {
        Map<Symbol, Object> errorInfo = new HashMap<>();
        errorInfo.put(Symbol.getSymbol("error"), "value");
        errorInfo.put(Symbol.getSymbol("error-list"), Arrays.asList("entry-1", "entry-2", "entry-3"));
        ErrorCondition remoteCondition = new ErrorCondition(Symbol.getSymbol("myerror"), "mydepeerion", errorInfo);

        final AtomicBoolean remotelyOpened = new AtomicBoolean();
        final AtomicBoolean remotelyClosed = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.remoteClose().withErrorCondition(remoteCondition).queue();
        peer.expectClose();

        Connection connection = engine.start();

        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });
        connection.closeHandler(result -> {
            remotelyClosed.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());
        assertTrue("Connection remote closed event did not fire", remotelyClosed.get());

        assertNull(connection.getCondition());
        assertNotNull(connection.getRemoteCondition());

        assertEquals(remoteCondition, connection.getRemoteCondition());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEmptyFrameBeforeOpenDoesNotCauseError() throws Exception {
        final AtomicBoolean remotelyOpened = new AtomicBoolean();
        final AtomicBoolean remotelyClosed = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.remoteEmptyFrame().queue();
        peer.remoteOpen().queue();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });
        connection.closeHandler(result -> {
            remotelyClosed.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());

        connection.close();

        assertTrue("Connection remote closed event did not fire", remotelyClosed.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testChannelMaxDefaultsToMax() throws Exception {
        final AtomicBoolean remotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withChannelMax(nullValue()).respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());

        assertEquals(65535, connection.getChannelMax());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testChannelMaxRangeEnforced() throws Exception {
        final AtomicBoolean remotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final short eventualChannelMax = 255;

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withChannelMax(eventualChannelMax).respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });

        try {
            connection.setChannelMax(-1);
            fail("Should not be able to set an invalid negative channel max");
        } catch (IllegalArgumentException iae) {}
        try {
            connection.setChannelMax(65536);
            fail("Should not be able to set an invalid to large channel max");
        } catch (IllegalArgumentException iae) {}

        connection.setChannelMax(eventualChannelMax);
        connection.open();

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());
        assertEquals(eventualChannelMax, connection.getChannelMax());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseConnectionAfterShutdownThrowsEngineStateExceptionOpenWrittenAndResponse() throws Exception {
        testCloseConnectionAfterShutdownThrowsEngineStateException(true, true);
    }

    @Test
    public void testCloseConnectionAfterShutdownThrowsEngineStateExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseConnectionAfterShutdownThrowsEngineStateException(true, false);
    }

    @Test
    public void testCloseConnectionAfterShutdownThrowsEngineStateExceptionOpenNotWritten() throws Exception {
        testCloseConnectionAfterShutdownThrowsEngineStateException(false, false);
    }

    private void testCloseConnectionAfterShutdownThrowsEngineStateException(boolean respondToHeader, boolean respondToOpen) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
            } else {
                peer.expectOpen();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        // Shutdown before header response
        engine.shutdown();

        // Close should not allow anything to be written as the engine is shutdown
        try {
            connection.close();
            fail("Should fail on close when Engine already shutdown");
        } catch (EngineShutdownException ex) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseWhileWaitingForHeaderResponseDoesNotWrite() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader();

        Connection connection = engine.start();
        connection.open();  // Trigger write of AMQP Header, we don't respond here.
        connection.close();

        peer.waitForScriptToComplete();

        engine.shutdown();

        assertNull(failure);
    }

    @Test
    public void testCannotCreateSessionFromLocallyClosedConnection() throws Exception {
        testCannotCreateSessionFromClosedConnection(true);
    }

    @Test
    public void testCannotCreateSessionFromRemotelyClosedConnection() throws Exception {
        testCannotCreateSessionFromClosedConnection(false);
    }

    private void testCannotCreateSessionFromClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.session();
            fail("Should not create new Session from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetContainerIdOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setContainerId("test");
            fail("Should not be able to set container ID from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetContainerIdOnLocallyClosedConnection() throws Exception {
        testCannotSetContainerIdOnClosedConnection(true);
    }

    @Test
    public void testCannotSetContainerIdOnRemotelyClosedConnection() throws Exception {
        testCannotSetContainerIdOnClosedConnection(false);
    }

    private void testCannotSetContainerIdOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setContainerId("test");
            fail("Should not be able to set container ID from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetHostnameOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setHostname("test");
            fail("Should not be able to set host name from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetHostnameOnLocallyClosedConnection() throws Exception {
        testCannotSetHostnameOnClosedConnection(true);
    }

    @Test
    public void testCannotSetHostnameOnRemotelyClosedConnection() throws Exception {
        testCannotSetHostnameOnClosedConnection(false);
    }

    private void testCannotSetHostnameOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setHostname("test");
            fail("Should not be able to set host name from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetChannelMaxOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setChannelMax(0);
            fail("Should not be able to set channel max from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetChannelMaxOnLocallyClosedConnection() throws Exception {
        testCannotSetChannelMaxOnClosedConnection(true);
    }

    @Test
    public void testCannotSetChannelMaxOnRemotelyClosedConnection() throws Exception {
        testCannotSetChannelMaxOnClosedConnection(false);
    }

    private void testCannotSetChannelMaxOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setChannelMax(0);
            fail("Should not be able to set channel max from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetMaxFrameSizeOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setMaxFrameSize(65535);
            fail("Should not be able to set max frame size from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetMaxFrameSizeOnLocallyClosedConnection() throws Exception {
        testCannotSetMaxFrameSizeOnClosedConnection(true);
    }

    @Test
    public void testCannotSetMaxFrameSizeOnRemotelyClosedConnection() throws Exception {
        testCannotSetMaxFrameSizeOnClosedConnection(false);
    }

    private void testCannotSetMaxFrameSizeOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setMaxFrameSize(65535);
            fail("Should not be able to set max frame size from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetIdleTimeoutOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setIdleTimeout(65535);
            fail("Should not be able to set idle timeout from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetIdleTimeoutOnLocallyClosedConnection() throws Exception {
        testCannotSetIdleTimeoutOnClosedConnection(true);
    }

    @Test
    public void testCannotSetIdleTimeoutOnRemotelyClosedConnection() throws Exception {
        testCannotSetIdleTimeoutOnClosedConnection(false);
    }

    private void testCannotSetIdleTimeoutOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setIdleTimeout(65535);
            fail("Should not be able to set idle timeout from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetOfferedCapabilitiesOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setOfferedCapabilities(Symbol.valueOf("ANONYMOUS_RELAY"));
            fail("Should not be able to set offered capabilities from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetOfferedCapabilitiesOnLocallyClosedConnection() throws Exception {
        testCannotSetOfferedCapabilitiesOnClosedConnection(true);
    }

    @Test
    public void testCannotSetOfferedCapabilitiesOnRemotelyClosedConnection() throws Exception {
        testCannotSetOfferedCapabilitiesOnClosedConnection(false);
    }

    private void testCannotSetOfferedCapabilitiesOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setOfferedCapabilities(Symbol.valueOf("ANONYMOUS_RELAY"));
            fail("Should not be able to set offered capabilities from closed Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetDesiredCapabilitiesOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setDesiredCapabilities(Symbol.valueOf("ANONYMOUS_RELAY"));
            fail("Should not be able to set desired capabilities from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetDesiredCapabilitiesOnLocallyClosedConnection() throws Exception {
        testCannotSetDesiredCapabilitiesOnClosedConnection(true);
    }

    @Test
    public void testCannotSetDesiredCapabilitiesOnRemotelyClosedConnection() throws Exception {
        testCannotSetDesiredCapabilitiesOnClosedConnection(false);
    }

    private void testCannotSetDesiredCapabilitiesOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setDesiredCapabilities(Symbol.valueOf("ANONYMOUS_RELAY"));
            fail("Should not be able to set desired capabilities from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetPropertiesOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();
        connection.open();

        try {
            connection.setProperties(Collections.emptyMap());
            fail("Should not be able to set properties from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotSetPropertiesOnLocallyClosedConnection() throws Exception {
        testCannotSetPropertiesOnClosedConnection(true);
    }

    @Test
    public void testCannotSetPropertiesOnRemotelyClosedConnection() throws Exception {
        testCannotSetPropertiesOnClosedConnection(false);
    }

    private void testCannotSetPropertiesOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        if (localClose) {
            peer.expectClose().respond();
        } else {
            peer.remoteClose().queue();
        }

        Connection connection = engine.start();
        connection.open();
        if (localClose) {
            connection.close();
        }

        try {
            connection.setProperties(Collections.emptyMap());
            fail("Should not be able to set properties from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }
}
