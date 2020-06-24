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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.test.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.test.driver.matchers.types.UnsignedIntegerMatcher;
import org.apache.qpid.proton4j.test.driver.matchers.types.UnsignedShortMatcher;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.transport.AMQPHeader;
import org.apache.qpid.proton4j.types.transport.ConnectionError;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Tests for behaviors of the ProtonConnection class
 */
public class ProtonConnectionTest extends ProtonEngineTestSupport {

    @Test(timeout = 10000)
    public void testNegotiateSendsAMQPHeader() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();

        Connection connection = engine.start();

        connection.negotiate();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 10000)
    public void testNegotiateSendsAMQPHeaderAndFireRemoteHeaderEvent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();

        Connection connection = engine.start();

        connection.negotiate();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 10000)
    public void testNegotiateSendsAMQPHeaderEnforcesNotNullEventHandler() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Connection connection = engine.start();

        try {
            connection.negotiate(null);
            fail("Should not allow null event handler");
        } catch (NullPointerException npe) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 10000)
    public void testNegotiateDoesNotSendAMQPHeaderAfterOpen() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        final AtomicInteger headerReceivedCallback = new AtomicInteger();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        connection.negotiate(amqpHeader -> headerReceivedCallback.incrementAndGet());
        assertEquals(1, headerReceivedCallback.get());
        connection.negotiate(amqpHeader -> headerReceivedCallback.incrementAndGet());
        assertEquals(2, headerReceivedCallback.get());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 10000)
    public void testConnectionEmitsOpenAndCloseEvents() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        final AtomicBoolean connectionLocalOpen = new AtomicBoolean();
        final AtomicBoolean connectionLocalClose = new AtomicBoolean();
        final AtomicBoolean connectionRemoteOpen = new AtomicBoolean();
        final AtomicBoolean connectionRemoteClose = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.localOpenHandler(result -> connectionLocalOpen.set(true))
                  .localCloseHandler(result -> connectionLocalClose.set(true))
                  .openHandler(result -> connectionRemoteOpen.set(true))
                  .closeHandler(result -> connectionRemoteClose.set(true));

        connection.open();
        connection.close();

        assertTrue("Connection should have reported local open", connectionLocalOpen.get());
        assertTrue("Connection should have reported local close", connectionLocalClose.get());
        assertTrue("Connection should have reported remote open", connectionRemoteOpen.get());
        assertTrue("Connection should have reported remote close", connectionRemoteClose.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 10000)
    public void testConnectionPopulatesRemoteData() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        Symbol[] offeredCapabilities = new Symbol[] { Symbol.valueOf("one"), Symbol.valueOf("two") };
        Symbol[] desiredCapabilities = new Symbol[] { Symbol.valueOf("three"), Symbol.valueOf("four") };

        Map<String, Object> expectedProperties = new HashMap<>();
        expectedProperties.put("test", "value");

        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), "value");

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("test")
                                   .withHostname("localhost")
                                   .withIdleTimeOut(60000)
                                   .withOfferedCapabilities(new String[] { "one", "two" })
                                   .withDesiredCapabilities(new String[] { "three", "four" })
                                   .withProperties(expectedProperties);
        peer.expectClose();

        Connection connection = engine.start().open();

        assertEquals("test", connection.getRemoteContainerId());
        assertEquals("localhost", connection.getRemoteHostname());
        assertEquals(60000, connection.getRemoteIdleTimeout());

        assertArrayEquals(offeredCapabilities, connection.getRemoteOfferedCapabilities());
        assertArrayEquals(desiredCapabilities, connection.getRemoteDesiredCapabilities());
        assertEquals(properties, connection.getRemoteProperties());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testOpenAndCloseConnectionWithNullSetsOnConnectionOptions() throws IOException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectClose();

        Connection connection = engine.start();

        connection.setProperties(null);
        connection.setOfferedCapabilities((Symbol[]) null);
        connection.setDesiredCapabilities((Symbol[]) null);
        connection.setCondition(null);
        connection.open();

        assertNotNull(connection.getAttachments());
        assertNull(connection.getProperties());
        assertNull(connection.getOfferedCapabilities());
        assertNull(connection.getDesiredCapabilities());
        assertNull(connection.getCondition());

        assertNull(connection.getRemoteProperties());
        assertNull(connection.getRemoteOfferedCapabilities());
        assertNull(connection.getRemoteDesiredCapabilities());
        assertNull(connection.getRemoteCondition());

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEndpointEmitsEngineShutdownEvent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        final AtomicBoolean engineShutdown = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.engineShutdownHandler(result -> engineShutdown.set(true));

        connection.open();
        connection.close();

        engine.shutdown();

        assertTrue("Connection should have reported engine shutdown", engineShutdown.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionOpenAndCloseAreIdempotent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue(remoteOpened.get());
        assertNull(failure);
    }

    @Test
    public void testConnectionRemoteOpenTriggeredWhenRemoteOpenArrivesBeforeLocalOpen() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openHandler((result) -> {
            remoteOpened.set(true);
        });

        peer.expectAMQPHeader();

        // Remote Header will prompt local response and then remote open should trigger
        // the connection handler to fire so that user knows remote opened.
        peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();
        peer.remoteOpen().now();

        peer.waitForScriptToComplete();

        assertTrue(remoteOpened.get());
        assertNull(failure);
    }

    @Test
    public void testConnectionRemoteCloseTriggeredWhenRemoteCloseArrives() throws EngineStateException {
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
        final int expectedMaxFrameSize = 32767;
        final Matcher<?> expectedMaxFrameSizeMatcher;
        if (setMaxFrameSize) {
            expectedMaxFrameSizeMatcher = new UnsignedIntegerMatcher(expectedMaxFrameSize);
        } else {
            expectedMaxFrameSizeMatcher = new UnsignedIntegerMatcher(ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE);
        }

        String expectedContainerId = "";
        if (setContainerId) {
            expectedContainerId = "test";
        }

        final short expectedChannelMax = 512;
        final Matcher<?> expectedChannelMaxMatcher;
        if (setChannelMax) {
            expectedChannelMaxMatcher = new UnsignedShortMatcher(expectedChannelMax);
        } else {
            expectedChannelMaxMatcher = nullValue();
        }

        String expectedHostname = null;
        if (setHostname) {
            expectedHostname = "localhost";
        }
        final int expectedIdleTimeout = 60000;
        final Matcher<?> expectedIdleTimeoutMatcher;
        if (setIdleTimeout) {
            expectedIdleTimeoutMatcher = new UnsignedIntegerMatcher(expectedIdleTimeout);
        } else {
            expectedIdleTimeoutMatcher = nullValue();
        }

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(expectedMaxFrameSizeMatcher)
                         .withChannelMax(expectedChannelMaxMatcher)
                         .withContainerId(expectedContainerId)
                         .withHostname(expectedHostname)
                         .withIdleTimeOut(expectedIdleTimeoutMatcher)
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
            connection.setIdleTimeout(expectedIdleTimeout);
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
        Symbol[] clientExpectedOfferedCapabilities = new Symbol[] { serverOfferedSymbol };
        Symbol[] clientExpectedDesiredCapabilities = new Symbol[] { serverDesiredSymbol };

        final AtomicBoolean remotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withOfferedCapabilities(new String[] { clientOfferedSymbol.toString() })
                         .withDesiredCapabilities(new String[] { clientDesiredSymbol.toString() })
                         .respond()
                         .withDesiredCapabilities(new String[] { serverDesiredSymbol.toString() })
                         .withOfferedCapabilities(new String[] { serverOfferedSymbol.toString() });
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
        assertArrayEquals(clientExpectedOfferedCapabilities, connection.getRemoteOfferedCapabilities());
        assertArrayEquals(clientExpectedDesiredCapabilities, connection.getRemoteDesiredCapabilities());

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

        Map<String, Object> clientPropertiesMap = new HashMap<>();
        clientPropertiesMap.put(clientPropertyName.toString(), clientPropertyValue);

        Map<String, Object> serverPropertiesMap = new HashMap<>();
        serverPropertiesMap.put(serverPropertyName.toString(), serverPropertyValue);

        final AtomicBoolean remotelyOpened = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withProperties(clientPropertiesMap)
                         .respond()
                         .withProperties(serverPropertiesMap);
        peer.expectClose().respond();

        Connection connection = engine.start();

        Map<Symbol, Object> clientProperties = new HashMap<>();
        clientProperties.put(clientPropertyName, clientPropertyValue);

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
        Map<String, Object> errorInfoExpectation = new HashMap<>();
        errorInfoExpectation.put("error", "value");
        errorInfoExpectation.put("error-list", Arrays.asList("entry-1", "entry-2", "entry-3"));

        Map<Symbol, Object> errorInfo = new HashMap<>();
        errorInfo.put(Symbol.getSymbol("error"), "value");
        errorInfo.put(Symbol.getSymbol("error-list"), Arrays.asList("entry-1", "entry-2", "entry-3"));
        ErrorCondition remoteCondition = new ErrorCondition(Symbol.getSymbol("myerror"), "mydescription", errorInfo);

        final AtomicBoolean remotelyOpened = new AtomicBoolean();
        final AtomicBoolean remotelyClosed = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.remoteClose().withErrorCondition("myerror", "mydescription", errorInfoExpectation).queue();
        peer.expectClose();

        Connection connection = engine.start();

        connection.openHandler(result -> {
            remotelyOpened.set(true);
        });
        connection.closeHandler(result -> {
            remotelyClosed.set(true);
        });
        connection.open();

        assertTrue(connection.isLocallyOpen());
        assertFalse(connection.isLocallyClosed());
        assertFalse(connection.isRemotelyOpen());
        assertTrue(connection.isRemotelyClosed());

        assertTrue("Connection remote opened event did not fire", remotelyOpened.get());
        assertTrue("Connection remote closed event did not fire", remotelyClosed.get());

        assertNull(connection.getCondition());
        assertNotNull(connection.getRemoteCondition());

        assertEquals(remoteCondition, connection.getRemoteCondition());

        connection.close();

        assertFalse(connection.isLocallyOpen());
        assertTrue(connection.isLocallyClosed());
        assertFalse(connection.isRemotelyOpen());
        assertTrue(connection.isRemotelyClosed());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEmptyFrameBeforeOpenDoesNotCauseError() throws Exception {
        final AtomicBoolean remotelyOpened = new AtomicBoolean();
        final AtomicBoolean remotelyClosed = new AtomicBoolean();

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        assertTrue(connection.isLocallyOpen());
        assertFalse(connection.isLocallyClosed());
        assertTrue(connection.isRemotelyOpen());
        assertFalse(connection.isRemotelyClosed());

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
        ProtonTestPeer peer = createTestPeer(engine);

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
        ProtonTestPeer peer = createTestPeer(engine);

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
    public void testCloseConnectionAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponse() throws Exception {
        testCloseConnectionAfterShutdownNoOutputAndNoException(true, true);
    }

    @Test
    public void testCloseConnectionAfterShutdownDoesNotThrowExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseConnectionAfterShutdownNoOutputAndNoException(true, false);
    }

    @Test
    public void testCloseConnectionAfterShutdownDoesNotThrowExceptionOpenNotWritten() throws Exception {
        testCloseConnectionAfterShutdownNoOutputAndNoException(false, false);
    }

    private void testCloseConnectionAfterShutdownNoOutputAndNoException(boolean respondToHeader, boolean respondToOpen) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

        engine.shutdown();

        // Should clean up and not throw as we knowingly shutdown engine operations.
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseConnectionAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponse() throws Exception {
        testCloseConnectionAfterEngineFailedThrowsAndNoOutputWritten(true, true);
    }

    @Test
    public void testCloseConnectionAfterFailureThrowsEngineStateExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseConnectionAfterEngineFailedThrowsAndNoOutputWritten(true, false);
    }

    @Test
    public void testCloseConnectionAfterFailureThrowsEngineStateExceptionOpenNotWritten() throws Exception {
        testCloseConnectionAfterEngineFailedThrowsAndNoOutputWritten(false, false);
    }

    private void testCloseConnectionAfterEngineFailedThrowsAndNoOutputWritten(boolean respondToHeader, boolean respondToOpen) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                peer.expectClose();
            } else {
                peer.expectOpen();
                peer.expectClose();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        engine.engineFailed(new IOException());

        try {
            connection.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        engine.shutdown();  // Explicit shutdown now allows local close to complete

        // Should clean up and not throw as we knowingly shutdown engine operations.
        connection.close();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
    }

    @Test
    public void testOpenAndCloseWhileWaitingForHeaderResponseDoesNotWriteUntilHeaderArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader();

        Connection connection = engine.start();
        connection.open();  // Trigger write of AMQP Header, we don't respond here.
        connection.close();

        peer.waitForScriptToComplete();

        // Now respond and Connection should open and close
        peer.expectOpen();
        peer.expectClose();
        peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();

        peer.waitForScriptToComplete();

        engine.shutdown();

        assertNull(failure);
    }

    @Test
    public void testOpenWhileWaitingForHeaderResponseDoesNotWriteThenWritesFlowAsExpected() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader();

        Connection connection = engine.start();
        connection.open();  // Trigger write of AMQP Header, we don't respond here.

        peer.waitForScriptToComplete();

        // Now respond and Connection should open and close
        peer.expectOpen();
        peer.expectClose().withError(notNullValue());
        peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();

        connection.setCondition(new ErrorCondition(ConnectionError.CONNECTION_FORCED, "something about errors")).close();

        peer.waitForScriptToComplete();

        engine.shutdown();

        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testCloseOrDetachWithErrorCondition() throws Exception {
        final String condition = "amqp:connection:forced";
        final String description = "something bad happened.";

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectClose().withError(condition, description).respond();

        Connection connection = engine.start();

        connection.open();
        connection.setCondition(new ErrorCondition(Symbol.valueOf(condition), description));
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
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
        ProtonTestPeer peer = createTestPeer(engine);

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
        ProtonTestPeer peer = createTestPeer(engine);

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
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetHostnameOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetHostnameOnLocallyClosedConnection() throws Exception {
        testCannotSetHostnameOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetHostnameOnRemotelyClosedConnection() throws Exception {
        testCannotSetHostnameOnClosedConnection(false);
    }

    private void testCannotSetHostnameOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetChannelMaxOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetChannelMaxOnLocallyClosedConnection() throws Exception {
        testCannotSetChannelMaxOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetChannelMaxOnRemotelyClosedConnection() throws Exception {
        testCannotSetChannelMaxOnClosedConnection(false);
    }

    private void testCannotSetChannelMaxOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetMaxFrameSizeOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetMaxFrameSizeOnLocallyClosedConnection() throws Exception {
        testCannotSetMaxFrameSizeOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetMaxFrameSizeOnRemotelyClosedConnection() throws Exception {
        testCannotSetMaxFrameSizeOnClosedConnection(false);
    }

    private void testCannotSetMaxFrameSizeOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetIdleTimeoutOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        Connection connection = engine.start();

        try {
            connection.setIdleTimeout(-1);
            fail("Should not be able to set idle timeout when negative value given");
        } catch (IllegalArgumentException error) {
            // Expected
        }
        try {
            connection.setIdleTimeout(Long.MAX_VALUE);
            fail("Should not be able to set idle timeout greater than unsigned integer value");
        } catch (IllegalArgumentException error) {
            // Expected
        }

        connection.open();

        assertEquals(0, connection.getIdleTimeout());

        try {
            connection.setIdleTimeout(65535);
            fail("Should not be able to set idle timeout from open Connection");
        } catch (IllegalStateException error) {
            // Expected
        }

        assertEquals(0, connection.getIdleTimeout());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testCannotSetIdleTimeoutOnLocallyClosedConnection() throws Exception {
        testCannotSetIdleTimeoutOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetIdleTimeoutOnRemotelyClosedConnection() throws Exception {
        testCannotSetIdleTimeoutOnClosedConnection(false);
    }

    private void testCannotSetIdleTimeoutOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetOfferedCapabilitiesOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetOfferedCapabilitiesOnLocallyClosedConnection() throws Exception {
        testCannotSetOfferedCapabilitiesOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetOfferedCapabilitiesOnRemotelyClosedConnection() throws Exception {
        testCannotSetOfferedCapabilitiesOnClosedConnection(false);
    }

    private void testCannotSetOfferedCapabilitiesOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetDesiredCapabilitiesOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetDesiredCapabilitiesOnLocallyClosedConnection() throws Exception {
        testCannotSetDesiredCapabilitiesOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetDesiredCapabilitiesOnRemotelyClosedConnection() throws Exception {
        testCannotSetDesiredCapabilitiesOnClosedConnection(false);
    }

    private void testCannotSetDesiredCapabilitiesOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetPropertiesOnOpenConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testCannotSetPropertiesOnLocallyClosedConnection() throws Exception {
        testCannotSetPropertiesOnClosedConnection(true);
    }

    @Test(timeout = 20000)
    public void testCannotSetPropertiesOnRemotelyClosedConnection() throws Exception {
        testCannotSetPropertiesOnClosedConnection(false);
    }

    private void testCannotSetPropertiesOnClosedConnection(boolean localClose) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

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

    @Test(timeout = 20000)
    public void testIterateAndCloseSessionsFromSessionsAPI() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectBegin().respond();
        peer.expectBegin().respond();

        Connection connection = engine.start().open();

        connection.session().open();
        connection.session().open();
        connection.session().open();

        peer.waitForScriptToComplete();

        peer.expectEnd().respond();
        peer.expectEnd().respond();
        peer.expectEnd().respond();
        peer.expectClose();

        connection.sessions().forEach(session -> session.close());
        connection.close();

        peer.waitForScriptToComplete();

        assertTrue(connection.sessions().isEmpty());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testConnectionClosedWhenChannelMaxExceeded() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        final AtomicBoolean closed = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withChannelMax(16).respond();
        peer.expectClose().withError(ConnectionError.FRAMING_ERROR.toString(), "Channel Max Exceeded for session Begin").respond();

        Connection connection = engine.start();

        connection.setChannelMax(16);
        connection.localCloseHandler(conn -> {
            closed.set(true);
        });
        connection.open();

        peer.remoteBegin().onChannel(32).now();

        peer.waitForScriptToComplete();

        assertTrue(connection.sessions().isEmpty());
        assertTrue(closed.get());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }
}
