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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test behaviors of the ProtonSession implementation.
 */
public class ProtonSessionTest extends ProtonEngineTestSupport {

    @Test
    public void testEngineEmitsBeginAfterLocalSessionOpened() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        connection.openEventHandler((result) -> {
            remoteOpened.set(true);
        });

        Session session = connection.session();
        session.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionFiresOpenedEventAfterRemoteOpensLocallyOpenedSession() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();

        final AtomicBoolean connectionRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        ProtonConnection connection = engine.start();

        connection.openEventHandler((result) -> {
            connectionRemotelyOpened.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", connectionRemotelyOpened.get());

        ProtonSession session = connection.session();
        session.openHandler(result -> {
            sessionRemotelyOpened.set(true);
        });
        session.open();

        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleSessions() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().onChannel(0).respond();
        script.expectBegin().onChannel(1).respond();
        script.expectEnd().onChannel(1).respond();
        script.expectEnd().onChannel(0).respond();
        script.expectClose();

        ProtonConnection connection = engine.start();
        connection.open();

        ProtonSession session1 = connection.session();
        session1.open();
        ProtonSession session2 = connection.session();
        session2.open();

        session2.close();
        session1.close();

        connection.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineFireRemotelyOpenedSessionEventWhenRemoteBeginArrives() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.remoteBegin();

        final AtomicBoolean connectionRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        final AtomicReference<Session> remoteSession = new AtomicReference<>();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler((result) -> {
            connectionRemotelyOpened.set(true);
        });
        connection.sessionOpenEventHandler(result -> {
            remoteSession.set(result);
            sessionRemotelyOpened.set(true);
        });
        connection.open();

        assertTrue("Connection remote opened event did not fire", connectionRemotelyOpened.get());
        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());
        assertNotNull("Connection did not create a local session for remote open", remoteSession.get());

        driver.assertScriptComplete();

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

        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        Matcher<?> expectedMaxFrameSize = nullValue();
        if (setMaxFrameSize) {
            expectedMaxFrameSize = equalTo(UnsignedInteger.valueOf(MAX_FRAME_SIZE));
        }

        int expectedIncomingWindow = Integer.MAX_VALUE;
        if (setIncomingCapacity) {
            expectedIncomingWindow = SESSION_INCOMING_CAPACITY / MAX_FRAME_SIZE;
        }

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().withMaxFrameSize(expectedMaxFrameSize).respond().withContainerId("driver");
        script.expectBegin().withHandleMax(nullValue())
                            .withNextOutgoingId(0)
                            .withIncomingWindow(expectedIncomingWindow)
                            .withOutgoingWindow(Integer.MAX_VALUE)
                            .withOfferedCapabilities(nullValue())
                            .withDesiredCapabilities(nullValue())
                            .withProperties(nullValue())
                            .respond();
        script.expectEnd().respond();

        ProtonConnection connection = engine.start();
        if (setMaxFrameSize) {
            connection.setMaxFrameSize(MAX_FRAME_SIZE);
        }
        connection.open();

        ProtonSession session = connection.session();
        if (setIncomingCapacity) {
            session.setIncomingCapacity(SESSION_INCOMING_CAPACITY);
        }
        session.open();
        session.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOpenFailsWhenConnectionClosed() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectClose().respond();

        final AtomicBoolean connectionOpenedSignaled = new AtomicBoolean();
        final AtomicBoolean connectionClosedSignaled = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler(result -> {
            connectionOpenedSignaled.set(true);
        });
        connection.closeEventHandler(result -> {
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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOpenFailsWhenConnectionRemotelyClosed() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.remoteClose();

        final AtomicBoolean connectionOpenedSignaled = new AtomicBoolean();
        final AtomicBoolean connectionClosedSignaled = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler(result -> {
            connectionOpenedSignaled.set(true);
        });
        connection.closeEventHandler(result -> {
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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionCloseDoesNothingWhenConnectionClosed() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectClose();

        Connection connection = engine.start();
        Session session = connection.session();
        connection.open();
        session.open();
        connection.close();

        // Now when calling close the operation should cause a frame to be emitted.
        session.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionOpenNotSentUntilConnectionOpened() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        Connection connection = engine.start();
        Session session = connection.session();
        session.open();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectClose();

        connection.open();
        connection.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSessionCloseNotSentUntilConnectionOpened() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        session.close();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectEnd().respond();
        script.expectClose();

        connection.open();
        connection.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Ignore("Handle invalid begin either by connection close or end of remotely opened resource.")
    @Test
    public void testHandleRemoteBeginWithInvalidRemoteChannelSet() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.remoteBegin().withRemoteChannel(3);
        final AtomicBoolean remoteOpened = new AtomicBoolean();
        final AtomicBoolean remoteSession = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        connection.openEventHandler((result) -> {
            remoteOpened.set(true);
        });

        connection.sessionOpenEventHandler(session -> {
            remoteSession.set(true);
        });

        driver.assertScriptComplete();

        assertFalse("Should not have seen a remote session open.", remoteSession.get());
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

        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().withOfferedCapabilities(clientOfferedCapabilities)
                            .withDesiredCapabilities(clientDesiredCapabilities)
                            .respond()
                            .withDesiredCapabilities(serverDesiredCapabilities)
                            .withOfferedCapabilities(serverOfferedCapabilities);
        script.expectEnd().respond();

        ProtonConnection connection = engine.start();
        connection.open();

        ProtonSession session = connection.session();

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

        driver.assertScriptComplete();

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

        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().withProperties(clientProperties)
                            .respond()
                            .withProperties(serverProperties);
        script.expectEnd().respond();

        ProtonConnection connection = engine.start();
        connection.open();

        ProtonSession session = connection.session();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testEmittedSessionIncomingWindowOnFirstFlow() {
        doSessionIncomingWindowTestImpl(false, false);
        doSessionIncomingWindowTestImpl(true, false);
        doSessionIncomingWindowTestImpl(false, true);
        doSessionIncomingWindowTestImpl(true, true);
    }

    private void doSessionIncomingWindowTestImpl(boolean setFrameSize, boolean setSessionCapacity) {
        final int TEST_MAX_FRAME_SIZE = 5 * 1024;
        final int TEST_SESSION_CAPACITY = 100 * 1024;

        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        Matcher<?> expectedMaxFrameSize = nullValue();
        if (setFrameSize) {
            expectedMaxFrameSize = equalTo(UnsignedInteger.valueOf(TEST_MAX_FRAME_SIZE));
        }

        long expectedWindowSize = 2147483647;
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = TEST_SESSION_CAPACITY / TEST_MAX_FRAME_SIZE;
        }

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().withMaxFrameSize(expectedMaxFrameSize).respond();
        script.expectBegin().withIncomingWindow(expectedWindowSize).respond();
        script.expectAttach().respond();

        ProtonConnection connection = engine.start();
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
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.incrementAndGet();
            delivered.set(delivery);
        });

        // Expect that a flow will be emitted and the window should match either default window
        // size or computed value if max frame size and capacity are set
        script.expectFlow().withLinkCredit(1)
                           .withIncomingWindow(expectedWindowSize);
        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0)  // TODO - Auto select last opened receiver link.
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0)
                               .withBody().withString("test-message");

        receiver.setCredit(1);

        assertEquals("Unexpected delivery count", 1, deliveryArrived.get());
        assertNotNull(delivered.get());

        // Flow more credit after receiving a message but not consuming it should result in a decrease in
        // the incoming window if the capacity and max frame size are configured.
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize - 1;
        }
        script.expectFlow().withLinkCredit(1)
                           .withIncomingWindow(expectedWindowSize);

        receiver.setCredit(1);

        // Settle the transfer then flow more credit, verify the emitted incoming window
        // (it should increase 1 if capacity and frame size set) otherwise remains unchanged.
        if (setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize + 1;
        }
        script.expectFlow().withLinkCredit(2)
                           .withIncomingWindow(expectedWindowSize);

        // This will consume the bytes and free them from the session window tracking.
        assertNotNull(delivered.get().readAll());

        receiver.setCredit(2);

        script.expectDetach().respond();
        script.expectEnd().respond();

        receiver.close();
        session.close();

        driver.assertScriptComplete();
        assertNull(failure);
    }
}
