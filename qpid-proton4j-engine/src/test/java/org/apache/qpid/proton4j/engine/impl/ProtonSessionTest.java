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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Session;
import org.junit.Test;

/**
 * Test behaviors of the ProtonSession implementation.
 */
public class ProtonSessionTest extends ProtonEngineTestSupport {

    @Test
    public void testEngineEmitsBeginAfterLocalSessionOpened() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
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

        // Create the test driver and link it to the engine for output handling.
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

        // Create the test driver and link it to the engine for output handling.
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

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.remoteBegin().withNextOutgoingId(1)
                            .withIncomingWindow(0)
                            .withOutgoingWindow(0).onChannel(1);

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

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        if (setMaxFrameSize) {
            script.expectOpen().withMaxFrameSize(MAX_FRAME_SIZE).respond().withContainerId("driver");
        } else {
            script.expectOpen().withMaxFrameSize(UnsignedInteger.MAX_VALUE).respond().withContainerId("driver");
        }

        int expectedIncomingWindow = Integer.MAX_VALUE;
        if (setIncomingCapacity) {
            expectedIncomingWindow = SESSION_INCOMING_CAPACITY / MAX_FRAME_SIZE;
        }

        script.expectBegin().withHandleMax(nullValue())
                            .withNextOutgoingId(1)
                            .withIncomingWindow(expectedIncomingWindow)
                            .withOutgoingWindow(Integer.MAX_VALUE)
                            .withHandleMax(nullValue())
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
}
