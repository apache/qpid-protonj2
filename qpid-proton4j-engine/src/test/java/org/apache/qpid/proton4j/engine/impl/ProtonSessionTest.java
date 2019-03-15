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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.types.AMQPHeaderType;
import org.apache.qpid.proton4j.amqp.driver.types.BeginType;
import org.apache.qpid.proton4j.amqp.driver.types.OpenType;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
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

        AMQPHeaderType.expectAMQPHeader(driver).respondWithAMQPHeader();
        OpenType.expect(driver).respond().withContainerId("driver");
        BeginType.expect(driver).respond().onChannel(1).withRemoteChannel(0).
                                                        withOutgoingWindow(0).
                                                        withIncomingWindow(0).
                                                        withNextOutgoingId(1);  // TODO make default response smarter

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

        final AtomicBoolean connectionRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        ProtonConnection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        engine.outputHandler((buffer) -> {
            engineWrites.add(buffer);
        });

        connection.open();
        connection.openEventHandler((result) -> {
            connectionRemotelyOpened.set(true);
        });

        // Expect the engine to emit the AMQP header
        assertEquals("Engine did not emit an AMQP Header on Open", 1, engineWrites.size());

        ProtonBuffer outputBuffer = engineWrites.get(0);
        assertEquals(AMQPHeader.HEADER_SIZE_BYTES, outputBuffer.getReadableBytes());
        AMQPHeader outputHeader = new AMQPHeader(outputBuffer);

        engine.ingest(outputHeader.getBuffer());

        // Expect the engine to emit the Open performative
        assertEquals("Engine did not emit an Open performative after receiving header response", 2, engineWrites.size());
        outputBuffer = engineWrites.get(1);
        assertNotNull(unwrapFrame(outputBuffer, Open.class));

        engine.ingest(wrapInFrame(new Open(), 0));

        assertTrue("Connection remote opened event did not fire", connectionRemotelyOpened.get());

        ProtonSession session = connection.session();
        session.openHandler(result -> {
            sessionRemotelyOpened.set(true);
        });
        session.open();

        // Expect the engine to emit the Open performative
        assertEquals("Engine did not emit an Begin performative after session was locally opened.", 3, engineWrites.size());
        outputBuffer = engineWrites.get(2);
        assertNotNull(unwrapFrame(outputBuffer, Begin.class));

        // Emit a remote Begin for this session and expect it to signal it was opened.
        Begin remoteBegin = new Begin();
        remoteBegin.setRemoteChannel(session.getLocalChannel());
        remoteBegin.setNextOutgoingId(1);
        remoteBegin.setIncomingWindow(0);
        remoteBegin.setOutgoingWindow(0);
        engine.ingest(wrapInFrame(remoteBegin, 1));

        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());
    }

    @Test
    public void testEngineFireRemotelyOpenedSessionEventWhenRemoteBeginArrives() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        final AtomicBoolean connectionRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean sessionRemotelyOpened = new AtomicBoolean();

        final AtomicReference<Session> remoteSession = new AtomicReference<>();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        engine.outputHandler((buffer) -> {
            engineWrites.add(buffer);
        });

        connection.open();
        connection.openEventHandler((result) -> {
            connectionRemotelyOpened.set(true);
        });
        connection.sessionOpenEventHandler(result -> {
            remoteSession.set(result);
            sessionRemotelyOpened.set(true);
        });

        // Expect the engine to emit the AMQP header
        assertEquals("Engine did not emit an AMQP Header on Open", 1, engineWrites.size());

        ProtonBuffer outputBuffer = engineWrites.get(0);
        assertEquals(AMQPHeader.HEADER_SIZE_BYTES, outputBuffer.getReadableBytes());
        AMQPHeader outputHeader = new AMQPHeader(outputBuffer);

        engine.ingest(outputHeader.getBuffer());

        // Expect the engine to emit the Open performative
        assertEquals("Engine did not emit an Open performative after receiving header response", 2, engineWrites.size());
        outputBuffer = engineWrites.get(1);
        assertNotNull(unwrapFrame(outputBuffer, Open.class));

        engine.ingest(wrapInFrame(new Open(), 0));

        assertTrue("Connection remote opened event did not fire", connectionRemotelyOpened.get());

        // Emit a remote Begin and expect the Connection to generate a local event and session
        Begin remoteBegin = new Begin();
        remoteBegin.setNextOutgoingId(1);
        remoteBegin.setIncomingWindow(0);
        remoteBegin.setOutgoingWindow(0);
        engine.ingest(wrapInFrame(remoteBegin, 1));

        assertTrue("Session remote opened event did not fire", sessionRemotelyOpened.get());
        assertNotNull("Connection did not create a local session for remote open", remoteSession.get());
    }
}
