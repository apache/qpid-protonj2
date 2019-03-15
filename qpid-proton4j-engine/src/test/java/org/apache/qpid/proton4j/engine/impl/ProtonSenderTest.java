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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Sender;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the {@link ProtonSender}
 */
public class ProtonSenderTest extends ProtonEngineTestSupport {

    private ProtonConnection connection;

    @Before
    public void setUp() {
        connection = null;
    }

    @Test
    public void testEngineEmitsAttachAfterLocalSenderOpened() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        ProtonSession session = setupEngineAndOpenSession(engine);

        Sender sender = session.sender("test");
        sender.open();

        // Expect the engine to emit the Attach performative
        assertEquals("Engine did not emit an Attach performative after sender was locally opened.", 4, engineWrites.size());
        ProtonBuffer outputBuffer = engineWrites.get(3);
        assertNotNull(unwrapFrame(outputBuffer, Attach.class));

        sender.close();

        // Expect the engine to emit the Detach performative
        assertEquals("Engine did not emit an Detach performative after sender was locally closed.", 5, engineWrites.size());
        outputBuffer = engineWrites.get(4);
        assertNotNull(unwrapFrame(outputBuffer, Detach.class));
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        ProtonSession session = setupEngineAndOpenSession(engine);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        Sender sender = session.sender("test");
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.open();

        // Expect the engine to emit the Attach performative
        assertEquals("Engine did not emit an Attach performative after sender was locally opened.", 4, engineWrites.size());
        ProtonBuffer outputBuffer = engineWrites.get(3);
        assertNotNull(unwrapFrame(outputBuffer, Attach.class));

        // Emit a remote Attach for this sender and expect the opened event to fire
        Attach remoteAttach = new Attach();
        remoteAttach.setName("test");
        remoteAttach.setHandle(1);
        remoteAttach.setRole(Role.RECEIVER);
        remoteAttach.setInitialDeliveryCount(0);
        engine.ingest(wrapInFrame(remoteAttach, 1));

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());

        sender.close();

        // Expect the engine to emit the Detach performative
        assertEquals("Engine did not emit an Detach performative after sender was locally closed.", 5, engineWrites.size());
        outputBuffer = engineWrites.get(4);
        assertNotNull(unwrapFrame(outputBuffer, Detach.class));
    }

    @Test
    public void testSenderFireClosedEventAfterRemoteDetachArrives() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        ProtonSession session = setupEngineAndOpenSession(engine);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();

        Sender sender = session.sender("test");
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.closeHandler(result -> {
            senderRemotelyClosed.set(true);
        });
        sender.open();

        // Expect the engine to emit the Attach performative
        assertEquals("Engine did not emit an Attach performative after link was locally opened.", 4, engineWrites.size());
        ProtonBuffer outputBuffer = engineWrites.get(3);
        assertNotNull(unwrapFrame(outputBuffer, Attach.class));

        // Emit a remote Attach for this link and expect the opened event to fire
        Attach remoteAttach = new Attach();
        remoteAttach.setName("test");
        remoteAttach.setHandle(1);
        remoteAttach.setRole(Role.RECEIVER);
        remoteAttach.setInitialDeliveryCount(0);
        engine.ingest(wrapInFrame(remoteAttach, 1));

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());

        sender.close();

        // Expect the engine to emit the Detach performative
        assertEquals("Engine did not emit an Detach performative after link was locally closed.", 5, engineWrites.size());
        outputBuffer = engineWrites.get(4);
        assertNotNull(unwrapFrame(outputBuffer, Detach.class));

        // Emit a remote Detach for this Sender and expect the opened event to fire
        Detach remoteDetach = new Detach();
        remoteDetach.setClosed(true);
        remoteDetach.setHandle(1);
        engine.ingest(wrapInFrame(remoteDetach, 1));

        assertTrue("Sender remote closed event did not fire", senderRemotelyClosed.get());
    }

    @Test
    public void testConnectionSignalsRemoteSenderOpen() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        ProtonSession session = setupEngineAndOpenSession(engine);

        assertNotNull(session);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Sender> sender = new AtomicReference<>();

        connection.senderOpenEventHandler(result -> {
            senderRemotelyOpened.set(true);
            sender.set(result);
        });

        // Emit a remote Attach for this sender and expect the opened event to fire
        Attach remoteAttach = new Attach();
        remoteAttach.setName("test");
        remoteAttach.setHandle(1);
        remoteAttach.setRole(Role.RECEIVER);
        remoteAttach.setInitialDeliveryCount(0);
        engine.ingest(wrapInFrame(remoteAttach, 1));

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());

        sender.get().open();

        // Expect the engine to emit the Attach performative
        assertEquals("Engine did not emit an Attach performative after sender was locally opened.", 4, engineWrites.size());
        ProtonBuffer outputBuffer = engineWrites.get(3);
        assertNotNull(unwrapFrame(outputBuffer, Attach.class));

        sender.get().close();

        // Expect the engine to emit the Detach performative
        assertEquals("Engine did not emit an Detach performative after sender was locally closed.", 5, engineWrites.size());
        outputBuffer = engineWrites.get(4);
        assertNotNull(unwrapFrame(outputBuffer, Detach.class));
    }

    private ProtonSession setupEngineAndOpenSession(ProtonEngine engine) throws Exception {
        final AtomicBoolean remoteOpened = new AtomicBoolean();

        connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        engine.outputHandler((buffer) -> {
            engineWrites.add(buffer);
        });

        connection.open();
        connection.openEventHandler((result) -> {
            remoteOpened.set(true);
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

        assertTrue("Connection remote opened event did not fire", remoteOpened.get());

        ProtonSession session = connection.session();
        session.open();

        // Expect the engine to emit the Begin performative
        assertEquals("Engine did not emit an Begin performative after session was locally opened.", 3, engineWrites.size());
        outputBuffer = engineWrites.get(2);
        assertNotNull(unwrapFrame(outputBuffer, Begin.class));

        // Emit a remote Begin for this session
        Begin remoteBegin = new Begin();
        remoteBegin.setRemoteChannel(session.getLocalChannel());
        remoteBegin.setNextOutgoingId(1);
        remoteBegin.setIncomingWindow(0);
        remoteBegin.setOutgoingWindow(0);
        engine.ingest(wrapInFrame(remoteBegin, 1));

        return session;
    }
}
