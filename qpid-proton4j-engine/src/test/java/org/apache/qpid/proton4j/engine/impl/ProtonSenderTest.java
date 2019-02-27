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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.junit.Test;

/**
 * Test the {@link ProtonSender}
 */
public class ProtonSenderTest extends ProtonEngineTestSupport {

    protected Connection connection;

    @Test
    public void testEngineEmitsAttachAfterLocalSenderOpened() throws IOException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        engine.start(result -> {
            connection = result.get();
        });

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

        Session session = connection.session();
        session.open();

        // Expect the engine to emit the Begin performative
        assertEquals("Engine did not emit an Begin performative after session was locally opened.", 3, engineWrites.size());
        outputBuffer = engineWrites.get(2);
        assertNotNull(unwrapFrame(outputBuffer, Begin.class));

        Sender sender = session.sender("test");
        sender.open();

        // Expect the engine to emit the Attach performative
        assertEquals("Engine did not emit an Attach performative after sender was locally opened.", 4, engineWrites.size());
        outputBuffer = engineWrites.get(3);
        assertNotNull(unwrapFrame(outputBuffer, Attach.class));

        sender.close();

        // Expect the engine to emit the Detach performative
        assertEquals("Engine did not emit an Detach performative after sender was locally closed.", 5, engineWrites.size());
        outputBuffer = engineWrites.get(4);
        assertNotNull(unwrapFrame(outputBuffer, Detach.class));
    }
}
