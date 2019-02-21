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

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.junit.Test;

/**
 * Tests for behaviors of the ProtonConnection class
 */
public class ProtonConnectionTest extends ProtonEngineTestSupport {

    private Connection connection;

    @Test
    public void testConnectionRemoteOpenTriggeredWhenRemoteOpenArrives() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        engine.start(result -> {
            connection = result.get();
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler((result) -> {
            remoteOpened.set(true);
        });

        engine.outputHandler((buffer) -> {
            engineWrites.add(buffer);
        });

        // Expect the engine to have remained idle
        assertEquals("Engine should not have emitted any frames yet", 0, engineWrites.size());

        engine.ingest(AMQPHeader.getAMQPHeader().getBuffer());
        engine.ingest(wrapInFrame(new Open(), 0));

        // Expect the engine to emit the AMQP header
        assertEquals("Engine did not emit an AMQP Header on Open", 1, engineWrites.size());

        assertTrue("Connection remote opened event did not fire", remoteOpened.get());
    }

    @Test
    public void testConnectionRemoteCloseTriggeredWhenRemoteCloseArrives() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        final AtomicBoolean connectionOpenedSignaled = new AtomicBoolean();
        final AtomicBoolean connectionClosedSignaled = new AtomicBoolean();

        engine.start(result -> {
            connection = result.get();
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler(result -> {
            connectionOpenedSignaled.set(true);
        });
        connection.closeEventHandler(result -> {
            connectionClosedSignaled.set(true);
        });

        engine.outputHandler((buffer) -> {
            engineWrites.add(buffer);
        });

        // Expect the engine to have remained idle
        assertEquals("Engine should not have emitted any frames yet", 0, engineWrites.size());

        engine.ingest(AMQPHeader.getAMQPHeader().getBuffer());
        engine.ingest(wrapInFrame(new Open(), 0));
        engine.ingest(wrapInFrame(new Close(), 0));

        // Expect the engine to emit the AMQP header
        assertEquals("Engine did not emit an AMQP Header on Open", 1, engineWrites.size());

        assertTrue("Connection remote opened event did not fire", connectionOpenedSignaled.get());
        assertTrue("Connection remote closed event did not fire", connectionClosedSignaled.get());
    }
}
