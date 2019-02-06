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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Connection;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for basic functionality of the ProtonEngine implementation.
 */
public class ProtonEngineTest {

    private Connection connection;
    private ArrayList<ProtonBuffer> engineWrites = new ArrayList<>();

    @After
    public void tearDown() {
        engineWrites.clear();
    }

    @Test
    public void testEngineStart() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        engine.start(result -> {
            assertTrue(result.succeeded());
            connection = result.get();
            assertNotNull(connection);
        });

        // Default engine should start and return a connection immediately
        assertTrue(engine.isWritable());
        assertNotNull(connection);
    }

    @Ignore("Building out idea of how Engine behaves, when do we emit the header etc")
    @Test
    public void testEngineEmitsAMQPHeaderOnConnectionOpen() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        engine.start(result -> {
            connection = result.get();
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        engine.outputHandler((buffer) -> {
            engineWrites.add(buffer);
        });

        connection.open();

        // Expect the engine to emit the AMQP header
        assertEquals("Engine did not emit an AMQP Header on Open", 1, engineWrites.size());

        ProtonBuffer outputBuffer = engineWrites.get(0);
        assertEquals(AMQPHeader.HEADER_SIZE_BYTES, outputBuffer.getReadableBytes());
        AMQPHeader outputHeader = new AMQPHeader(outputBuffer);

        assertFalse(outputHeader.isSaslHeader());
    }
}
