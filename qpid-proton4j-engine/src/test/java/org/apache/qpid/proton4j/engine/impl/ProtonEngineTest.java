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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.engine.Connection;
import org.junit.After;
import org.junit.Test;

/**
 * Test for basic functionality of the ProtonEngine implementation.
 */
public class ProtonEngineTest {

    // TODO - The engine testing would benefit from a Qpid JMS style TestPeer that can expect and
    //        emit frames to validate behaviors from the engine.

    private Connection connection;
    private ArrayList<ProtonBuffer> engineWrites = new ArrayList<>();

    private Decoder decoder = CodecFactory.getDefaultDecoder();
    private DecoderState decoderState = decoder.newDecoderState();

    @After
    public void tearDown() {
        engineWrites.clear();
        decoderState.reset();
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

    @Test
    public void testEngineEmitsOnConnectionOpenAfterHeaderReceived() throws IOException {
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

        engine.ingest(outputHeader.getBuffer());

        // Expect the engine to emit the Open performative
        assertEquals("Engine did not emit an Open performative after receiving header response", 2, engineWrites.size());
        outputBuffer = engineWrites.get(1);
        assertNotNull(unwrapFrame(outputBuffer, Open.class));
    }

    @SuppressWarnings({ "unchecked", "unused" })
    private <E> E unwrapFrame(ProtonBuffer buffer, Class<E> typeClass) throws IOException {
        int frameSize = buffer.readInt();
        int dataOffset = (buffer.readByte() << 2) & 0x3FF;
        int type = buffer.readByte() & 0xFF;
        short channel = buffer.readShort();
        if (dataOffset != 8) {
            buffer.setReadIndex(buffer.getReadIndex() + dataOffset - 8);
        }

        final int frameBodySize = frameSize - dataOffset;

        ProtonBuffer payload = null;
        Object val = null;

        if (frameBodySize > 0) {
            val = decoder.readObject(buffer, decoderState);
        } else {
            val = null;
        }

        return (E) val;
    }
}
