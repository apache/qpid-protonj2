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
package org.apache.qpid.proton4j.driver.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.codec.Data;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.junit.Test;

/**
 * Test some basic operations of the Data type codec
 */
public class DataImplTest {

    private final Encoder encoder = CodecFactory.getDefaultEncoder();
    private final EncoderState encoderState = encoder.newEncoderState();

    private final Decoder decoder = CodecFactory.getDefaultDecoder();
    private final DecoderState decoderState = decoder.newDecoderState();

    @Test
    public void testDecodeOpen() {
        Open open = new Open();
        open.setContainerId("test");
        open.setHostname("localhost");

        ProtonBuffer encoded = encodeProtonPerformative(open);
        int expectedRead = encoded.getReadableBytes();

        Data codec = Data.Factory.create();

        assertEquals(expectedRead, codec.decode(encoded));

        org.apache.qpid.proton4j.amqp.driver.codec.types.Open described =
            (org.apache.qpid.proton4j.amqp.driver.codec.types.Open) codec.getDescribedType();
        assertNotNull(described);
        assertEquals(Open.DESCRIPTOR_CODE, described.getDescriptor());

        assertEquals(open.getContainerId(), described.getContainerId());
        assertEquals(open.getHostname(), described.getHostname());
    }

    @Test
    public void testEncodeOpen() throws IOException {
        org.apache.qpid.proton4j.amqp.driver.codec.types.Open open =
            new org.apache.qpid.proton4j.amqp.driver.codec.types.Open();
        open.setContainerId("test");
        open.setHostname("localhost");

        Data codec = Data.Factory.create();

        codec.putDescribedType(open);
        ProtonBuffer encoded = ProtonByteBufferAllocator.DEFAULT.allocate((int) codec.encodedSize());
        codec.encode(encoded);

        Performative decoded = decodeProtonPerformative(encoded);
        assertNotNull(decoded);
        assertTrue(decoded instanceof Open);

        Open performative = (Open) decoded;
        assertEquals(open.getContainerId(), performative.getContainerId());
        assertEquals(open.getHostname(), performative.getHostname());
    }

    @Test
    public void testDecodeBegin() {
        Begin begin = new Begin();
        begin.setHandleMax(512);
        begin.setRemoteChannel(1);

        ProtonBuffer encoded = encodeProtonPerformative(begin);
        int expectedRead = encoded.getReadableBytes();

        Data codec = Data.Factory.create();

        assertEquals(expectedRead, codec.decode(encoded));

        org.apache.qpid.proton4j.amqp.driver.codec.types.Begin described =
            (org.apache.qpid.proton4j.amqp.driver.codec.types.Begin) codec.getDescribedType();
        assertNotNull(described);
        assertEquals(Begin.DESCRIPTOR_CODE, described.getDescriptor());

        assertEquals(described.getHandleMax(), UnsignedInteger.valueOf(512));
        assertEquals(described.getRemoteChannel(), UnsignedShort.valueOf((short) 1));
    }

    @Test
    public void testEncodeBegin() throws IOException {
        org.apache.qpid.proton4j.amqp.driver.codec.types.Begin begin =
            new org.apache.qpid.proton4j.amqp.driver.codec.types.Begin();
        begin.setHandleMax(UnsignedInteger.valueOf(512));
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 1));
        begin.setIncomingWindow(UnsignedInteger.valueOf(2));
        begin.setNextOutgoingId(UnsignedInteger.valueOf(2));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(3));

        Data codec = Data.Factory.create();

        codec.putDescribedType(begin);
        ProtonBuffer encoded = ProtonByteBufferAllocator.DEFAULT.allocate((int) codec.encodedSize());
        codec.encode(encoded);

        Performative decoded = decodeProtonPerformative(encoded);
        assertNotNull(decoded);
        assertTrue(decoded instanceof Begin);

        Begin performative = (Begin) decoded;
        assertEquals(performative.getHandleMax(), 512);
        assertEquals(performative.getRemoteChannel(), 1);
    }

    private Performative decodeProtonPerformative(ProtonBuffer buffer) throws IOException {
        Performative performative = null;

        try {
            performative = (Performative) decoder.readObject(buffer, decoderState);
        } finally {
            decoderState.reset();
        }

        return performative;
    }

    private ProtonBuffer encodeProtonPerformative(Performative performative) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        try {
            encoder.writeObject(buffer, encoderState, performative);
        } finally {
            encoderState.reset();
        }

        return buffer;
    }
}
