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
package org.apache.qpid.protonj2.test.driver.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.SenderSettleMode;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test some basic operations of the Data type codec
 */
public class DataImplTest {

    private final Codec codec = Codec.Factory.create();

    @Test
    public void testDecodeOpen() {
        Open open = new Open();
        open.setContainerId("test");
        open.setHostname("localhost");

        ByteBuf encoded = encodeProtonPerformative(open);
        int expectedRead = encoded.readableBytes();

        Codec codec = Codec.Factory.create();

        assertEquals(expectedRead, codec.decode(encoded));

        Open described = (Open) codec.getDescribedType();
        assertNotNull(described);
        assertEquals(Open.DESCRIPTOR_SYMBOL, described.getDescriptor());

        assertEquals(open.getContainerId(), described.getContainerId());
        assertEquals(open.getHostname(), described.getHostname());
    }

    @Test
    public void testEncodeOpen() throws IOException {
        Open open =new Open();
        open.setContainerId("test");
        open.setHostname("localhost");

        Codec codec = Codec.Factory.create();

        codec.putDescribedType(open);
        ByteBuf encoded = Unpooled.buffer((int) codec.encodedSize());
        codec.encode(encoded);

        DescribedType decoded = decodeProtonPerformative(encoded);
        assertNotNull(decoded);
        assertTrue(decoded instanceof Open);

        Open performative = (Open) decoded;
        assertEquals(open.getContainerId(), performative.getContainerId());
        assertEquals(open.getHostname(), performative.getHostname());
    }

    @Test
    public void testDecodeBegin() {
        Begin begin = new Begin();
        begin.setHandleMax(UnsignedInteger.valueOf(512));
        begin.setRemoteChannel(UnsignedShort.valueOf(1));

        ByteBuf encoded = encodeProtonPerformative(begin);
        int expectedRead = encoded.readableBytes();

        Codec codec = Codec.Factory.create();

        assertEquals(expectedRead, codec.decode(encoded));

        Begin described = (Begin) codec.getDescribedType();
        assertNotNull(described);
        assertEquals(Begin.DESCRIPTOR_SYMBOL, described.getDescriptor());

        assertEquals(described.getHandleMax(), UnsignedInteger.valueOf(512));
        assertEquals(described.getRemoteChannel(), UnsignedShort.valueOf((short) 1));
    }

    @Test
    public void testEncodeBegin() throws IOException {
        Begin begin = new Begin();
        begin.setHandleMax(UnsignedInteger.valueOf(512));
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 1));
        begin.setIncomingWindow(UnsignedInteger.valueOf(2));
        begin.setNextOutgoingId(UnsignedInteger.valueOf(2));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(3));

        Codec codec = Codec.Factory.create();

        codec.putDescribedType(begin);
        ByteBuf encoded = Unpooled.buffer((int) codec.encodedSize());
        codec.encode(encoded);

        DescribedType decoded = decodeProtonPerformative(encoded);
        assertNotNull(decoded);
        assertTrue(decoded instanceof Begin);

        Begin performative = (Begin) decoded;
        assertEquals(performative.getHandleMax(), UnsignedInteger.valueOf(512));
        assertEquals(performative.getRemoteChannel(), UnsignedShort.valueOf((short) 1));
    }

    @Test
    public void testDecodeAttach() {
        Attach attach = new Attach();
        attach.setName("test");
        attach.setHandle(UnsignedInteger.valueOf(1));
        attach.setRole(Role.SENDER);
        attach.setSenderSettleMode(SenderSettleMode.MIXED);
        attach.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        attach.setSource(new Source());
        attach.setTarget(new Target());

        ByteBuf encoded = encodeProtonPerformative(attach);
        int expectedRead = encoded.readableBytes();

        Codec codec = Codec.Factory.create();

        assertEquals(expectedRead, codec.decode(encoded));

        Attach described = (Attach) codec.getDescribedType();
        assertNotNull(described);
        assertEquals(Attach.DESCRIPTOR_SYMBOL, described.getDescriptor());

        assertEquals(described.getHandle(), UnsignedInteger.valueOf(1));
        assertEquals(described.getName(), "test");
    }

    @Test
    public void testEncodeAttach() throws IOException {
        Attach attach = new Attach();
        attach.setName("test");
        attach.setHandle(UnsignedInteger.valueOf(1));
        attach.setRole(Role.SENDER.getValue());
        attach.setSenderSettleMode(SenderSettleMode.MIXED.getValue());
        attach.setReceiverSettleMode(ReceiverSettleMode.FIRST.getValue());
        attach.setSource(new Source());
        attach.setTarget(new Target());

        Codec codec = Codec.Factory.create();

        codec.putDescribedType(attach);
        ByteBuf encoded = Unpooled.buffer((int) codec.encodedSize());
        codec.encode(encoded);

        DescribedType decoded = decodeProtonPerformative(encoded);
        assertNotNull(decoded);
        assertTrue(decoded instanceof Attach);

        Attach performative = (Attach) decoded;
        assertEquals(performative.getHandle(), UnsignedInteger.valueOf(1));
        assertEquals(performative.getName(), "test");
    }

    @Test
    public void testDecodeOfComplexOpenEncoding() throws IOException {
        // Frame data for: Open
        //   Open{ containerId='container', hostname='localhost', maxFrameSize=16384, channelMax=65535,
        //         idleTimeOut=36000, outgoingLocales=null, incomingLocales=null, offeredCapabilities=[SOMETHING],
        //         desiredCapabilities=[ANONYMOUS-RELAY, DELAYED-DELIVERY], properties={queue-prefix=queue://}}
        final byte[] completeOpen = new byte[] {0, 83, 16, -64, 116, 10, -95, 9, 99, 111, 110, 116, 97, 105, 110,
                                                101, 114, -95, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 112,
                                                0, 0, 64, 0, 96, -1, -1, 112, 0, 0, -116, -96, 64, 64, -32, 12, 1,
                                                -93, 9, 83, 79, 77, 69, 84, 72, 73, 78, 71, -32, 35, 2, -93, 15,
                                                65, 78, 79, 78, 89, 77, 79, 85, 83, 45, 82, 69, 76, 65, 89, 16, 68,
                                                69, 76, 65, 89, 69, 68, 45, 68, 69, 76, 73, 86, 69, 82, 89, -63, 25,
                                                2, -95, 12, 113, 117, 101, 117, 101, 45, 112, 114, 101, 102, 105,
                                                120, -95, 8, 113, 117, 101, 117, 101, 58, 47, 47};

        ByteBuf encoded = Unpooled.wrappedBuffer(completeOpen);

        DescribedType decoded = decodeProtonPerformative(encoded);
        assertNotNull(decoded);
        assertTrue(decoded instanceof Open);

        Open performative = (Open) decoded;

        assertEquals("container", performative.getContainerId());
        assertEquals("localhost", performative.getHostname());
        assertEquals(UnsignedInteger.valueOf(16384), performative.getMaxFrameSize());
        assertEquals(UnsignedInteger.valueOf(36000), performative.getIdleTimeOut());
        assertEquals(UnsignedShort.valueOf(65535), performative.getChannelMax());

        Symbol[] offered = performative.getOfferedCapabilities();
        Symbol[] desired = performative.getDesiredCapabilities();

        assertArrayEquals(new Symbol[] { Symbol.valueOf("SOMETHING") }, offered);
        assertArrayEquals(new Symbol[] { Symbol.valueOf("ANONYMOUS-RELAY"), Symbol.valueOf("DELAYED-DELIVERY") }, desired);

        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("queue-prefix", "queue://");

        assertEquals(expected, performative.getProperties());
    }

    private DescribedType decodeProtonPerformative(ByteBuf buffer) throws IOException {
        DescribedType performative = null;

        try {
            codec.decode(buffer);
        } catch (Exception e) {
            throw new AssertionError("Decoder failed reading remote input:", e);
        }

        Codec.DataType dataType = codec.type();
        if (dataType != Codec.DataType.DESCRIBED) {
            throw new IllegalArgumentException(
                "Frame body type expected to be " + Codec.DataType.DESCRIBED + " but was: " + dataType);
        }

        try {
            performative = codec.getDescribedType();
        } finally {
            codec.clear();
        }

        return performative;
    }

    private ByteBuf encodeProtonPerformative(DescribedType performative) {
        ByteBuf buffer = Unpooled.buffer();

        if (performative != null) {
            try {
                codec.putDescribedType(performative);
                codec.encode(buffer);
            } finally {
                codec.clear();
            }
        }

        return buffer;
    }
}
