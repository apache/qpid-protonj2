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
package org.apache.qpid.proton4j.codec.transport;

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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class OpenTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setChannelMax(UnsignedShort.valueOf(512).intValue());
        input.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        input.setIdleTimeOut(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertEquals("test", result.getContainerId());
        assertEquals("localhost", result.getHostname());
        assertEquals(UnsignedShort.valueOf(512).intValue(), result.getChannelMax());
        assertEquals(UnsignedInteger.ONE.longValue(), result.getMaxFrameSize());
        assertEquals(UnsignedInteger.ZERO.longValue(), result.getIdleTimeOut());
        assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
        assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }

    @Test
    public void testEncodeUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        input.setIdleTimeOut(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Open);
        final Open result = (Open) decoded;

        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        input.setIdleTimeOut(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        assertNotNull(buffer);

        final Open result = (Open) decoder.readObject(buffer, decoderState);
        assertNotNull(result);

        assertTypesEqual(input, result);
    }

    @Test
    public void testToStringWhenEmptyNoNPE() {
        Open open = new Open();
        assertNotNull(open.toString());
    }

    @Test
    public void testPerformativeType() {
        Open open = new Open();
        assertEquals(Performative.PerformativeType.OPEN, open.getPerformativeType());
    }

    @Test
    public void testIsEmpty() {
        Open open = new Open();

        // Open defaults to an empty string container ID so not empty
        assertFalse(open.isEmpty());
        open.setMaxFrameSize(1024);
        assertFalse(open.isEmpty());
    }

    @Test
    public void testContainerId() {
        Open open = new Open();

        // Open defaults to an empty string container ID
        assertTrue(open.hasContainerId());
        assertEquals("", open.getContainerId());
        open.setContainerId("test");
        assertTrue(open.hasContainerId());
        assertEquals("test", open.getContainerId());

        try {
            open.setContainerId(null);
            fail("Should not be able to set a null container id for mandatory field.");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testHostname() {
        Open open = new Open();

        assertFalse(open.hasHostname());
        assertNull(open.getHostname());
        open.setHostname("localhost");
        assertTrue(open.hasHostname());
        assertEquals("localhost", open.getHostname());
        open.setHostname(null);
        assertFalse(open.hasHostname());
        assertNull(open.getHostname());
    }

    @Test
    public void testMaxFrameSize() {
        Open open = new Open();

        assertFalse(open.hasMaxFrameSize());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), open.getMaxFrameSize());
        open.setMaxFrameSize(1024);
        assertTrue(open.hasMaxFrameSize());
        assertEquals(1024, open.getMaxFrameSize());
    }

    @Test
    public void testChannelMax() {
        Open open = new Open();

        assertFalse(open.hasChannelMax());
        assertEquals(UnsignedShort.MAX_VALUE.longValue(), open.getChannelMax());
        open.setChannelMax(1024);
        assertTrue(open.hasChannelMax());
        assertEquals(1024, open.getChannelMax());
    }

    @Test
    public void testIdleTimeout() {
        Open open = new Open();

        assertFalse(open.hasIdleTimeout());
        assertEquals(0, open.getIdleTimeOut());
        open.setIdleTimeOut(1024);
        assertTrue(open.hasIdleTimeout());
        assertEquals(1024, open.getIdleTimeOut());
    }

    @Test
    public void testOutgoingLocales() {
        Open open = new Open();

        assertFalse(open.hasOutgoingLocales());
        assertNull(open.getOutgoingLocales());
        open.setOutgoingLocales(Symbol.valueOf("test"));
        assertTrue(open.hasOutgoingLocales());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("test") }, open.getOutgoingLocales());
        open.setOutgoingLocales((Symbol[]) null);
        assertFalse(open.hasDesiredCapabilites());
        assertNull(open.getDesiredCapabilities());
    }

    @Test
    public void testIncomingLocales() {
        Open open = new Open();

        assertFalse(open.hasIncomingLocales());
        assertNull(open.getIncomingLocales());
        open.setIncomingLocales(Symbol.valueOf("test"));
        assertTrue(open.hasIncomingLocales());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("test") }, open.getIncomingLocales());
        open.setIncomingLocales((Symbol[]) null);
        assertFalse(open.hasDesiredCapabilites());
        assertNull(open.getDesiredCapabilities());
    }

    @Test
    public void testOfferedCapabilities() {
        Open open = new Open();

        assertFalse(open.hasOfferedCapabilites());
        assertNull(open.getOfferedCapabilities());
        open.setOfferedCapabilities(Symbol.valueOf("test"));
        assertTrue(open.hasOfferedCapabilites());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("test") }, open.getOfferedCapabilities());
        open.setOfferedCapabilities((Symbol[]) null);
        assertFalse(open.hasDesiredCapabilites());
        assertNull(open.getDesiredCapabilities());
    }

    @Test
    public void testDesiredCapabilities() {
        Open open = new Open();

        assertFalse(open.hasDesiredCapabilites());
        assertNull(open.getDesiredCapabilities());
        open.setDesiredCapabilities(Symbol.valueOf("test"));
        assertTrue(open.hasDesiredCapabilites());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("test") }, open.getDesiredCapabilities());
        open.setDesiredCapabilities((Symbol[]) null);
        assertFalse(open.hasDesiredCapabilites());
        assertNull(open.getDesiredCapabilities());
    }

    @Test
    public void testProperties() {
        Open open = new Open();

        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), Boolean.FALSE);

        assertFalse(open.hasProperties());
        assertNull(open.getProperties());
        open.setProperties(properties);
        assertTrue(open.hasProperties());
        assertEquals(properties, open.getProperties());
        open.setProperties(null);
        assertFalse(open.hasProperties());
        assertNull(open.getProperties());
    }

    @Test
    public void testCopy() {
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("test"), Boolean.FALSE);

        Symbol outgoingLocale = Symbol.valueOf("outgoing");
        Symbol incomingLocale = Symbol.valueOf("incoming");
        Symbol offeredCapability = Symbol.valueOf("offered");
        Symbol desiredCapability = Symbol.valueOf("desired");

        Open open = new Open();

        open.setContainerId("test");
        open.setHostname("localhost");
        open.setMaxFrameSize(1024);
        open.setChannelMax(64);
        open.setIdleTimeOut(360000);
        open.setOutgoingLocales(outgoingLocale);
        open.setIncomingLocales(incomingLocale);
        open.setOfferedCapabilities(offeredCapability);
        open.setDesiredCapabilities(desiredCapability);
        open.setProperties(properties);

        Open copy = open.copy();

        assertNotNull(copy);

        assertEquals("test", open.getContainerId());
        assertEquals("localhost", open.getHostname());
        assertEquals(1024, open.getMaxFrameSize());
        assertEquals(64, open.getChannelMax());
        assertEquals(360000, open.getIdleTimeOut());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("outgoing") }, open.getOutgoingLocales());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("incoming") }, open.getIncomingLocales());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("offered") }, open.getOfferedCapabilities());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("desired") }, open.getDesiredCapabilities());
        assertEquals(properties, open.getProperties());

        open.setOutgoingLocales((Symbol[]) null);
        open.setIncomingLocales((Symbol[]) null);
        open.setOfferedCapabilities((Symbol[]) null);
        open.setDesiredCapabilities((Symbol[]) null);
        open.setProperties(null);

        copy = open.copy();

        assertNotNull(copy);

        assertEquals("test", open.getContainerId());
        assertEquals("localhost", open.getHostname());
        assertEquals(1024, open.getMaxFrameSize());
        assertEquals(64, open.getChannelMax());
        assertEquals(360000, open.getIdleTimeOut());
        assertNull(open.getOutgoingLocales());
        assertNull(open.getIncomingLocales());
        assertNull(open.getOfferedCapabilities());
        assertNull(open.getDesiredCapabilities());
        assertNull(open.getProperties());
    }
}
