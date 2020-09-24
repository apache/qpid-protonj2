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
package org.apache.qpid.protonj2.codec.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.OpenTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.OpenTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.transport.Open;
import org.apache.qpid.protonj2.types.transport.Performative;
import org.junit.jupiter.api.Test;

public class OpenTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Open.class, new OpenTypeDecoder().getTypeClass());
        assertEquals(Open.class, new OpenTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Open.DESCRIPTOR_CODE, new OpenTypeDecoder().getDescriptorCode());
        assertEquals(Open.DESCRIPTOR_CODE, new OpenTypeEncoder().getDescriptorCode());
        assertEquals(Open.DESCRIPTOR_SYMBOL, new OpenTypeDecoder().getDescriptorSymbol());
        assertEquals(Open.DESCRIPTOR_SYMBOL, new OpenTypeEncoder().getDescriptorSymbol());
    }

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
        input.setIdleTimeout(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertEquals("test", result.getContainerId());
        assertEquals("localhost", result.getHostname());
        assertEquals(UnsignedShort.valueOf(512).intValue(), result.getChannelMax());
        assertEquals(UnsignedInteger.ONE.longValue(), result.getMaxFrameSize());
        assertEquals(UnsignedInteger.ZERO.longValue(), result.getIdleTimeout());
        assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
        assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }

    @Test
    public void testOpenEncodesDefaultMaxFrameSizeWhenSet() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final Open input = new Open();

        encoder.writeObject(buffer, encoderState, input);

        final Open resultWithDefault = (Open) decoder.readObject(buffer, decoderState);

        assertFalse(resultWithDefault.hasMaxFrameSize());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), resultWithDefault.getMaxFrameSize());

        input.setMaxFrameSize(UnsignedInteger.MAX_VALUE.longValue());

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertTrue(result.hasMaxFrameSize());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), result.getMaxFrameSize());
    }

    @Test
    public void testOpenEncodesDefaultIdleTimeoutWhenSet() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final Open input = new Open();

        encoder.writeObject(buffer, encoderState, input);

        final Open resultWithDefault = (Open) decoder.readObject(buffer, decoderState);

        assertFalse(resultWithDefault.hasIdleTimeout());
        assertEquals(UnsignedInteger.ZERO.longValue(), resultWithDefault.getIdleTimeout());

        input.setIdleTimeout(UnsignedInteger.ZERO.longValue());

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertTrue(result.hasIdleTimeout());
        assertEquals(UnsignedInteger.ZERO.longValue(), result.getIdleTimeout());
    }

    @Test
    public void testOpenEncodesDefaultChannelMaxWhenSet() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final Open input = new Open();

        encoder.writeObject(buffer, encoderState, input);

        final Open resultWithDefault = (Open) decoder.readObject(buffer, decoderState);

        assertFalse(resultWithDefault.hasChannelMax());
        assertEquals(UnsignedShort.MAX_VALUE.intValue(), resultWithDefault.getChannelMax());

        input.setChannelMax(UnsignedShort.MAX_VALUE.intValue());

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertTrue(result.hasChannelMax());
        assertEquals(UnsignedShort.MAX_VALUE.intValue(), result.getChannelMax());
    }

    @Test
    public void testEncodeAndDecodeOpenWithMaxMaxFrameSize() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setChannelMax(UnsignedShort.MAX_VALUE.intValue());
        input.setMaxFrameSize(UnsignedInteger.MAX_VALUE.longValue());

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertEquals("test", result.getContainerId());
        assertEquals("localhost", result.getHostname());
        assertEquals(UnsignedShort.MAX_VALUE.intValue(), result.getChannelMax());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), result.getMaxFrameSize());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Open close = new Open();

        close.setContainerId("skip");
        close.setHostname("google");
        close.setChannelMax(UnsignedShort.valueOf(256).intValue());
        close.setMaxFrameSize(UnsignedInteger.ZERO.longValue());
        close.setIdleTimeout(UnsignedInteger.ONE.longValue());

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, close);
        }

        close.setContainerId("test");
        close.setHostname("localhost");
        close.setChannelMax(UnsignedShort.valueOf(512).intValue());
        close.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        close.setIdleTimeout(UnsignedInteger.ZERO.longValue());

        encoder.writeObject(buffer, encoderState, close);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Open.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Open);

        Open value = (Open) result;
        assertEquals("test", value.getContainerId());
        assertEquals("localhost", value.getHostname());
        assertEquals(UnsignedShort.valueOf(512).intValue(), value.getChannelMax());
        assertEquals(UnsignedInteger.ONE.longValue(), value.getMaxFrameSize());
        assertEquals(UnsignedInteger.ZERO.longValue(), value.getIdleTimeout());
        assertNull(value.getOfferedCapabilities());
        assertNull(value.getDesiredCapabilities());
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
        input.setIdleTimeout(UnsignedInteger.ZERO.longValue());
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
        input.setMaxFrameSize(UnsignedInteger.MAX_VALUE.longValue());
        input.setIdleTimeout(UnsignedInteger.ZERO.longValue());
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
        assertEquals(0, open.getIdleTimeout());
        open.setIdleTimeout(1024);
        assertTrue(open.hasIdleTimeout());
        assertEquals(1024, open.getIdleTimeout());
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
        open.setIdleTimeout(360000);
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
        assertEquals(360000, open.getIdleTimeout());
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
        assertEquals(360000, open.getIdleTimeout());
        assertNull(open.getOutgoingLocales());
        assertNull(open.getIncomingLocales());
        assertNull(open.getOfferedCapabilities());
        assertNull(open.getDesiredCapabilities());
        assertNull(open.getProperties());
    }

    @Test
    public void testSkipValueWithInvalidMap32Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32);
    }

    @Test
    public void testSkipValueWithInvalidMap8Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8);
    }

    private void doTestSkipValueWithInvalidMapType(byte mapType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Open.DESCRIPTOR_CODE.byteValue());
        if (mapType == EncodingCodes.MAP32) {
            buffer.writeByte(EncodingCodes.MAP32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.MAP8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        }

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(Open.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testDecodedWithInvalidMap32Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32);
    }

    @Test
    public void testDecodeWithInvalidMap8Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8);
    }

    private void doTestDecodeWithInvalidMapType(byte mapType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Open.DESCRIPTOR_CODE.byteValue());
        if (mapType == EncodingCodes.MAP32) {
            buffer.writeByte(EncodingCodes.MAP32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.MAP8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        }

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Open[] array = new Open[3];

        array[0] = new Open();
        array[1] = new Open();
        array[2] = new Open();

        array[0].setHostname("1").setIdleTimeout(1).setMaxFrameSize(1);
        array[1].setHostname("2").setIdleTimeout(2).setMaxFrameSize(2);
        array[2].setHostname("3").setIdleTimeout(3).setMaxFrameSize(3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Open.class, result.getClass().getComponentType());

        Open[] resultArray = (Open[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Open);
            assertEquals(array[i].getHostname(), resultArray[i].getHostname());
            assertEquals(array[i].getIdleTimeout(), resultArray[i].getIdleTimeout());
            assertEquals(array[i].getMaxFrameSize(), resultArray[i].getMaxFrameSize());
        }
    }
}
