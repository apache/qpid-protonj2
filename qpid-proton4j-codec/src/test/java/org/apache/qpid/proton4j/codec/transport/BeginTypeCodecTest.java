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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.BeginTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transport.BeginTypeEncoder;
import org.junit.Test;

public class BeginTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Begin.class, new BeginTypeDecoder().getTypeClass());
        assertEquals(Begin.class, new BeginTypeEncoder().getTypeClass());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
       Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};
       Map<Symbol, Object> properties = new HashMap<>();
       properties.put(Symbol.valueOf("property"), "value");

       Begin input = new Begin();

       input.setRemoteChannel(16);
       input.setNextOutgoingId(24);
       input.setIncomingWindow(32);
       input.setOutgoingWindow(12);
       input.setHandleMax(255);
       input.setOfferedCapabilities(offeredCapabilities);
       input.setDesiredCapabilities(desiredCapabilities);
       input.setProperties(properties);

       encoder.writeObject(buffer, encoderState, input);

       final Begin result = (Begin) decoder.readObject(buffer, decoderState);

       assertEquals(16, result.getRemoteChannel());
       assertEquals(24, result.getNextOutgoingId());
       assertEquals(32, result.getIncomingWindow());
       assertEquals(12, result.getOutgoingWindow());
       assertEquals(255, result.getHandleMax());
       assertNotNull(result.getProperties());
       assertEquals(1, properties.size());
       assertTrue(properties.containsKey(Symbol.valueOf("property")));
       assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
       assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }

    @Test
    public void testEncodeUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};
        Map<Symbol, Object> properties = new LinkedHashMap<>();
        properties.put(Symbol.valueOf("property"), "value");

        Begin input = new Begin();

        input.setRemoteChannel(16);
        input.setNextOutgoingId(24);
        input.setIncomingWindow(32);
        input.setOutgoingWindow(12);
        input.setHandleMax(255);
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setProperties(properties);

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Begin);
        final Begin result = (Begin) decoded;

        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("property"), "value");

        Begin input = new Begin();

        input.setRemoteChannel(16);
        input.setNextOutgoingId(24);
        input.setIncomingWindow(32);
        input.setOutgoingWindow(12);
        input.setHandleMax(255);
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setProperties(properties);

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        assertNotNull(buffer);

        final Begin result = (Begin) decoder.readObject(buffer, decoderState);
        assertNotNull(result);

        assertTypesEqual(input, result);
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Begin begin = new Begin();

        begin.setRemoteChannel(1);
        begin.setHandleMax(25);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, begin);
        }

        begin.setRemoteChannel(2);
        begin.setHandleMax(50);

        encoder.writeObject(buffer, encoderState, begin);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Begin.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Begin);

        Begin value = (Begin) result;
        assertEquals(2, value.getRemoteChannel());
        assertEquals(50, value.getHandleMax());
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
        buffer.writeByte(Begin.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Begin.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (IOException ex) {}
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
        buffer.writeByte(Begin.DESCRIPTOR_CODE.byteValue());
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
        } catch (IOException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Begin[] array = new Begin[3];

        array[0] = new Begin();
        array[1] = new Begin();
        array[2] = new Begin();

        array[0].setNextOutgoingId(0).setRemoteChannel(0).setIncomingWindow(0).setOutgoingWindow(0);
        array[1].setNextOutgoingId(1).setRemoteChannel(1).setIncomingWindow(1).setOutgoingWindow(1);
        array[2].setNextOutgoingId(2).setRemoteChannel(2).setIncomingWindow(2).setOutgoingWindow(2);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Begin.class, result.getClass().getComponentType());

        Begin[] resultArray = (Begin[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Begin);
            assertEquals(array[i].getNextOutgoingId(), resultArray[i].getNextOutgoingId());
            assertEquals(array[i].getOutgoingWindow(), resultArray[i].getOutgoingWindow());
            assertEquals(array[i].getIncomingWindow(), resultArray[i].getIncomingWindow());
            assertEquals(array[i].getRemoteChannel(), resultArray[i].getRemoteChannel());
        }
    }
}
