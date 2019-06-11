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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
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
}
