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
package org.apache.qpid.proton4j.codec.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.FooterTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.FooterTypeEncoder;
import org.junit.Test;

public class FooterTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Footer.class, new FooterTypeEncoder().getTypeClass());
        assertEquals(Footer.class, new FooterTypeDecoder().getTypeClass());
    }

    @Test
    public void testDecodeSmallSeriesOfFooter() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfFooter() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Object, Object> propertiesMap = new LinkedHashMap<>();
        Footer properties = new Footer(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Footer);

            Footer decoded = (Footer) result;

            assertEquals(8, decoded.getValue().size());
            assertTrue(decoded.getValue().equals(propertiesMap));
        }
    }

    @Test
    public void testDecodeFailsWhenDescriedValueIsNotMapType() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(0);
        buffer.writeInt(0);

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (IOException ex) {}
    }

    @Test
    public void testDecodeWithNullBodyUsingDescriptorCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        final Footer result = (Footer) decoder.readObject(buffer, decoderState);

        assertNull(result.getValue());
    }

    @Test
    public void testDecodeWithNullBodyUsingDescriptorSymbol() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SYM8);
        buffer.writeByte(Footer.DESCRIPTOR_SYMBOL.getLength());
        Footer.DESCRIPTOR_SYMBOL.writeTo(buffer);
        buffer.writeByte(EncodingCodes.NULL);

        final Footer result = (Footer) decoder.readObject(buffer, decoderState);

        assertNull(result.getValue());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Object, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("one"), 1);
        map.put(Symbol.valueOf("two"), Boolean.TRUE);
        map.put(Symbol.valueOf("three"), "test");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new Footer(map));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Footer.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.getUndeliverableHere());
        assertFalse(modified.getDeliveryFailed());
    }
}
