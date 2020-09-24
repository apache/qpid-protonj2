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
package org.apache.qpid.protonj2.codec.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Date;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.PropertiesTypeEncoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.junit.jupiter.api.Test;

/**
 * Test for decoder of AMQP Properties type.
 */
public class PropertiesTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Properties.class, new PropertiesTypeDecoder().getTypeClass());
        assertEquals(Properties.class, new PropertiesTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Properties.DESCRIPTOR_CODE, new PropertiesTypeDecoder().getDescriptorCode());
        assertEquals(Properties.DESCRIPTOR_CODE, new PropertiesTypeEncoder().getDescriptorCode());
        assertEquals(Properties.DESCRIPTOR_SYMBOL, new PropertiesTypeDecoder().getDescriptorSymbol());
        assertEquals(Properties.DESCRIPTOR_SYMBOL, new PropertiesTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeSmallSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);
    }

    private void doTestDecodePropertiesSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Properties properties = new Properties();

        Date timeNow = new Date(System.currentTimeMillis());

        properties.setMessageId("ID:Message-1:1:1:0");
        properties.setUserId(new Binary(new byte[1]));
        properties.setTo("queue:work");
        properties.setSubject("help");
        properties.setReplyTo("queue:temp:me");
        properties.setContentEncoding("text/UTF-8");
        properties.setContentType("text");
        properties.setCorrelationId("correlation-id");
        properties.setAbsoluteExpiryTime(timeNow.getTime());
        properties.setCreationTime(timeNow.getTime());
        properties.setGroupId("group-1");
        properties.setGroupSequence(1);
        properties.setReplyToGroupId("group-1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Properties);

            Properties decoded = (Properties) result;

            assertNotNull(decoded.getAbsoluteExpiryTime());
            assertEquals(timeNow.getTime(), decoded.getAbsoluteExpiryTime());
            assertEquals("text/UTF-8", decoded.getContentEncoding());
            assertEquals("text", decoded.getContentType());
            assertEquals("correlation-id", decoded.getCorrelationId());
            assertEquals(timeNow.getTime(), decoded.getCreationTime());
            assertEquals("group-1", decoded.getGroupId());
            assertEquals(1, decoded.getGroupSequence());
            assertEquals("ID:Message-1:1:1:0", decoded.getMessageId());
            assertEquals("queue:temp:me", decoded.getReplyTo());
            assertEquals("group-1", decoded.getReplyToGroupId());
            assertEquals("help", decoded.getSubject());
            assertEquals("queue:work", decoded.getTo());
            assertTrue(decoded.getUserId() instanceof Binary);
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(100);
        properties.setContentEncoding("UTF8");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Properties.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
    }

    @Test
    public void testDecodeWithInvalidMap32Type() throws IOException {
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
        buffer.writeByte(Properties.DESCRIPTOR_CODE.byteValue());
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
        buffer.writeByte(Properties.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Properties.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Properties[] array = new Properties[3];

        array[0] = new Properties();
        array[1] = new Properties();
        array[2] = new Properties();

        array[0].setAbsoluteExpiryTime(1);
        array[1].setAbsoluteExpiryTime(2);
        array[2].setAbsoluteExpiryTime(3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Properties.class, result.getClass().getComponentType());

        Properties[] resultArray = (Properties[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Properties);
            assertEquals(array[i].getAbsoluteExpiryTime(), resultArray[i].getAbsoluteExpiryTime());
        }
    }
}
