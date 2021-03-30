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
import java.io.InputStream;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.PropertiesTypeEncoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.UnsignedInteger;
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
        doTestDecodePropertiesSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfPropertiessFromStream() throws IOException {
        doTestDecodePropertiesSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiessStream() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE, true);
    }

    private void doTestDecodePropertiesSeries(int size, boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);

        final Random random = new Random();
        random.setSeed(System.nanoTime());

        final int randomGroupSequence = random.nextInt();
        final int randomAbsoluteExpiry = random.nextInt();
        final int randomCreateTime = random.nextInt();

        final Properties properties = new Properties();

        properties.setMessageId("ID:Message-1:1:1:0");
        properties.setUserId(new Binary(new byte[1]));
        properties.setTo("queue:work");
        properties.setSubject("help");
        properties.setReplyTo("queue:temp:me");
        properties.setContentEncoding("text/UTF-8");
        properties.setContentType("text");
        properties.setCorrelationId("correlation-id");
        properties.setAbsoluteExpiryTime(randomAbsoluteExpiry);
        properties.setCreationTime(randomCreateTime);
        properties.setGroupId("group-1");
        properties.setGroupSequence(randomGroupSequence);
        properties.setReplyToGroupId("group-1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof Properties);

            Properties decoded = (Properties) result;

            assertNotNull(decoded.getAbsoluteExpiryTime());
            assertEquals(Integer.toUnsignedLong(randomAbsoluteExpiry), decoded.getAbsoluteExpiryTime());
            assertEquals("text/UTF-8", decoded.getContentEncoding());
            assertEquals("text", decoded.getContentType());
            assertEquals("correlation-id", decoded.getCorrelationId());
            assertEquals(Integer.toUnsignedLong(randomCreateTime), decoded.getCreationTime());
            assertEquals("group-1", decoded.getGroupId());
            assertEquals(Integer.toUnsignedLong(randomGroupSequence), decoded.getGroupSequence());
            assertEquals("ID:Message-1:1:1:0", decoded.getMessageId());
            assertEquals("queue:temp:me", decoded.getReplyTo());
            assertEquals("group-1", decoded.getReplyToGroupId());
            assertEquals("help", decoded.getSubject());
            assertEquals("queue:work", decoded.getTo());
            assertTrue(decoded.getUserId() instanceof Binary);
        }
    }

    @Test
    public void testEncodeAndDecodeWithMaxUnsignedValuesFromLongs() throws IOException {
        doTestEncodeAndDecodeWithMaxUnsignedValuesFromLongs(false);
    }

    @Test
    public void testEncodeAndDecodeWithMaxUnsignedValuesFromLongsFromStream() throws IOException {
        doTestEncodeAndDecodeWithMaxUnsignedValuesFromLongs(true);
    }

    private void doTestEncodeAndDecodeWithMaxUnsignedValuesFromLongs(boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);
        final Properties properties = new Properties();

        properties.setAbsoluteExpiryTime(UnsignedInteger.MAX_VALUE.longValue());
        properties.setCreationTime(UnsignedInteger.MAX_VALUE.longValue());
        properties.setGroupSequence(UnsignedInteger.MAX_VALUE.longValue());

        encoder.writeObject(buffer, encoderState, properties);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Properties);

        Properties decoded = (Properties) result;

        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), decoded.getAbsoluteExpiryTime());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), decoded.getCreationTime());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), decoded.getGroupSequence());
    }

    @Test
    public void testSkipValue() throws IOException {
        doTestSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        doTestSkipValue(true);
    }

    public void doTestSkipValue(boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);

        Properties properties = new Properties();
        properties.setAbsoluteExpiryTime(100);
        properties.setContentEncoding("UTF8");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Properties.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Properties.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(buffer, decoderState);
            }
        }

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
    }

    @Test
    public void testDecodeWithInvalidMap32Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, false);
    }

    @Test
    public void testDecodeWithInvalidMap8Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, false);
    }

    @Test
    public void testDecodeWithInvalidMap32TypeFromStream() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, true);
    }

    @Test
    public void testDecodeWithInvalidMap8TypeFromStream() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, true);
    }

    private void doTestDecodeWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);

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

        if (fromStream) {
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("Should not decode type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("Should not decode type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testSkipValueWithInvalidMap32Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32, false);
    }

    @Test
    public void testSkipValueWithInvalidMap8Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8, false);
    }

    @Test
    public void testSkipValueWithInvalidMap32TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32, true);
    }

    @Test
    public void testSkipValueWithInvalidMap8TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8, true);
    }

    private void doTestSkipValueWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);

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

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Properties.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Properties.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        doTestEncodeDecodeArray(false);
    }

    @Test
    public void testEncodeDecodeArrayFromStream() throws IOException {
        doTestEncodeDecodeArray(true);
    }

    private void doTestEncodeDecodeArray(boolean fromStream) throws IOException {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);

        Properties[] array = new Properties[3];

        array[0] = new Properties();
        array[1] = new Properties();
        array[2] = new Properties();

        array[0].setAbsoluteExpiryTime(1);
        array[1].setAbsoluteExpiryTime(2);
        array[2].setAbsoluteExpiryTime(3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

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
