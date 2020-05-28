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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.HeaderTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.HeaderTypeEncoder;
import org.junit.Test;

/**
 * Test for decoder of AMQP Header type.
 */
public class HeaderTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Header.class, new HeaderTypeDecoder().getTypeClass());
        assertEquals(Header.class, new HeaderTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Header.DESCRIPTOR_CODE, new HeaderTypeDecoder().getDescriptorCode());
        assertEquals(Header.DESCRIPTOR_CODE, new HeaderTypeEncoder().getDescriptorCode());
        assertEquals(Header.DESCRIPTOR_SYMBOL, new HeaderTypeDecoder().getDescriptorSymbol());
        assertEquals(Header.DESCRIPTOR_SYMBOL, new HeaderTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeHeader() throws IOException {
        doTestDecodeHeaderSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority((byte) 3);
        header.setDeliveryCount(10);
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTimeToLive(500);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, header);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Header);

            Header decoded = (Header) result;

            assertEquals(3, decoded.getPriority());
            assertTrue(decoded.isDurable());
        }
    }

    @Test
    public void testEncodeDecodeZeroSizedArrayOfHeaders() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header[] headerArray = new Header[0];

        encoder.writeObject(buffer, encoderState, headerArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Header.class, result.getClass().getComponentType());

        Header[] resultArray = (Header[]) result;
        assertEquals(0, resultArray.length);
    }

    @Test
    public void testEncodeDecodeArrayOfHeaders() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header[] headerArray = new Header[3];

        headerArray[0] = new Header();
        headerArray[1] = new Header();
        headerArray[2] = new Header();

        headerArray[0].setDurable(true);
        headerArray[1].setDurable(true);
        headerArray[2].setDurable(true);

        encoder.writeObject(buffer, encoderState, headerArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Header.class, result.getClass().getComponentType());

        Header[] resultArray = (Header[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Header);
            assertEquals(headerArray[i].isDurable(), resultArray[i].isDurable());
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority((byte) 3);
        header.setDeliveryCount(10);
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTimeToLive(500);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new Header());
        }

        encoder.writeObject(buffer, encoderState, header);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Header);

        Header value = (Header) result;
        assertEquals(3, value.getPriority());
        assertTrue(value.isDurable());
        assertFalse(value.isFirstAcquirer());
        assertEquals(500, header.getTimeToLive());
        assertEquals(10, header.getDeliveryCount());
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
        buffer.writeByte(Header.DESCRIPTOR_CODE.byteValue());
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
        buffer.writeByte(Header.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Header.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header[] array = new Header[3];

        array[0] = new Header();
        array[1] = new Header();
        array[2] = new Header();

        array[0].setPriority((byte) 1).setDeliveryCount(1);
        array[1].setPriority((byte) 2).setDeliveryCount(2);
        array[2].setPriority((byte) 3).setDeliveryCount(3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Header.class, result.getClass().getComponentType());

        Header[] resultArray = (Header[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Header);
            assertEquals(array[i].getPriority(), resultArray[i].getPriority());
            assertEquals(array[i].getDeliveryCount(), resultArray[i].getDeliveryCount());
        }
    }
}
