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
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.HeaderTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.HeaderTypeEncoder;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.junit.jupiter.api.Test;

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
        doTestDecodeHeaderSeries(1, false);
    }

    @Test
    public void testDecodeSmallSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeHeaderFromStream() throws IOException {
        doTestDecodeHeaderSeries(1, true);
    }

    @Test
    public void testDecodeSmallSeriesOfHeadersFromStream() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfHeadersFromStream() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeHeaderSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Header header = new Header();

        final Random random = new Random();
        random.setSeed(System.nanoTime());

        final int randomDeliveryCount = random.nextInt();
        final int randomTimeToLive = random.nextInt();

        header.setDurable(Boolean.TRUE);
        header.setPriority((byte) 3);
        header.setDeliveryCount(randomDeliveryCount);
        header.setFirstAcquirer(Boolean.TRUE);
        header.setTimeToLive(randomTimeToLive);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, header);
        }

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof Header);

            Header decoded = (Header) result;

            assertEquals(3, decoded.getPriority());
            assertEquals(Integer.toUnsignedLong(randomTimeToLive), decoded.getTimeToLive());
            assertEquals(Integer.toUnsignedLong(randomDeliveryCount), decoded.getDeliveryCount());
            assertTrue(decoded.isDurable());
            assertTrue(decoded.isFirstAcquirer());
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
        final ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
        final Header header = new Header();

        header.setDeliveryCount(UnsignedInteger.MAX_VALUE.longValue());
        header.setTimeToLive(UnsignedInteger.MAX_VALUE.longValue());

        encoder.writeObject(buffer, encoderState, header);

        final Object result;
        if (fromStream) {
            final InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Header);

        Header decoded = (Header) result;

        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), decoded.getDeliveryCount());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), decoded.getTimeToLive());
    }

    @Test
    public void testEncodeDecodeZeroSizedArrayOfHeaders() throws IOException {
        dotestEncodeDecodeZeroSizedArrayOfHeaders(false);
    }

    @Test
    public void testEncodeDecodeZeroSizedArrayOfHeadersFromStream() throws IOException {
        dotestEncodeDecodeZeroSizedArrayOfHeaders(true);
    }

    private void dotestEncodeDecodeZeroSizedArrayOfHeaders(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Header[] headerArray = new Header[0];

        encoder.writeObject(buffer, encoderState, headerArray);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Header.class, result.getClass().getComponentType());

        Header[] resultArray = (Header[]) result;
        assertEquals(0, resultArray.length);
    }

    @Test
    public void testEncodeDecodeArrayOfHeaders() throws IOException {
        doTestEncodeDecodeArrayOfHeaders(false);
    }

    @Test
    public void testEncodeDecodeArrayOfHeadersFromStream() throws IOException {
        doTestEncodeDecodeArrayOfHeaders(true);
    }

    private void doTestEncodeDecodeArrayOfHeaders(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Header[] headerArray = new Header[3];

        headerArray[0] = new Header();
        headerArray[1] = new Header();
        headerArray[2] = new Header();

        headerArray[0].setDurable(true);
        headerArray[1].setDurable(true);
        headerArray[2].setDurable(true);

        encoder.writeObject(buffer, encoderState, headerArray);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

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
        doTestSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        doTestSkipValue(true);
    }

    private void doTestSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Header.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Header.class, typeDecoder.getTypeClass());
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

        if (fromStream) {
            try {
                InputStream stream = new ProtonBufferInputStream(buffer);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodeFailsWhenList8IndicateToManyEntries() throws IOException {
        doTestDecodeFailsWhenListIndicateToManyEntries(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeFailsWhenList32IndicateToManyEntries() throws IOException {
        doTestDecodeFailsWhenListIndicateToManyEntries(EncodingCodes.LIST32, true);
    }

    @Test
    public void testDecodeFailsWhenList8IndicateToManyEntriesFromStream() throws IOException {
        doTestDecodeFailsWhenListIndicateToManyEntries(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeFailsWhenList32IndicateToManyEntriesFromStream() throws IOException {
        doTestDecodeFailsWhenListIndicateToManyEntries(EncodingCodes.LIST32, true);
    }

    private void doTestDecodeFailsWhenListIndicateToManyEntries(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Header.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt((byte) 20);  // Size
            buffer.writeInt((byte) 10);  // Count
        } else {
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 20);  // Size
            buffer.writeByte((byte) 10);  // Count
        }

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.readValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.readValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodeFailsWhenList8IndicateOverflowedEntries() throws IOException {
        doTestDecodeFailsWhenListIndicateOverflowedEntries(EncodingCodes.LIST8, false);
    }

    @Test
    public void testDecodeFailsWhenList32IndicateOverflowedEntries() throws IOException {
        doTestDecodeFailsWhenListIndicateOverflowedEntries(EncodingCodes.LIST32, false);
    }

    @Test
    public void testDecodeFailsWhenList8IndicateOverflowedEntriesFromStream() throws IOException {
        doTestDecodeFailsWhenListIndicateOverflowedEntries(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeFailsWhenList32IndicateOverflowedEntriesFromStream() throws IOException {
        doTestDecodeFailsWhenListIndicateOverflowedEntries(EncodingCodes.LIST32, true);
    }

    private void doTestDecodeFailsWhenListIndicateOverflowedEntries(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Header.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt(20);  // Size
            buffer.writeInt(-1);  // Count
        } else {
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 20);  // Size
            buffer.writeByte((byte) 255);  // Count
        }

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.readValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Header.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.readValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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
