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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DataTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DataTypeEncoder;
import org.junit.Test;

public class DataTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Data.class, new DataTypeDecoder().getTypeClass());
        assertEquals(Data.class, new DataTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Data.DESCRIPTOR_CODE, new DataTypeDecoder().getDescriptorCode());
        assertEquals(Data.DESCRIPTOR_CODE, new DataTypeEncoder().getDescriptorCode());
        assertEquals(Data.DESCRIPTOR_SYMBOL, new DataTypeDecoder().getDescriptorSymbol());
        assertEquals(Data.DESCRIPTOR_SYMBOL, new DataTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeData() throws IOException {
        doTestDecodeDataSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfDatas() throws IOException {
        doTestDecodeDataSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfDatas() throws IOException {
        doTestDecodeDataSeries(LARGE_SIZE);
    }

    private void doTestDecodeDataSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Data data = new Data(new Binary(new byte[] { 1, 2, 3}));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, data);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Data);

            Data decoded = (Data) result;

            assertEquals(data.getValue(), decoded.getValue());
        }
    }

    @Test
    public void testEncodeDecodeArrayOfDataSections() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Data[] dataArray = new Data[3];

        dataArray[0] = new Data(new Binary(new byte[] { 1, 2, 3}));
        dataArray[1] = new Data(new Binary(new byte[] { 4, 5, 6}));
        dataArray[2] = new Data(new Binary(new byte[] { 7, 8, 9}));

        encoder.writeObject(buffer, encoderState, dataArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Data.class, result.getClass().getComponentType());

        Data[] resultArray = (Data[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Data);
            assertEquals(dataArray[i].getValue(), resultArray[i].getValue());
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new Data(new Binary(new byte[] { (byte) i })));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Data.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.getUndeliverableHere());
        assertFalse(modified.getDeliveryFailed());
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
        buffer.writeByte(Data.DESCRIPTOR_CODE.byteValue());
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
        buffer.writeByte(Data.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Data.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (IOException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Data[] array = new Data[3];

        Binary bytes1 = new Binary(new byte[] {0});
        Binary bytes2 = new Binary(new byte[] {1});
        Binary bytes3 = new Binary(new byte[] {2});

        array[0] = new Data(bytes1);
        array[1] = new Data(bytes2);
        array[2] = new Data(bytes3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Data.class, result.getClass().getComponentType());

        Data[] resultArray = (Data[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Data);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }
}
