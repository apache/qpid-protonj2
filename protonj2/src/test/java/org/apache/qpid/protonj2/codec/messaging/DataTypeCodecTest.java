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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DataTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DataTypeEncoder;
import org.apache.qpid.protonj2.codec.util.SimplePojo;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

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
        doTestDecodeDataSeries(1, false);
    }

    @Test
    public void testDecodeSmallSeriesOfDatas() throws IOException {
        doTestDecodeDataSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfDatas() throws IOException {
        doTestDecodeDataSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeDataFromStream() throws IOException {
        doTestDecodeDataSeries(1, true);
    }

    @Test
    public void testDecodeSmallSeriesOfDatasFromStream() throws IOException {
        doTestDecodeDataSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfDatasFromStream() throws IOException {
        doTestDecodeDataSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeDataSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Data data = new Data(new Binary(new byte[] { 1, 2, 3}));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, data);
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof Data);

            Data decoded = (Data) result;

            assertArrayEquals(data.getValue(), decoded.getValue());
        }
    }

    @Test
    public void testDecodeDataWithPayloadInUpperBoundsOfSmallBinaryEncoding() throws IOException {
        doTestDecodeDataWithPayloadInUpperBoundsOfSmallBinaryEncoding(false);
    }

    @Test
    public void testDecodeDataWithPayloadInUpperBoundsOfSmallBinaryEncodingFromStream() throws IOException {
        doTestDecodeDataWithPayloadInUpperBoundsOfSmallBinaryEncoding(true);
    }

    private void doTestDecodeDataWithPayloadInUpperBoundsOfSmallBinaryEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int SIZE = 240;

        Data data = new Data(new Binary(new byte[SIZE]));
        for (int i = 0; i < SIZE; ++i) {
            data.getValue()[i] = (byte) i;
        }

        for (int i = 0; i < SIZE; ++i) {
            data.getBinary().getArray()[i] = (byte) i;
        }

        encoder.writeObject(buffer, encoderState, data);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Data);

        Data decoded = (Data) result;

        assertEquals(data.getBinary(), decoded.getBinary());
    }

    @Test
    public void testDecodeDataWithPayloadInVBIN32BinaryEncoding() throws IOException {
        doTestDecodeDataWithPayloadInVBIN32BinaryEncoding(false);
    }

    @Test
    public void testDecodeDataWithPayloadInVBIN32BinaryEncodingFromStream() throws IOException {
        doTestDecodeDataWithPayloadInVBIN32BinaryEncoding(true);
    }

    private void doTestDecodeDataWithPayloadInVBIN32BinaryEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        final int SIZE = 65535;

        Data data = new Data(new Binary(new byte[SIZE]));
        for (int i = 0; i < SIZE; ++i) {
            data.getValue()[i] = (byte) i;
        }

        for (int i = 0; i < SIZE; ++i) {
            data.getBinary().getArray()[i] = (byte) i;
        }

        encoder.writeObject(buffer, encoderState, data);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Data);

        Data decoded = (Data) result;

        assertEquals(data.getBinary(), decoded.getBinary());
    }

    @Test
    public void testEncodeDecodeArrayOfDataSections() throws IOException {
        doTestEncodeDecodeArrayOfDataSections(false);
    }

    @Test
    public void testEncodeDecodeArrayOfDataSectionsFromStream() throws IOException {
        doTestEncodeDecodeArrayOfDataSections(true);
    }

    private void doTestEncodeDecodeArrayOfDataSections(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Data[] dataArray = new Data[3];

        dataArray[0] = new Data(new Binary(new byte[] { 1, 2, 3}));
        dataArray[1] = new Data(new Binary(new byte[] { 4, 5, 6}));
        dataArray[2] = new Data(new Binary(new byte[] { 7, 8, 9}));

        encoder.writeObject(buffer, encoderState, dataArray);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Data.class, result.getClass().getComponentType());

        Data[] resultArray = (Data[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Data);
            assertEquals(dataArray[i].getBinary(), resultArray[i].getBinary());
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new Data(new Binary(new byte[] { (byte) i })));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Data.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Data.class, typeDecoder.getTypeClass());
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

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

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Data.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Data.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodeSerializedTypeFromDataSection() throws IOException {
        doTestDecodeSerializedTypeFromDataSection(false);
    }

    @Test
    public void testDecodeSerializedTypeFromDataSectionFromStream() throws IOException {
        doTestDecodeSerializedTypeFromDataSection(true);
    }

    private void doTestDecodeSerializedTypeFromDataSection(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        SimplePojo expectedContent = new SimplePojo(UUID.randomUUID());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(expectedContent);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        encoder.writeObject(buffer, encoderState, new Binary(bytes));

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Binary);
        Binary binary = (Binary) result;
        assertEquals(bytes.length, binary.getLength());
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        testEncodeDecodeArray(false);
    }

    @Test
    public void testEncodeDecodeArrayFromStream() throws IOException {
        testEncodeDecodeArray(true);
    }

    private void testEncodeDecodeArray(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Data[] array = new Data[3];

        Binary bytes1 = new Binary(new byte[] {0});
        Binary bytes2 = new Binary(new byte[] {1});
        Binary bytes3 = new Binary(new byte[] {2});

        array[0] = new Data(bytes1);
        array[1] = new Data(bytes2);
        array[2] = new Data(bytes3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Data.class, result.getClass().getComponentType());

        Data[] resultArray = (Data[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Data);
            assertArrayEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }
}
