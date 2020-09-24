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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.TransferTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.TransferTypeEncoder;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.transport.Transfer;
import org.junit.jupiter.api.Test;

public class TransferTypeCodeTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Transfer.class, new TransferTypeDecoder().getTypeClass());
        assertEquals(Transfer.class, new TransferTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Transfer.DESCRIPTOR_CODE, new TransferTypeDecoder().getDescriptorCode());
        assertEquals(Transfer.DESCRIPTOR_CODE, new TransferTypeEncoder().getDescriptorCode());
        assertEquals(Transfer.DESCRIPTOR_SYMBOL, new TransferTypeDecoder().getDescriptorSymbol());
        assertEquals(Transfer.DESCRIPTOR_SYMBOL, new TransferTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ProtonBuffer tag = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2});

        Transfer input = new Transfer();

        input.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        input.setDeliveryId(10);
        input.setDeliveryTag(tag);
        input.setMessageFormat(0);
        input.setSettled(false);
        input.setBatchable(false);

        encoder.writeObject(buffer, encoderState, input);

        final Transfer result = (Transfer) decoder.readObject(buffer, decoderState);

        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), result.getHandle());
        assertEquals(10, result.getDeliveryId());
        assertEquals(tag, result.getDeliveryTag().tagBuffer());
        assertEquals(0, result.getMessageFormat());
        assertFalse(result.getSettled());
        assertFalse(result.getBatchable());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ProtonBuffer tag = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2});

        Transfer input = new Transfer();

        input.setHandle(UnsignedInteger.valueOf(2).longValue());
        input.setDeliveryId(100);
        input.setDeliveryTag(tag);
        input.setMessageFormat(1);
        input.setSettled(true);
        input.setBatchable(true);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, input);
        }

        input.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        input.setDeliveryId(10);
        input.setDeliveryTag(tag);
        input.setMessageFormat(0);
        input.setSettled(false);
        input.setBatchable(false);

        encoder.writeObject(buffer, encoderState, input);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Transfer.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Transfer);

        Transfer value = (Transfer) result;
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), value.getHandle());
        assertEquals(10, value.getDeliveryId());
        assertEquals(tag, value.getDeliveryTag().tagBuffer());
        assertEquals(0, value.getMessageFormat());
        assertFalse(value.getSettled());
        assertFalse(value.getBatchable());
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
        buffer.writeByte(Transfer.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Transfer.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Transfer.DESCRIPTOR_CODE.byteValue());
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

        Transfer[] array = new Transfer[3];

        array[0] = new Transfer();
        array[1] = new Transfer();
        array[2] = new Transfer();

        ProtonBuffer tag1 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0});
        ProtonBuffer tag2 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {1});
        ProtonBuffer tag3 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {2});

        array[0].setHandle(0).setDeliveryTag(tag1);
        array[1].setHandle(1).setDeliveryTag(tag2);
        array[2].setHandle(2).setDeliveryTag(tag3);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Transfer.class, result.getClass().getComponentType());

        Transfer[] resultArray = (Transfer[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Transfer);
            assertEquals(array[i].getHandle(), resultArray[i].getHandle());
            assertEquals(array[i].getDeliveryTag(), resultArray[i].getDeliveryTag());
        }
    }
}
