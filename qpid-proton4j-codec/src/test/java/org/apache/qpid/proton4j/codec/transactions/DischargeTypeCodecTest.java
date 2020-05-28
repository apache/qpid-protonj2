/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton4j.codec.transactions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DischargeTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DischargeTypeEncoder;
import org.junit.Test;

/**
 * Test for handling Declare serialization
 */
public class DischargeTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Discharge.class, new DischargeTypeDecoder().getTypeClass());
        assertEquals(Discharge.class, new DischargeTypeDecoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        DischargeTypeDecoder decoder = new DischargeTypeDecoder();
        DischargeTypeEncoder encoder = new DischargeTypeEncoder();

        assertEquals(Discharge.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(Discharge.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(Discharge.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(Discharge.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Discharge input = new Discharge();

        input.setFail(true);
        input.setTxnId(new Binary(new byte[] { 8, 7, 6, 5 }));

        encoder.writeObject(buffer, encoderState, input);

        final Discharge result = (Discharge) decoder.readObject(buffer, decoderState);

        assertTrue(result.getFail());

        assertNotNull(result.getTxnId());
        assertNotNull(result.getTxnId().getArray());

        assertArrayEquals(new byte[] { 8, 7, 6, 5 }, result.getTxnId().getArray());
    }

    @Test
    public void testEncodeDecodeTypeWithLargeResponseBlob() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        byte[] txnId = new byte[512];

        Random rand = new Random();
        rand.setSeed(System.currentTimeMillis());
        rand.nextBytes(txnId);

        Discharge input = new Discharge();

        input.setFail(true);
        input.setTxnId(new Binary(txnId));

        encoder.writeObject(buffer, encoderState, input);

        final Discharge result = (Discharge) decoder.readObject(buffer, decoderState);

        assertArrayEquals(txnId, result.getTxnId().getArray());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Discharge discharge = new Discharge();

        discharge.setTxnId(new Binary(new byte[] {0}));
        discharge.setFail(false);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, discharge);
        }

        discharge.setTxnId(new Binary(new byte[] {1, 2}));
        discharge.setFail(true);

        encoder.writeObject(buffer, encoderState, discharge);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Discharge.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Discharge);

        Discharge value = (Discharge) result;
        assertArrayEquals(new byte[] {1, 2}, value.getTxnId().getArray());
        assertTrue(value.getFail());
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
        buffer.writeByte(Discharge.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Discharge.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Discharge.DESCRIPTOR_CODE.byteValue());
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

        Discharge[] array = new Discharge[3];

        array[0] = new Discharge();
        array[1] = new Discharge();
        array[2] = new Discharge();

        array[0].setTxnId(new Binary(new byte[] {0})).setFail(true);
        array[1].setTxnId(new Binary(new byte[] {1})).setFail(false);
        array[2].setTxnId(new Binary(new byte[] {2})).setFail(true);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Discharge.class, result.getClass().getComponentType());

        Discharge[] resultArray = (Discharge[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Discharge);
            assertEquals(array[i].getTxnId(), resultArray[i].getTxnId());
            assertEquals(array[i].getFail(), resultArray[i].getFail());
        }
    }
}
