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
package org.apache.qpid.proton4j.codec.security;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.security.SaslCode;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.decoders.security.SaslOutcomeTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton4j.codec.encoders.security.SaslOutcomeTypeEncoder;
import org.junit.Before;
import org.junit.Test;

public class SaslOutcomeTypeCodecTest extends CodecTestSupport {

    @Override
    @Before
    public void setUp() {
        decoder = ProtonDecoderFactory.createSasl();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.createSasl();
        encoderState = encoder.newEncoderState();
    }

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(SaslOutcome.class, new SaslOutcomeTypeDecoder().getTypeClass());
        assertEquals(SaslOutcome.class, new SaslOutcomeTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        SaslOutcomeTypeDecoder decoder = new SaslOutcomeTypeDecoder();
        SaslOutcomeTypeEncoder encoder = new SaslOutcomeTypeEncoder();

        assertEquals(SaslOutcome.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(SaslOutcome.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(SaslOutcome.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(SaslOutcome.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        byte[] data = new byte[] { 1, 2, 3, 4 };
        SaslCode code = SaslCode.AUTH;

        SaslOutcome input = new SaslOutcome();
        input.setAdditionalData(new Binary(data));
        input.setCode(code);

        encoder.writeObject(buffer, encoderState, input);

        final SaslOutcome result = (SaslOutcome) decoder.readObject(buffer, decoderState);

        assertEquals(code, result.getCode());
        assertArrayEquals(data, result.getAdditionalData().getArray());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslOutcome outcome = new SaslOutcome();

        outcome.setAdditionalData(new Binary(new byte[] {0}));
        outcome.setCode(SaslCode.AUTH);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, outcome);
        }

        outcome.setAdditionalData(new Binary(new byte[] {1, 2}));
        outcome.setCode(SaslCode.SYS_TEMP);

        encoder.writeObject(buffer, encoderState, outcome);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(SaslOutcome.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof SaslOutcome);

        SaslOutcome value = (SaslOutcome) result;
        assertArrayEquals(new byte[] {1, 2}, value.getAdditionalData().getArray());
        assertEquals(SaslCode.SYS_TEMP, value.getCode());
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
        buffer.writeByte(SaslOutcome.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(SaslOutcome.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (IOException ex) {}
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
        buffer.writeByte(SaslOutcome.DESCRIPTOR_CODE.byteValue());
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
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslOutcome[] array = new SaslOutcome[3];

        array[0] = new SaslOutcome();
        array[1] = new SaslOutcome();
        array[2] = new SaslOutcome();

        array[0].setCode(SaslCode.OK).setAdditionalData(new Binary(new byte[] {0}));
        array[1].setCode(SaslCode.SYS_TEMP).setAdditionalData(new Binary(new byte[] {1}));
        array[2].setCode(SaslCode.AUTH).setAdditionalData(new Binary(new byte[] {2}));

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(SaslOutcome.class, result.getClass().getComponentType());

        SaslOutcome[] resultArray = (SaslOutcome[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof SaslOutcome);
            assertEquals(array[i].getCode(), resultArray[i].getCode());
            assertEquals(array[i].getAdditionalData(), resultArray[i].getAdditionalData());
        }
    }
}
