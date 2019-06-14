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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.decoders.security.SaslInitTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton4j.codec.encoders.security.SaslInitTypeEncoder;
import org.junit.Before;
import org.junit.Test;

public class SaslInitTypeCodecTest extends CodecTestSupport {

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
        assertEquals(SaslInit.class, new SaslInitTypeDecoder().getTypeClass());
        assertEquals(SaslInit.class, new SaslInitTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        SaslInitTypeDecoder decoder = new SaslInitTypeDecoder();
        SaslInitTypeEncoder encoder = new SaslInitTypeEncoder();

        assertEquals(SaslInit.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(SaslInit.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(SaslInit.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(SaslInit.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeTypeMechanismOnly() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslInit input = new SaslInit();
        input.setMechanism(Symbol.valueOf("ANONYMOUS"));

        encoder.writeObject(buffer, encoderState, input);

        final SaslInit result = (SaslInit) decoder.readObject(buffer, decoderState);

        assertEquals(Symbol.valueOf("ANONYMOUS"), result.getMechanism());
        assertNull(result.getHostname());
        assertNull(result.getInitialResponse());
    }

    @Test
    public void testEncodeDecodeTypeMechanismAndHostname() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslInit input = new SaslInit();
        input.setMechanism(Symbol.valueOf("ANONYMOUS"));
        input.setHostname("test");

        encoder.writeObject(buffer, encoderState, input);

        final SaslInit result = (SaslInit) decoder.readObject(buffer, decoderState);

        assertEquals(Symbol.valueOf("ANONYMOUS"), result.getMechanism());
        assertEquals("test", result.getHostname());
        assertNull(result.getInitialResponse());
    }

    @Test
    public void testEncodeDecodeTypeAllFieldsSet() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        byte[] initialResponse = new byte[] { 1, 2, 3, 4 };

        SaslInit input = new SaslInit();
        input.setInitialResponse(new Binary(initialResponse));
        input.setHostname("test");
        input.setMechanism(Symbol.valueOf("ANONYMOUS"));

        encoder.writeObject(buffer, encoderState, input);

        final SaslInit result = (SaslInit) decoder.readObject(buffer, decoderState);

        assertEquals("test", result.getHostname());
        assertEquals(Symbol.valueOf("ANONYMOUS"), result.getMechanism());
        assertArrayEquals(initialResponse, result.getInitialResponse().getArray());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslInit init = new SaslInit();

        init.setInitialResponse(new Binary(new byte[] {0}));
        init.setHostname("skip");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, init);
        }

        init.setInitialResponse(new Binary(new byte[] {1, 2}));
        init.setHostname("localhost");
        init.setMechanism(Symbol.valueOf("PLAIN"));

        encoder.writeObject(buffer, encoderState, init);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(SaslInit.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof SaslInit);

        SaslInit value = (SaslInit) result;
        assertArrayEquals(new byte[] {1, 2}, value.getInitialResponse().getArray());
        assertEquals("localhost", value.getHostname());
        assertEquals(Symbol.valueOf("PLAIN"), value.getMechanism());
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
        buffer.writeByte(SaslInit.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(SaslInit.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(SaslInit.DESCRIPTOR_CODE.byteValue());
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
}
