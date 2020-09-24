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
package org.apache.qpid.protonj2.codec.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.protonj2.codec.decoders.security.SaslMechanismsTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.protonj2.codec.encoders.security.SaslMechanismsTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.security.SaslMechanisms;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SaslMechanismsTypeCodecTest extends CodecTestSupport {

    @Override
    @BeforeEach
    public void setUp() {
        decoder = ProtonDecoderFactory.createSasl();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.createSasl();
        encoderState = encoder.newEncoderState();
    }

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(SaslMechanisms.class, new SaslMechanismsTypeDecoder().getTypeClass());
        assertEquals(SaslMechanisms.class, new SaslMechanismsTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        SaslMechanismsTypeDecoder decoder = new SaslMechanismsTypeDecoder();
        SaslMechanismsTypeEncoder encoder = new SaslMechanismsTypeEncoder();

        assertEquals(SaslMechanisms.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(SaslMechanisms.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(SaslMechanisms.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(SaslMechanisms.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] mechanisms = new Symbol[] { Symbol.valueOf("ANONYMOUS"), Symbol.valueOf("EXTERNAL") };

        SaslMechanisms input = new SaslMechanisms();
        input.setSaslServerMechanisms(mechanisms);

        encoder.writeObject(buffer, encoderState, input);

        final SaslMechanisms result = (SaslMechanisms) decoder.readObject(buffer, decoderState);

        assertArrayEquals(mechanisms, result.getSaslServerMechanisms());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslMechanisms mechanisms = new SaslMechanisms();

        mechanisms.setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, mechanisms);
        }

        mechanisms.setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"), Symbol.valueOf("EXTERNAL"));

        encoder.writeObject(buffer, encoderState, mechanisms);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(SaslMechanisms.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof SaslMechanisms);

        SaslMechanisms value = (SaslMechanisms) result;
        assertArrayEquals(new Symbol[] {Symbol.valueOf("ANONYMOUS"), Symbol.valueOf("EXTERNAL")}, value.getSaslServerMechanisms());
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
        buffer.writeByte(SaslMechanisms.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(SaslMechanisms.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(SaslMechanisms.DESCRIPTOR_CODE.byteValue());
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

        SaslMechanisms[] array = new SaslMechanisms[3];

        array[0] = new SaslMechanisms();
        array[1] = new SaslMechanisms();
        array[2] = new SaslMechanisms();

        array[0].setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"), Symbol.valueOf("PLAIN"), Symbol.valueOf("EXTERNAL"));
        array[1].setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"), Symbol.valueOf("PLAIN"));
        array[2].setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"));

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(SaslMechanisms.class, result.getClass().getComponentType());

        SaslMechanisms[] resultArray = (SaslMechanisms[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof SaslMechanisms);
            assertArrayEquals(array[i].getSaslServerMechanisms(), resultArray[i].getSaslServerMechanisms());
        }
    }
}
