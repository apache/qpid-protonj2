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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.CloseTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.CloseTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.junit.jupiter.api.Test;

public class CloseTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Close.class, new CloseTypeDecoder().getTypeClass());
        assertEquals(Close.class, new CloseTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Close.DESCRIPTOR_CODE, new CloseTypeDecoder().getDescriptorCode());
        assertEquals(Close.DESCRIPTOR_CODE, new CloseTypeEncoder().getDescriptorCode());
        assertEquals(Close.DESCRIPTOR_SYMBOL, new CloseTypeDecoder().getDescriptorSymbol());
        assertEquals(Close.DESCRIPTOR_SYMBOL, new CloseTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeTypeWithNoError() throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Close input = new Close();

       encoder.writeObject(buffer, encoderState, input);

       final Close result = (Close) decoder.readObject(buffer, decoderState);

       assertNull(result.getError());
    }

    @Test
    public void testEncodeDecodeTypeWithError() throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), null);

       Close input = new Close();

       input.setError(error);

       encoder.writeObject(buffer, encoderState, input);

       final Close result = (Close) decoder.readObject(buffer, decoderState);

       assertNotNull(result.getError());
       assertNotNull(result.getError().getCondition());
       assertNull(result.getError().getDescription());
    }

    @Test
    public void testEncodeUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "error message");

        Close input = new Close();

        input.setError(error);

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Close);
        final Close result = (Close) decoded;

        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeEmptyUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Close input = new Close();

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Close);
        final Close result = (Close) decoded;

        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "error message");

        Close input = new Close();

        input.setError(error);

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        assertNotNull(buffer);

        final Close result = (Close) decoder.readObject(buffer, decoderState);
        assertNotNull(result);

        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeEmptyUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        Close input = new Close();

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        assertNotNull(buffer);

        final Close result = (Close) decoder.readObject(buffer, decoderState);
        assertNotNull(result);

        assertTypesEqual(input, result);
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Close close = new Close();

        close.setError(new ErrorCondition(AmqpError.INVALID_FIELD, "test"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, close);
        }

        close.setError(null);

        encoder.writeObject(buffer, encoderState, close);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Close.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Close);

        Close value = (Close) result;
        assertNull(value.getError());
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
        buffer.writeByte(Close.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Close.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Close.DESCRIPTOR_CODE.byteValue());
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

        Close[] array = new Close[3];

        array[0] = new Close();
        array[1] = new Close();
        array[2] = new Close();

        array[0].setError(new ErrorCondition(AmqpError.DECODE_ERROR, "1"));
        array[1].setError(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "2"));
        array[2].setError(new ErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED, "3"));

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Close.class, result.getClass().getComponentType());

        Close[] resultArray = (Close[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Close);
            assertEquals(array[i].getError(), resultArray[i].getError());
        }
    }
}
