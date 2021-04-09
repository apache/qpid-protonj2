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
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
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
    public void testEncodeDecodeTypeWithNoError() throws IOException {
        doTestEncodeDecodeTypeWithNoError(false);
    }

    @Test
    public void testEncodeDecodeTypeWithNoErrorFromStream() throws IOException {
        doTestEncodeDecodeTypeWithNoError(true);
    }

    private void doTestEncodeDecodeTypeWithNoError(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

       Close input = new Close();

       encoder.writeObject(buffer, encoderState, input);

       final Close result;
       if (fromStream) {
           result = (Close) streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = (Close) decoder.readObject(buffer, decoderState);
       }

       assertNull(result.getError());
    }

    @Test
    public void testEncodeDecodeTypeWithError() throws Exception {
        doTestEncodeDecodeTypeWithError(false);
    }

    @Test
    public void testEncodeDecodeTypeWithErrorFromStream() throws Exception {
        doTestEncodeDecodeTypeWithError(true);
    }

    private void doTestEncodeDecodeTypeWithError(boolean fromStream) throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), null);
       InputStream stream = new ProtonBufferInputStream(buffer);

       Close input = new Close();

       input.setError(error);

       encoder.writeObject(buffer, encoderState, input);

       final Close result;
       if (fromStream) {
           result = (Close) streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = (Close) decoder.readObject(buffer, decoderState);
       }

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
        doTestEncodeUsingLegacyCodecAndDecodeWithNewCodec(false);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodecFromStream() throws Exception {
        doTestEncodeUsingLegacyCodecAndDecodeWithNewCodec(true);
    }

    private void doTestEncodeUsingLegacyCodecAndDecodeWithNewCodec(boolean fromStream) throws Exception {
        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "error message");

        Close input = new Close();

        input.setError(error);

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        InputStream stream = new ProtonBufferInputStream(buffer);

        assertNotNull(buffer);

        final Close result;
        if (fromStream) {
            result = (Close) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Close) decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);

        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeEmptyUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        doTestEncodeEmptyUsingLegacyCodecAndDecodeWithNewCodec(false);
    }

    @Test
    public void testEncodeEmptyUsingLegacyCodecAndDecodeWithNewCodecFromStream() throws Exception {
        doTestEncodeEmptyUsingLegacyCodecAndDecodeWithNewCodec(true);
    }

    private void doTestEncodeEmptyUsingLegacyCodecAndDecodeWithNewCodec(boolean fromStream) throws Exception {
        Close input = new Close();

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        InputStream stream = new ProtonBufferInputStream(buffer);

        assertNotNull(buffer);

        final Close result;
        if (fromStream) {
            result = (Close) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Close) decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);

        assertTypesEqual(input, result);
    }

    @Test
    public void testSkipValue() throws IOException {
        testSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        testSkipValue(true);
    }

    private void testSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Close close = new Close();

        close.setError(new ErrorCondition(AmqpError.INVALID_FIELD, "test"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, close);
        }

        close.setError(null);

        encoder.writeObject(buffer, encoderState, close);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Close.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Close.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Close);

        Close value = (Close) result;
        assertNull(value.getError());
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

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Close.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Close.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodedWithInvalidMap32Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, false);
    }

    @Test
    public void testDecodeWithInvalidMap8Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, false);
    }

    @Test
    public void testDecodedWithInvalidMap32TypeFromStream() throws IOException {
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

        Close[] array = new Close[3];

        array[0] = new Close();
        array[1] = new Close();
        array[2] = new Close();

        array[0].setError(new ErrorCondition(AmqpError.DECODE_ERROR, "1"));
        array[1].setError(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "2"));
        array[2].setError(new ErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED, "3"));

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

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
