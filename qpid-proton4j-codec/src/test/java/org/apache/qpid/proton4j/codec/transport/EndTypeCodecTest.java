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
package org.apache.qpid.proton4j.codec.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.EndTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transport.EndTypeEncoder;
import org.junit.Test;

public class EndTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(End.class, new EndTypeDecoder().getTypeClass());
        assertEquals(End.class, new EndTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(End.DESCRIPTOR_CODE, new EndTypeDecoder().getDescriptorCode());
        assertEquals(End.DESCRIPTOR_CODE, new EndTypeEncoder().getDescriptorCode());
        assertEquals(End.DESCRIPTOR_SYMBOL, new EndTypeDecoder().getDescriptorSymbol());
        assertEquals(End.DESCRIPTOR_SYMBOL, new EndTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Symbol, Object> infoMap = new LinkedHashMap<>();
        infoMap.put(Symbol.valueOf("1"), true);
        infoMap.put(Symbol.valueOf("2"), "string");

        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "Something bad", infoMap);

        End input = new End();
        input.setError(error);

        encoder.writeObject(buffer, encoderState, input);

        final End result = (End) decoder.readObject(buffer, decoderState);
        final ErrorCondition resultError = result.getError();

        assertNotNull(resultError);
        assertNotNull(resultError.getCondition());
        assertNotNull(resultError.getDescription());
        assertNotNull(resultError.getInfo());

        assertEquals(Symbol.valueOf("amqp-error"), resultError.getCondition());
        assertEquals("Something bad", resultError.getDescription());
        assertEquals(infoMap, resultError.getInfo());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        End end = new End();

        end.setError(new ErrorCondition(AmqpError.INVALID_FIELD, "test"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, end);
        }

        end.setError(null);

        encoder.writeObject(buffer, encoderState, end);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(End.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof End);

        End value = (End) result;
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
        buffer.writeByte(End.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(End.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(End.DESCRIPTOR_CODE.byteValue());
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

        End[] array = new End[3];

        array[0] = new End();
        array[1] = new End();
        array[2] = new End();

        array[0].setError(new ErrorCondition(AmqpError.DECODE_ERROR, "1"));
        array[1].setError(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "2"));
        array[2].setError(new ErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED, "3"));

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(End.class, result.getClass().getComponentType());

        End[] resultArray = (End[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof End);
            assertEquals(array[i].getError(), resultArray[i].getError());
        }
    }
}
