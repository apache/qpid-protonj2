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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.DetachTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transport.DetachTypeEncoder;
import org.junit.Test;

public class DetachTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Detach.class, new DetachTypeDecoder().getTypeClass());
        assertEquals(Detach.class, new DetachTypeEncoder().getTypeClass());
    }

    @Test
    public void testEncodeDecodeTypeWithNoError() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Detach input = new Detach();
        input.setHandle(1);
        input.setClosed(false);

        encoder.writeObject(buffer, encoderState, input);

        final Detach result = (Detach) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getHandle());
        assertFalse(result.getClosed());
        assertNull(result.getError());
    }

    @Test
    public void testEncodeDecodeTypeWithError() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), null);

        Detach input = new Detach();
        input.setHandle(1);
        input.setClosed(true);
        input.setError(error);

        encoder.writeObject(buffer, encoderState, input);

        final Detach result = (Detach) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getHandle());
        assertTrue(result.getClosed());
        assertNotNull(result.getError());
        assertNotNull(result.getError().getCondition());
        assertNull(result.getError().getDescription());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Detach detach = new Detach();

        detach.setError(new ErrorCondition(AmqpError.INVALID_FIELD, "test"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, detach);
        }

        detach.setError(null);

        encoder.writeObject(buffer, encoderState, detach);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Detach.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Detach);

        Detach value = (Detach) result;
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
        buffer.writeByte(Detach.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Detach.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Detach.DESCRIPTOR_CODE.byteValue());
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

        Detach[] array = new Detach[3];

        array[0] = new Detach();
        array[1] = new Detach();
        array[2] = new Detach();

        array[0].setError(new ErrorCondition(AmqpError.DECODE_ERROR, "1"));
        array[1].setError(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "2"));
        array[2].setError(new ErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED, "3"));

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Detach.class, result.getClass().getComponentType());

        Detach[] resultArray = (Detach[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Detach);
            assertEquals(array[i].getError(), resultArray[i].getError());
        }
    }
}
