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
package org.apache.qpid.proton4j.codec.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ModifiedTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ModifiedTypeEncoder;
import org.junit.Test;

/**
 * Test codec handling of Modified types.
 */
public class ModifiedTypeCodecTest  extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Modified.class, new ModifiedTypeDecoder().getTypeClass());
        assertEquals(Modified.class, new ModifiedTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Modified.DESCRIPTOR_CODE, new ModifiedTypeDecoder().getDescriptorCode());
        assertEquals(Modified.DESCRIPTOR_CODE, new ModifiedTypeEncoder().getDescriptorCode());
        assertEquals(Modified.DESCRIPTOR_SYMBOL, new ModifiedTypeDecoder().getDescriptorSymbol());
        assertEquals(Modified.DESCRIPTOR_SYMBOL, new ModifiedTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeModified() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Modified value = new Modified();

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);

        value = (Modified) result;
        assertFalse(value.getDeliveryFailed());
        assertFalse(value.getUndeliverableHere());
    }

    @Test
    public void testDecodeModifiedDeliveryFailed() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Modified value = new Modified();
        value.setDeliveryFailed(true);

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);

        value = (Modified) result;
        assertTrue(value.getDeliveryFailed());
        assertFalse(value.getUndeliverableHere());
    }

    @Test
    public void testDecodeModifiedDeliveryFailedUndeliverableHere() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Modified value = new Modified();
        value.setDeliveryFailed(true);
        value.setUndeliverableHere(true);

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);

        value = (Modified) result;
        assertTrue(value.getDeliveryFailed());
        assertTrue(value.getUndeliverableHere());
    }

    @Test
    public void testDecodeModifiedWithAnnotations() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Symbol, Object> annotations = new LinkedHashMap<>();
        annotations.put(Symbol.valueOf("test"), "value");

        Modified value = new Modified();
        value.setMessageAnnotations(annotations);

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);

        value = (Modified) result;
        assertFalse(value.getDeliveryFailed());
        assertFalse(value.getUndeliverableHere());
        assertEquals(annotations, value.getMessageAnnotations());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Modified value = new Modified();
        value.setDeliveryFailed(true);
        value.setUndeliverableHere(true);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, value);
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Modified.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        value = (Modified) result;
        assertFalse(value.getUndeliverableHere());
        assertFalse(value.getDeliveryFailed());
    }

    @Test
    public void testDecodeModifiedWithList8() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Modified.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 0);  // Size
        buffer.writeByte((byte) 0);  // Count

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
    }

    @Test
    public void testDecodeModifiedWithList32() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Modified.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt((byte) 0);  // Size
        buffer.writeInt((byte) 0);  // Count

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
    }

    @Test
    public void testDecodeWithInvalidMap32Type() throws IOException {
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
        buffer.writeByte(Modified.DESCRIPTOR_CODE.byteValue());
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
        buffer.writeByte(Modified.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Modified.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (IOException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Modified[] array = new Modified[3];

        array[0] = new Modified();
        array[1] = new Modified();
        array[2] = new Modified();

        array[0].setDeliveryFailed(true).setUndeliverableHere(true);
        array[1].setDeliveryFailed(false).setUndeliverableHere(true);
        array[2].setDeliveryFailed(false).setUndeliverableHere(false);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Modified.class, result.getClass().getComponentType());

        Modified[] resultArray = (Modified[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Modified);
            assertEquals(array[i].getDeliveryFailed(), resultArray[i].getDeliveryFailed());
            assertEquals(array[i].getUndeliverableHere(), resultArray[i].getUndeliverableHere());
        }
    }
}
