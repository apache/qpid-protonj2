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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeliveryAnnotationsTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeliveryAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedShort;
import org.apache.qpid.proton4j.types.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.types.messaging.Modified;
import org.junit.Test;

public class DeliveryAnnotationsTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(DeliveryAnnotations.class, new DeliveryAnnotationsTypeDecoder().getTypeClass());
        assertEquals(DeliveryAnnotations.class, new DeliveryAnnotationsTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(DeliveryAnnotations.DESCRIPTOR_CODE, new DeliveryAnnotationsTypeDecoder().getDescriptorCode());
        assertEquals(DeliveryAnnotations.DESCRIPTOR_CODE, new DeliveryAnnotationsTypeEncoder().getDescriptorCode());
        assertEquals(DeliveryAnnotations.DESCRIPTOR_SYMBOL, new DeliveryAnnotationsTypeDecoder().getDescriptorSymbol());
        assertEquals(DeliveryAnnotations.DESCRIPTOR_SYMBOL, new DeliveryAnnotationsTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeSmallSeriesOfDeliveryAnnotations() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfDeliveryAnnotations() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(LARGE_SIZE);
    }

    @Test
    public void testDecodeDeliveryAnnotations() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(1);
    }

    private void doTestDecodeDeliveryAnnotationsSeries(int size) throws IOException {

        final Symbol SYMBOL_1 = Symbol.valueOf("test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("test2");
        final Symbol SYMBOL_3 = Symbol.valueOf("test3");

        DeliveryAnnotations annotations = new DeliveryAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(SYMBOL_2, UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(SYMBOL_3, UnsignedInteger.valueOf(128));

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, annotations);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof DeliveryAnnotations);

            DeliveryAnnotations readAnnotations = (DeliveryAnnotations) result;

            Map<Symbol, Object> resultMap = readAnnotations.getValue();

            assertEquals(annotations.getValue().size(), resultMap.size());
            assertEquals(resultMap.get(SYMBOL_1), UnsignedByte.valueOf((byte) 128));
            assertEquals(resultMap.get(SYMBOL_2), UnsignedShort.valueOf((short) 128));
            assertEquals(resultMap.get(SYMBOL_3), UnsignedInteger.valueOf(128));
        }
    }

    @Test
    public void testEncodeDecodeDeliveryAnnotationsArray() throws IOException {
        final Symbol SYMBOL_1 = Symbol.valueOf("test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("test2");
        final Symbol SYMBOL_3 = Symbol.valueOf("test3");

        DeliveryAnnotations[] array = new DeliveryAnnotations[3];

        DeliveryAnnotations annotations = new DeliveryAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(SYMBOL_2, UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(SYMBOL_3, UnsignedInteger.valueOf(128));

        array[0] = annotations;
        array[1] = annotations;
        array[2] = annotations;

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(DeliveryAnnotations.class, result.getClass().getComponentType());

        DeliveryAnnotations[] resultArray = (DeliveryAnnotations[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            DeliveryAnnotations readAnnotations = resultArray[i];

            Map<Symbol, Object> resultMap = readAnnotations.getValue();

            assertEquals(annotations.getValue().size(), resultMap.size());
            assertEquals(resultMap.get(SYMBOL_1), UnsignedByte.valueOf((byte) 128));
            assertEquals(resultMap.get(SYMBOL_2), UnsignedShort.valueOf((short) 128));
            assertEquals(resultMap.get(SYMBOL_3), UnsignedInteger.valueOf(128));
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("one"), 1);
        map.put(Symbol.valueOf("two"), Boolean.TRUE);
        map.put(Symbol.valueOf("three"), "test");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new DeliveryAnnotations(map));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
    }

    @Test
    public void testEncodeDecodeMessageAnnotationsWithEmptyValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeObject(buffer, encoderState, new DeliveryAnnotations(null));

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof DeliveryAnnotations);

        DeliveryAnnotations readAnnotations = (DeliveryAnnotations) result;
        assertNull(readAnnotations.getValue());
    }

    @Test
    public void testSkipValueWithInvalidList32Type() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST32);
    }

    @Test
    public void testSkipValueWithInvalidList8Type() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST8);
    }

    @Test
    public void testSkipValueWithInvalidList0Type() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST0);
    }

    private void doTestSkipValueWithInvalidListType(byte listType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeliveryAnnotations.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else if (listType == EncodingCodes.LIST8){
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.LIST0);
        }

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testSkipValueWithNullMapEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeliveryAnnotations.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
        } catch (DecodeException ex) {
            fail("Should be able to skip type with null inner encoding");
        }
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        DeliveryAnnotations[] array = new DeliveryAnnotations[3];

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("1"), Boolean.TRUE);
        map.put(Symbol.valueOf("2"), Boolean.FALSE);

        array[0] = new DeliveryAnnotations(new HashMap<>());
        array[1] = new DeliveryAnnotations(map);
        array[2] = new DeliveryAnnotations(map);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(DeliveryAnnotations.class, result.getClass().getComponentType());

        DeliveryAnnotations[] resultArray = (DeliveryAnnotations[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof DeliveryAnnotations);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }

    @Test
    public void testEncodeAndDecodeAnnoationsWithEmbeddedMaps() throws IOException {
        final Symbol SYMBOL_1 = Symbol.valueOf("x-opt-test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("x-opt-test2");

        final String VALUE_1 = "string";
        final UnsignedInteger VALUE_2 = UnsignedInteger.valueOf(42);
        final UUID VALUE_3 = UUID.randomUUID();

        Map<String, Object> stringKeyedMap = new HashMap<>();
        stringKeyedMap.put("key1", VALUE_1);
        stringKeyedMap.put("key2", VALUE_2);
        stringKeyedMap.put("key3", VALUE_3);

        Map<Symbol, Object> symbolKeyedMap = new HashMap<>();
        symbolKeyedMap.put(Symbol.valueOf("key1"), VALUE_1);
        symbolKeyedMap.put(Symbol.valueOf("key2"), VALUE_2);
        symbolKeyedMap.put(Symbol.valueOf("key3"), VALUE_3);

        DeliveryAnnotations annotations = new DeliveryAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, stringKeyedMap);
        annotations.getValue().put(SYMBOL_2, symbolKeyedMap);

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeObject(buffer, encoderState, annotations);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof DeliveryAnnotations);

        DeliveryAnnotations readAnnotations = (DeliveryAnnotations) result;

        Map<Symbol, Object> resultMap = readAnnotations.getValue();

        assertEquals(annotations.getValue().size(), resultMap.size());
        assertEquals(resultMap.get(SYMBOL_1), stringKeyedMap);
        assertEquals(resultMap.get(SYMBOL_2), symbolKeyedMap);
    }
}
