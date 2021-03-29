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
package org.apache.qpid.protonj2.codec.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.MessageAnnotationsTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.MessageAnnotationsTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

public class MessageAnnotationsTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(MessageAnnotations.class, new MessageAnnotationsTypeDecoder().getTypeClass());
        assertEquals(MessageAnnotations.class, new MessageAnnotationsTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(MessageAnnotations.DESCRIPTOR_CODE, new MessageAnnotationsTypeDecoder().getDescriptorCode());
        assertEquals(MessageAnnotations.DESCRIPTOR_CODE, new MessageAnnotationsTypeEncoder().getDescriptorCode());
        assertEquals(MessageAnnotations.DESCRIPTOR_SYMBOL, new MessageAnnotationsTypeDecoder().getDescriptorSymbol());
        assertEquals(MessageAnnotations.DESCRIPTOR_SYMBOL, new MessageAnnotationsTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeSmallSeriesOfMessageAnnotations() throws IOException {
        doTestDecodeMessageAnnotationsSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfMessageAnnotations() throws IOException {
        doTestDecodeMessageAnnotationsSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeMessageAnnotations() throws IOException {
        doTestDecodeMessageAnnotationsSeries(1, false);
    }

    @Test
    public void testDecodeSmallSeriesOfMessageAnnotationsFromStream() throws IOException {
        doTestDecodeMessageAnnotationsSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfMessageAnnotationsFromStream() throws IOException {
        doTestDecodeMessageAnnotationsSeries(LARGE_SIZE, true);
    }

    @Test
    public void testDecodeMessageAnnotationsFromStream() throws IOException {
        doTestDecodeMessageAnnotationsSeries(1, true);
    }

    private void doTestDecodeMessageAnnotationsSeries(int size, boolean fromStream) throws IOException {
        final Symbol SYMBOL_1 = Symbol.valueOf("test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("test2");
        final Symbol SYMBOL_3 = Symbol.valueOf("test3");

        MessageAnnotations annotations = new MessageAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(SYMBOL_2, UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(SYMBOL_3, UnsignedInteger.valueOf(128));

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, annotations);
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof MessageAnnotations);

            MessageAnnotations readAnnotations = (MessageAnnotations) result;

            Map<Symbol, Object> resultMap = readAnnotations.getValue();

            assertEquals(annotations.getValue().size(), resultMap.size());
            assertEquals(resultMap.get(SYMBOL_1), UnsignedByte.valueOf((byte) 128));
            assertEquals(resultMap.get(SYMBOL_2), UnsignedShort.valueOf((short) 128));
            assertEquals(resultMap.get(SYMBOL_3), UnsignedInteger.valueOf(128));
        }
    }

    @Test
    public void testEncodeDecodeMessageAnnotationsArray() throws IOException {
        doTstEncodeDecodeMessageAnnotationsArray(false);
    }

    @Test
    public void testEncodeDecodeMessageAnnotationsArrayFromStream() throws IOException {
        doTstEncodeDecodeMessageAnnotationsArray(true);
    }

    private void doTstEncodeDecodeMessageAnnotationsArray(boolean fromStream) throws IOException {
        final Symbol SYMBOL_1 = Symbol.valueOf("test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("test2");
        final Symbol SYMBOL_3 = Symbol.valueOf("test3");

        MessageAnnotations[] array = new MessageAnnotations[3];

        MessageAnnotations annotations = new MessageAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(SYMBOL_2, UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(SYMBOL_3, UnsignedInteger.valueOf(128));

        array[0] = annotations;
        array[1] = annotations;
        array[2] = annotations;

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(MessageAnnotations.class, result.getClass().getComponentType());

        MessageAnnotations[] resultArray = (MessageAnnotations[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            MessageAnnotations readAnnotations = resultArray[i];

            Map<Symbol, Object> resultMap = readAnnotations.getValue();

            assertEquals(annotations.getValue().size(), resultMap.size());
            assertEquals(resultMap.get(SYMBOL_1), UnsignedByte.valueOf((byte) 128));
            assertEquals(resultMap.get(SYMBOL_2), UnsignedShort.valueOf((short) 128));
            assertEquals(resultMap.get(SYMBOL_3), UnsignedInteger.valueOf(128));
        }
    }

    @Test
    public void testEncodeDecodeMessageAnnotationsWithEmptyValue() throws IOException {
        doTestEncodeDecodeMessageAnnotationsWithEmptyValue(false);
    }

    @Test
    public void testEncodeDecodeMessageAnnotationsWithEmptyValueFromStream() throws IOException {
        doTestEncodeDecodeMessageAnnotationsWithEmptyValue(true);
    }

    private void doTestEncodeDecodeMessageAnnotationsWithEmptyValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeObject(buffer, encoderState, new MessageAnnotations(null));

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof MessageAnnotations);

        MessageAnnotations readAnnotations = (MessageAnnotations) result;
        assertNull(readAnnotations.getValue());
    }

    @Test
    public void testSkipValue() throws IOException {
        doTestSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        doTestSkipValue(true);
    }

    private void doTestSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("one"), 1);
        map.put(Symbol.valueOf("two"), Boolean.TRUE);
        map.put(Symbol.valueOf("three"), "test");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new MessageAnnotations(map));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(MessageAnnotations.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(MessageAnnotations.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
    }

    @Test
    public void testSkipValueWithInvalidList32Type() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST32, false);
    }

    @Test
    public void testSkipValueWithInvalidList8Type() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST8, false);
    }

    @Test
    public void testSkipValueWithInvalidList0Type() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST0, false);
    }

    @Test
    public void testSkipValueWithInvalidList32TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST32, true);
    }

    @Test
    public void testSkipValueWithInvalidList8TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST8, true);
    }

    @Test
    public void testSkipValueWithInvalidList0TypeFromStream() throws IOException {
        doTestSkipValueWithInvalidListType(EncodingCodes.LIST0, true);
    }

    private void doTestSkipValueWithInvalidListType(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(MessageAnnotations.DESCRIPTOR_CODE.byteValue());
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

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(MessageAnnotations.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(MessageAnnotations.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testSkipValueWithNullMapEncoding() throws IOException {
        doTestSkipValueWithNullMapEncoding(false);
    }

    @Test
    public void testSkipValueWithNullMapEncodingFromStream() throws IOException {
        doTestSkipValueWithNullMapEncoding(true);
    }

    private void doTestSkipValueWithNullMapEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(MessageAnnotations.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(MessageAnnotations.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
            } catch (DecodeException ex) {
                fail("Should be able to skip type with null inner encoding");
            }
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(MessageAnnotations.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(buffer, decoderState);
            } catch (DecodeException ex) {
                fail("Should be able to skip type with null inner encoding");
            }
        }
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        doTestEncodeDecodeArray(false);
    }

    @Test
    public void testEncodeDecodeArrayFromStream() throws IOException {
        doTestEncodeDecodeArray(true);
    }

    private void doTestEncodeDecodeArray(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        MessageAnnotations[] array = new MessageAnnotations[3];

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("1"), Boolean.TRUE);
        map.put(Symbol.valueOf("2"), Boolean.FALSE);

        array[0] = new MessageAnnotations(new HashMap<>());
        array[1] = new MessageAnnotations(map);
        array[2] = new MessageAnnotations(map);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(MessageAnnotations.class, result.getClass().getComponentType());

        MessageAnnotations[] resultArray = (MessageAnnotations[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof MessageAnnotations);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }

    @Test
    public void testEncodeAndDecodeAnnoationsWithEmbeddedMaps() throws IOException {
        doTestEncodeAndDecodeAnnoationsWithEmbeddedMaps(false);
    }

    @Test
    public void testEncodeAndDecodeAnnoationsWithEmbeddedMapsFromStream() throws IOException {
        doTestEncodeAndDecodeAnnoationsWithEmbeddedMaps(true);
    }

    private void doTestEncodeAndDecodeAnnoationsWithEmbeddedMaps(boolean fromStream) throws IOException {
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

        MessageAnnotations annotations = new MessageAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, stringKeyedMap);
        annotations.getValue().put(SYMBOL_2, symbolKeyedMap);

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        encoder.writeObject(buffer, encoderState, annotations);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof MessageAnnotations);

        MessageAnnotations readAnnotations = (MessageAnnotations) result;

        Map<Symbol, Object> resultMap = readAnnotations.getValue();

        assertEquals(annotations.getValue().size(), resultMap.size());
        assertEquals(resultMap.get(SYMBOL_1), stringKeyedMap);
        assertEquals(resultMap.get(SYMBOL_2), symbolKeyedMap);
    }
}
