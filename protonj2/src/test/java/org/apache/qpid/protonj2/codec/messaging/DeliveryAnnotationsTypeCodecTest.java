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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ScanningContext;
import org.apache.qpid.protonj2.codec.decoders.StreamScanningContext;
import org.apache.qpid.protonj2.codec.decoders.messaging.DeliveryAnnotationsTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DeliveryAnnotationsTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

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
        doTestDecodeDeliveryAnnotationsSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfDeliveryAnnotations() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeDeliveryAnnotations() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(1, false);
    }

    @Test
    public void testDecodeSmallSeriesOfDeliveryAnnotationsFromStream() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfDeliveryAnnotationsFromStream() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(LARGE_SIZE, true);
    }

    @Test
    public void testDecodeDeliveryAnnotationsFromStream() throws IOException {
        doTestDecodeDeliveryAnnotationsSeries(1, true);
    }

    private void doTestDecodeDeliveryAnnotationsSeries(int size, boolean fromStream) throws IOException {
        final Symbol SYMBOL_1 = Symbol.valueOf("test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("test2");
        final Symbol SYMBOL_3 = Symbol.valueOf("test3");
        final Symbol SYMBOL_4 = Symbol.valueOf("test4");

        DeliveryAnnotations annotations = new DeliveryAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(SYMBOL_2, UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(SYMBOL_3, UnsignedInteger.valueOf(128));
        annotations.getValue().put(SYMBOL_4, UnsignedLong.valueOf(128));

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, annotations);
        }

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState);
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof DeliveryAnnotations);

            DeliveryAnnotations readAnnotations = (DeliveryAnnotations) result;

            Map<Symbol, Object> resultMap = readAnnotations.getValue();

            assertEquals(annotations.getValue().size(), resultMap.size());
            assertEquals(resultMap.get(SYMBOL_1), UnsignedByte.valueOf((byte) 128));
            assertEquals(resultMap.get(SYMBOL_2), UnsignedShort.valueOf((short) 128));
            assertEquals(resultMap.get(SYMBOL_3), UnsignedInteger.valueOf(128));
            assertEquals(resultMap.get(SYMBOL_4), UnsignedLong.valueOf(128));
        }
    }

    @Test
    public void testEncodeDecodeDeliveryAnnotationsArray() throws IOException {
        doTestEncodeDecodeDeliveryAnnotationsArray(false);
    }

    @Test
    public void testEncodeDecodeDeliveryAnnotationsArrayFromStream() throws IOException {
        doTestEncodeDecodeDeliveryAnnotationsArray(true);
    }

    private void doTestEncodeDecodeDeliveryAnnotationsArray(boolean fromStream) throws IOException {
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

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

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
        doTestSkipValue(false);
    }

    @Test
    public void testSkipValueFromStream() throws IOException {
        doTestSkipValue(true);
    }

    private void doTestSkipValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("one"), 1);
        map.put(Symbol.valueOf("two"), Boolean.TRUE);
        map.put(Symbol.valueOf("three"), "test");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new DeliveryAnnotations(map));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());
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
    public void testEncodeDecodeMessageAnnotationsWithEmptyValue() throws IOException {
        doTestEncodeDecodeMessageAnnotationsWithEmptyValue(false);
    }

    @Test
    public void testEncodeDecodeMessageAnnotationsWithEmptyValueFromStream() throws IOException {
        doTestEncodeDecodeMessageAnnotationsWithEmptyValue(true);
    }

    private void doTestEncodeDecodeMessageAnnotationsWithEmptyValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, new DeliveryAnnotations(null));

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof DeliveryAnnotations);

        DeliveryAnnotations readAnnotations = (DeliveryAnnotations) result;
        assertNull(readAnnotations.getValue());
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

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

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeliveryAnnotations.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
            } catch (DecodeException ex) {
                fail("Should be able to skip type with null inner encoding");
            }
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(DeliveryAnnotations.class, typeDecoder.getTypeClass());

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        DeliveryAnnotations[] array = new DeliveryAnnotations[3];

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("1"), Boolean.TRUE);
        map.put(Symbol.valueOf("2"), Boolean.FALSE);

        array[0] = new DeliveryAnnotations(new HashMap<>());
        array[1] = new DeliveryAnnotations(map);
        array[2] = new DeliveryAnnotations(map);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

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
    public void testEncodeAndDecodeAnnotationsWithEmbeddedMaps() throws IOException {
        doTestEncodeAndDecodeAnnotationsWithEmbeddedMaps(false);
    }

    @Test
    public void testEncodeAndDecodeAnnotationsWithEmbeddedMapsFromStream() throws IOException {
        doTestEncodeAndDecodeAnnotationsWithEmbeddedMaps(true);
    }

    public void doTestEncodeAndDecodeAnnotationsWithEmbeddedMaps(boolean fromStream) throws IOException {
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

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, annotations);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof DeliveryAnnotations);

        DeliveryAnnotations readAnnotations = (DeliveryAnnotations) result;

        Map<Symbol, Object> resultMap = readAnnotations.getValue();

        assertEquals(annotations.getValue().size(), resultMap.size());
        assertEquals(resultMap.get(SYMBOL_1), stringKeyedMap);
        assertEquals(resultMap.get(SYMBOL_2), symbolKeyedMap);
    }

    @Test
    public void testReadTypeWithNullEncoding() throws IOException {
        testReadTypeWithNullEncoding(false);
    }

    @Test
    public void testReadTypeWithNullEncodingFromStream() throws IOException {
        testReadTypeWithNullEncoding(true);
    }

    private void testReadTypeWithNullEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeliveryAnnotations.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof DeliveryAnnotations);

        DeliveryAnnotations decoded = (DeliveryAnnotations) result;
        assertNull(decoded.getValue());
    }

    @Test
    public void testReadTypeWithOverLargeEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeliveryAnnotations.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.MAP32);
        buffer.writeInt(Integer.MAX_VALUE);  // Size
        buffer.writeInt(4);  // Count

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsForSpecificKey() throws IOException {
        doTestScanEncodedDeliveryAnnotationsForSpecificKey(false);
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsForSpecificKeyFromStream() throws IOException {
        doTestScanEncodedDeliveryAnnotationsForSpecificKey(true);
    }

    private void doTestScanEncodedDeliveryAnnotationsForSpecificKey(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        DeliveryAnnotations properties = new DeliveryAnnotations(propertiesMap);

        propertiesMap.put(Symbol.valueOf("key-1"), "1");
        propertiesMap.put(Symbol.valueOf("key-2"), "2");
        propertiesMap.put(Symbol.valueOf("key-3"), "3");
        propertiesMap.put(Symbol.valueOf("key-4"), "4");
        propertiesMap.put(Symbol.valueOf("key-5"), "5");
        propertiesMap.put(Symbol.valueOf("key-6"), "6");
        propertiesMap.put(Symbol.valueOf("key-7"), "7");
        propertiesMap.put(Symbol.valueOf("key-8"), "8");

        final Collection<Symbol> searchDomain = new ArrayList<>();
        searchDomain.add(Symbol.valueOf("key-2"));

        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final AtomicBoolean matchFound = new AtomicBoolean();

        final DeliveryAnnotationsTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createStreamScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.set(true);
            });
        } else {
            final ScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.set(true);
            });
        }

        assertTrue(matchFound.get());
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsForSpecificKeys() throws IOException {
        doTestScanEncodedDeliveryAnnotationsForSpecificKeys(false);
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsForSpecificKeysFromStream() throws IOException {
        doTestScanEncodedDeliveryAnnotationsForSpecificKeys(true);
    }

    private void doTestScanEncodedDeliveryAnnotationsForSpecificKeys(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        DeliveryAnnotations properties = new DeliveryAnnotations(propertiesMap);

        propertiesMap.put(Symbol.valueOf("key-1"), "1");
        propertiesMap.put(Symbol.valueOf("key-2"), "2");
        propertiesMap.put(Symbol.valueOf("key-3"), "3");
        propertiesMap.put(Symbol.valueOf("key-4"), "4");
        propertiesMap.put(Symbol.valueOf("key-5"), "5");
        propertiesMap.put(Symbol.valueOf("key-6"), "6");
        propertiesMap.put(Symbol.valueOf("key-7"), "7");
        propertiesMap.put(Symbol.valueOf("key-8"), "8");

        final Collection<Symbol> searchDomain = new ArrayList<>();
        searchDomain.add(Symbol.valueOf("key-2"));
        searchDomain.add(Symbol.valueOf("key-7"));

        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final AtomicInteger matchFound = new AtomicInteger();

        final DeliveryAnnotationsTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createStreamScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                matchFound.incrementAndGet();
                if (!Symbol.valueOf("key-2").equals(k) && !Symbol.valueOf("key-7").equals(k)) {
                    fail("Should not find any matches");
                }
            });
        } else {
            final ScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                matchFound.incrementAndGet();
                if (!Symbol.valueOf("key-2").equals(k) && !Symbol.valueOf("key-7").equals(k)) {
                    fail("Should not find any matches");
                }
            });
        }

        assertEquals(2, matchFound.get());
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsNotTriggeredOnNearMatch() throws IOException {
        doTestScanEncodedDeliveryAnnotationsNotTriggeredOnNearMatchFromStream(false);
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsNotTriggeredOnNearMatchFromStream() throws IOException {
        doTestScanEncodedDeliveryAnnotationsNotTriggeredOnNearMatchFromStream(true);
    }

    private void doTestScanEncodedDeliveryAnnotationsNotTriggeredOnNearMatchFromStream(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        DeliveryAnnotations properties = new DeliveryAnnotations(propertiesMap);

        propertiesMap.put(Symbol.valueOf("key-1"), "1");
        propertiesMap.put(Symbol.valueOf("key-21"), "2");
        propertiesMap.put(Symbol.valueOf("key-3"), "3");
        propertiesMap.put(Symbol.valueOf("key-4"), "4");
        propertiesMap.put(Symbol.valueOf("key-5"), "5");
        propertiesMap.put(Symbol.valueOf("key-6"), "6");
        propertiesMap.put(Symbol.valueOf("key-7"), "7");
        propertiesMap.put(Symbol.valueOf("key-8"), "8");

        final Collection<Symbol> searchDomain = new ArrayList<>();
        searchDomain.add(Symbol.valueOf("key-2"));

        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final DeliveryAnnotationsTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createStreamScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
        } else {
            final ScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
        }
    }

    @Test
    public void testScanMultipleEncodedDeliveryAnnotationsForSpecificKey() throws IOException {
        doTestScanMultipleEncodedDeliveryAnnotationsForSpecificKey(false);
    }

    @Test
    public void testScanMultipleEncodedDeliveryAnnotationsForSpecificKeyFromStream() throws IOException {
        doTestScanMultipleEncodedDeliveryAnnotationsForSpecificKey(true);
    }

    private void doTestScanMultipleEncodedDeliveryAnnotationsForSpecificKey(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        DeliveryAnnotations properties = new DeliveryAnnotations(propertiesMap);

        propertiesMap.put(Symbol.valueOf("key-1"), "1");
        propertiesMap.put(Symbol.valueOf("key-2"), "2");
        propertiesMap.put(Symbol.valueOf("key-3"), "3");
        propertiesMap.put(Symbol.valueOf("key-4"), "4");
        propertiesMap.put(Symbol.valueOf("key-5"), "5");
        propertiesMap.put(Symbol.valueOf("key-6"), "6");
        propertiesMap.put(Symbol.valueOf("key-7"), "7");
        propertiesMap.put(Symbol.valueOf("key-8"), "8");

        final Collection<Symbol> searchDomain = new ArrayList<>();
        searchDomain.add(Symbol.valueOf("key-2"));

        encoder.writeObject(buffer, encoderState, properties);
        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final AtomicInteger matchFound = new AtomicInteger();

        DeliveryAnnotationsTypeDecoder result;

        if (fromStream) {
            final StreamScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createStreamScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
        } else {
            final ScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
        }

        assertEquals(2, matchFound.get());
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsWithNoMatchConsumesEncoding() throws IOException {
        doTestScanEncodedDeliveryAnnotationsWithNoMatchConsumesEncoding(false);
    }

    @Test
    public void testScanEncodedDeliveryAnnotationsWithNoMatchConsumesEncodingFromStream() throws IOException {
        doTestScanEncodedDeliveryAnnotationsWithNoMatchConsumesEncoding(true);
    }

    private void doTestScanEncodedDeliveryAnnotationsWithNoMatchConsumesEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap1 = new LinkedHashMap<>();
        DeliveryAnnotations properties1 = new DeliveryAnnotations(propertiesMap1);
        propertiesMap1.put(Symbol.valueOf("key-1"), "1");
        propertiesMap1.put(Symbol.valueOf("key-2"), "2");
        propertiesMap1.put(Symbol.valueOf("key-3"), "3");
        Map<Symbol, Object> propertiesMap2 = new LinkedHashMap<>();
        DeliveryAnnotations properties2 = new DeliveryAnnotations(propertiesMap2);
        propertiesMap2.put(Symbol.valueOf("key-4"), "4");
        propertiesMap2.put(Symbol.valueOf("key-5"), "5");
        propertiesMap2.put(Symbol.valueOf("key-6"), "6");

        final Collection<Symbol> searchDomain = new ArrayList<>();
        searchDomain.add(Symbol.valueOf("key-9"));

        encoder.writeObject(buffer, encoderState, properties1);
        encoder.writeObject(buffer, encoderState, properties2);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        DeliveryAnnotationsTypeDecoder result;

        if (fromStream) {
            final StreamScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createStreamScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
            result = (DeliveryAnnotationsTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            DeliveryAnnotations decoded = result.readValue(stream, streamDecoderState);
            assertEquals(propertiesMap2, decoded.getValue());
        } else {
            final ScanningContext<Symbol> context = DeliveryAnnotationsTypeDecoder.createScanContext(searchDomain);
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
            result = (DeliveryAnnotationsTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            DeliveryAnnotations decoded = result.readValue(buffer, decoderState);
            assertEquals(propertiesMap2, decoded.getValue());
        }
    }
}
