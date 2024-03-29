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
import org.apache.qpid.protonj2.codec.decoders.messaging.FooterTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.FooterTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

public class FooterTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Footer.class, new FooterTypeEncoder().getTypeClass());
        assertEquals(Footer.class, new FooterTypeDecoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Footer.DESCRIPTOR_CODE, new FooterTypeEncoder().getDescriptorCode());
        assertEquals(Footer.DESCRIPTOR_CODE, new FooterTypeDecoder().getDescriptorCode());
        assertEquals(Footer.DESCRIPTOR_SYMBOL, new FooterTypeEncoder().getDescriptorSymbol());
        assertEquals(Footer.DESCRIPTOR_SYMBOL, new FooterTypeDecoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeSmallSeriesOfFooter() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfFooter() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfFooterFromStream() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfFooterFromStream() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, false);
    }

    private void doTestDecodeHeaderSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        Footer properties = new Footer(propertiesMap);

        propertiesMap.put(Symbol.valueOf("key-1"), "1");
        propertiesMap.put(Symbol.valueOf("key-2"), "2");
        propertiesMap.put(Symbol.valueOf("key-3"), "3");
        propertiesMap.put(Symbol.valueOf("key-4"), "4");
        propertiesMap.put(Symbol.valueOf("key-5"), "5");
        propertiesMap.put(Symbol.valueOf("key-6"), "6");
        propertiesMap.put(Symbol.valueOf("key-7"), "7");
        propertiesMap.put(Symbol.valueOf("key-8"), "8");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
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
            assertTrue(result instanceof Footer);

            Footer decoded = (Footer) result;

            assertEquals(8, decoded.getValue().size());
            assertTrue(decoded.getValue().equals(propertiesMap));
        }
    }

    @Test
    public void testDecodeFailsWhenDescriedValueIsNotMapType() throws IOException {
        doTestDecodeFailsWhenDescriedValueIsNotMapType(false);
    }

    @Test
    public void testDecodeFailsWhenDescriedValueIsNotMapTypeFromStream() throws IOException {
        doTestDecodeFailsWhenDescriedValueIsNotMapType(true);
    }

    private void doTestDecodeFailsWhenDescriedValueIsNotMapType(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(0);
        buffer.writeInt(0);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
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
    public void testDecodeWithNullBodyUsingDescriptorCode() throws IOException {
        doTestDecodeWithNullBodyUsingDescriptorCode(false);
    }

    @Test
    public void testDecodeWithNullBodyUsingDescriptorCodeFromStream() throws IOException {
        doTestDecodeWithNullBodyUsingDescriptorCode(true);
    }

    private void doTestDecodeWithNullBodyUsingDescriptorCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        final Footer result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = (Footer) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Footer) decoder.readObject(buffer, decoderState);
        }

        assertNull(result.getValue());
    }

    @Test
    public void testDecodeWithNullBodyUsingDescriptorSymbol() throws IOException {
        testDecodeWithNullBodyUsingDescriptorSymbol(false);
    }

    @Test
    public void testDecodeWithNullBodyUsingDescriptorSymbolFromStream() throws IOException {
        testDecodeWithNullBodyUsingDescriptorSymbol(true);
    }

    private void testDecodeWithNullBodyUsingDescriptorSymbol(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SYM8);
        buffer.writeByte((byte) Footer.DESCRIPTOR_SYMBOL.getLength());
        Footer.DESCRIPTOR_SYMBOL.writeTo(buffer);
        buffer.writeByte(EncodingCodes.NULL);

        final Footer result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = (Footer) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Footer) decoder.readObject(buffer, decoderState);
        }

        assertNull(result.getValue());
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
            encoder.writeObject(buffer, encoderState, new Footer(map));
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
                assertEquals(Footer.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Footer.class, typeDecoder.getTypeClass());
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
    public void testEncodeDecodeFooterWithEmptyValue() throws IOException {
        doTestEncodeDecodeFooterWithEmptyValue(false);
    }

    @Test
    public void testEncodeDecodeFooterWithEmptyValueFromStream() throws IOException {
        doTestEncodeDecodeFooterWithEmptyValue(true);
    }

    private void doTestEncodeDecodeFooterWithEmptyValue(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        encoder.writeObject(buffer, encoderState, new Footer(null));

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Footer);

        Footer readAnnotations = (Footer) result;
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
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
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
            assertEquals(Footer.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Footer.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Footer.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
            } catch (DecodeException ex) {
                fail("Should be able to skip type with null inner encoding");
            }
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Footer.class, typeDecoder.getTypeClass());

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

        Footer[] array = new Footer[3];

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("1"), Boolean.TRUE);
        map.put(Symbol.valueOf("2"), Boolean.FALSE);

        array[0] = new Footer(new HashMap<>());
        array[1] = new Footer(map);
        array[2] = new Footer(map);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Footer.class, result.getClass().getComponentType());

        Footer[] resultArray = (Footer[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Footer);
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

    private void doTestEncodeAndDecodeAnnotationsWithEmbeddedMaps(boolean fromStream) throws IOException {
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

        Footer annotations = new Footer(new HashMap<>());
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
        assertTrue(result instanceof Footer);

        Footer readAnnotations = (Footer) result;

        Map<Symbol, Object> resultMap = readAnnotations.getValue();

        assertEquals(annotations.getValue().size(), resultMap.size());
        assertEquals(resultMap.get(SYMBOL_1), stringKeyedMap);
        assertEquals(resultMap.get(SYMBOL_2), symbolKeyedMap);
    }

    @Test
    public void testScanEncodedFooterForSpecificKey() throws IOException {
        doTestScanEncodedFooterForSpecificKey(false);
    }

    @Test
    public void testScanEncodedFooterForSpecificKeyFromStream() throws IOException {
        doTestScanEncodedFooterForSpecificKey(true);
    }

    private void doTestScanEncodedFooterForSpecificKey(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        Footer properties = new Footer(propertiesMap);

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

        final FooterTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<Symbol> context = FooterTypeDecoder.createStreamScanContext(searchDomain);
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.set(true);
            });
        } else {
            final ScanningContext<Symbol> context = FooterTypeDecoder.createScanContext(searchDomain);
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
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
    public void testScanEncodedFooterForSpecificKeys() throws IOException {
        doTestScanEncodedFooterForSpecificKeys(false);
    }

    @Test
    public void testScanEncodedFooterForSpecificKeysFromStream() throws IOException {
        doTestScanEncodedFooterForSpecificKeys(true);
    }

    private void doTestScanEncodedFooterForSpecificKeys(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        Footer properties = new Footer(propertiesMap);

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

        final FooterTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<Symbol> context = FooterTypeDecoder.createStreamScanContext(searchDomain);
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                matchFound.incrementAndGet();
                if (!Symbol.valueOf("key-2").equals(k) && !Symbol.valueOf("key-7").equals(k)) {
                    fail("Should not find any matches");
                }
            });
        } else {
            final ScanningContext<Symbol> context = FooterTypeDecoder.createScanContext(searchDomain);
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
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
    public void testScanEncodedFooterNotTriggeredOnNearMatch() throws IOException {
        doTestScanEncodedFooterNotTriggeredOnNearMatchFromStream(false);
    }

    @Test
    public void testScanEncodedFooterNotTriggeredOnNearMatchFromStream() throws IOException {
        doTestScanEncodedFooterNotTriggeredOnNearMatchFromStream(true);
    }

    private void doTestScanEncodedFooterNotTriggeredOnNearMatchFromStream(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        Footer properties = new Footer(propertiesMap);

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

        final FooterTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<Symbol> context = FooterTypeDecoder.createStreamScanContext(searchDomain);
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
        } else {
            final ScanningContext<Symbol> context = FooterTypeDecoder.createScanContext(searchDomain);
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
        }
    }

    @Test
    public void testScanMultipleEncodedFooterForSpecificKey() throws IOException {
        doTestScanMultipleEncodedFooterForSpecificKey(false);
    }

    @Test
    public void testScanMultipleEncodedFooterForSpecificKeyFromStream() throws IOException {
        doTestScanMultipleEncodedFooterForSpecificKey(true);
    }

    private void doTestScanMultipleEncodedFooterForSpecificKey(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap = new LinkedHashMap<>();
        Footer properties = new Footer(propertiesMap);

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

        FooterTypeDecoder result;

        if (fromStream) {
            final StreamScanningContext<Symbol> context = FooterTypeDecoder.createStreamScanContext(searchDomain);
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
        } else {
            final ScanningContext<Symbol> context = FooterTypeDecoder.createScanContext(searchDomain);
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                assertEquals(Symbol.valueOf("key-2"), k);
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
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
    public void testScanEncodedFooterWithNoMatchConsumesEncoding() throws IOException {
        doTestScanEncodedFooterWithNoMatchConsumesEncoding(false);
    }

    @Test
    public void testScanEncodedFooterWithNoMatchConsumesEncodingFromStream() throws IOException {
        doTestScanEncodedFooterWithNoMatchConsumesEncoding(true);
    }

    private void doTestScanEncodedFooterWithNoMatchConsumesEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<Symbol, Object> propertiesMap1 = new LinkedHashMap<>();
        Footer properties1 = new Footer(propertiesMap1);
        propertiesMap1.put(Symbol.valueOf("key-1"), "1");
        propertiesMap1.put(Symbol.valueOf("key-2"), "2");
        propertiesMap1.put(Symbol.valueOf("key-3"), "3");
        Map<Symbol, Object> propertiesMap2 = new LinkedHashMap<>();
        Footer properties2 = new Footer(propertiesMap2);
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

        FooterTypeDecoder result;

        if (fromStream) {
            final StreamScanningContext<Symbol> context = FooterTypeDecoder.createStreamScanContext(searchDomain);
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanAnnotations(stream, streamDecoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
            result = (FooterTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            Footer decoded = result.readValue(stream, streamDecoderState);
            assertEquals(propertiesMap2, decoded.getValue());
        } else {
            final ScanningContext<Symbol> context = FooterTypeDecoder.createScanContext(searchDomain);
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanAnnotations(buffer, decoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
            result = (FooterTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            Footer decoded = result.readValue(buffer, decoderState);
            assertEquals(propertiesMap2, decoded.getValue());
        }
    }
}
