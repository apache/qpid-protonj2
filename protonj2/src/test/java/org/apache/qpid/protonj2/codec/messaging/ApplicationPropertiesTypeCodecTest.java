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
import org.apache.qpid.protonj2.codec.decoders.messaging.ApplicationPropertiesTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.ApplicationPropertiesTypeEncoder;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

public class ApplicationPropertiesTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(ApplicationProperties.class, new ApplicationPropertiesTypeDecoder().getTypeClass());
        assertEquals(ApplicationProperties.class, new ApplicationPropertiesTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(ApplicationProperties.DESCRIPTOR_CODE, new ApplicationPropertiesTypeDecoder().getDescriptorCode());
        assertEquals(ApplicationProperties.DESCRIPTOR_CODE, new ApplicationPropertiesTypeEncoder().getDescriptorCode());
        assertEquals(ApplicationProperties.DESCRIPTOR_SYMBOL, new ApplicationPropertiesTypeDecoder().getDescriptorSymbol());
        assertEquals(ApplicationProperties.DESCRIPTOR_SYMBOL, new ApplicationPropertiesTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeSmallSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeSmallSeriesOfApplicationPropertiesFromStream() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, true);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationPropertiesFromStream() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeHeaderSeries(int size, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

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
            final ApplicationProperties result;
            if (fromStream) {
                result = streamDecoder.readObject(stream, streamDecoderState, ApplicationProperties.class);
            } else {
                result = decoder.readObject(buffer, decoderState, ApplicationProperties.class);
            }

            assertNotNull(result);
            assertEquals(8, result.getValue().size());
            assertTrue(result.getValue().equals(propertiesMap));
        }
    }

    @Test
    public void testEncodeDecodeZeroSizedArrayOfApplicationProperties() throws IOException {
        doTestEncodeDecodeZeroSizedArrayOfApplicationProperties(false);
    }

    @Test
    public void testEncodeDecodeZeroSizedArrayOfApplicationPropertiesFromStream() throws IOException {
        doTestEncodeDecodeZeroSizedArrayOfApplicationProperties(true);
    }

    private void doTestEncodeDecodeZeroSizedArrayOfApplicationProperties(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        ApplicationProperties[] array = new ApplicationProperties[0];

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(ApplicationProperties.class, result.getClass().getComponentType());

        ApplicationProperties[] resultArray = (ApplicationProperties[]) result;
        assertEquals(0, resultArray.length);
    }

    @Test
    public void testEncodeDecodeArrayOfApplicationProperties() throws IOException {
        testEncodeDecodeArrayOfApplicationProperties(false);
    }

    @Test
    public void testEncodeDecodeArrayOfApplicationPropertiesFromStream() throws IOException {
        testEncodeDecodeArrayOfApplicationProperties(true);
    }

    private void testEncodeDecodeArrayOfApplicationProperties(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        ApplicationProperties[] array = new ApplicationProperties[3];

        array[0] = new ApplicationProperties(new HashMap<String, Object>());
        array[1] = new ApplicationProperties(new HashMap<String, Object>());
        array[2] = new ApplicationProperties(new HashMap<String, Object>());

        array[0].getValue().put("key-1", "1");
        array[1].getValue().put("key-1", "2");
        array[2].getValue().put("key-1", "3");

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(ApplicationProperties.class, result.getClass().getComponentType());

        ApplicationProperties[] resultArray = (ApplicationProperties[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof ApplicationProperties);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
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

        Map<String, Object> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", Boolean.TRUE);
        map.put("three", "test");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new ApplicationProperties(map));
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
                assertEquals(ApplicationProperties.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(ApplicationProperties.class, typeDecoder.getTypeClass());
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

        encoder.writeObject(buffer, encoderState, new ApplicationProperties(null));

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof ApplicationProperties);

        ApplicationProperties readAnnotations = (ApplicationProperties) result;
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
        buffer.writeByte(ApplicationProperties.DESCRIPTOR_CODE.byteValue());
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
            assertEquals(ApplicationProperties.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(ApplicationProperties.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(ApplicationProperties.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(ApplicationProperties.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
            } catch (DecodeException ex) {
                fail("Should be able to skip type with null inner encoding");
            }
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(ApplicationProperties.class, typeDecoder.getTypeClass());

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

        ApplicationProperties[] array = new ApplicationProperties[3];

        Map<String, Object> map = new HashMap<>();
        map.put("1", Boolean.TRUE);
        map.put("2", Boolean.FALSE);

        array[0] = new ApplicationProperties(new HashMap<>());
        array[1] = new ApplicationProperties(map);
        array[2] = new ApplicationProperties(map);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(ApplicationProperties.class, result.getClass().getComponentType());

        ApplicationProperties[] resultArray = (ApplicationProperties[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof ApplicationProperties);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
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
        buffer.writeByte(ApplicationProperties.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof ApplicationProperties);

        ApplicationProperties decoded = (ApplicationProperties) result;
        assertNull(decoded.getValue());
    }

    @Test
    public void testReadTypeWithOverLargeEncoding() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(ApplicationProperties.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.MAP32);
        buffer.writeInt(Integer.MAX_VALUE);  // Size
        buffer.writeInt(4);  // Count

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testScanEncodedApplicationPropertiesForSpecificKey() throws IOException {
        doTestScanEncodedApplicationPropertiesForSpecificKey(false);
    }

    @Test
    public void testScanEncodedApplicationPropertiesForSpecificKeyFromStream() throws IOException {
        doTestScanEncodedApplicationPropertiesForSpecificKey(true);
    }

    private void doTestScanEncodedApplicationPropertiesForSpecificKey(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        final Collection<String> searchDomain = new ArrayList<>();
        searchDomain.add("key-2");

        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final AtomicBoolean matchFound = new AtomicBoolean();

        final ApplicationPropertiesTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<String> context = ApplicationPropertiesTypeDecoder.createStreamScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanProperties(stream, streamDecoderState, context, (k, v) -> {
                assertEquals("2", v);
                matchFound.set(true);
            });
        } else {
            final ScanningContext<String> context = ApplicationPropertiesTypeDecoder.createScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanProperties(buffer, decoderState, context, (k, v) -> {
                assertEquals("2", v);
                matchFound.set(true);
            });
        }

        assertTrue(matchFound.get());
    }

    @Test
    public void testScanEncodedApplicationPropertiesForSpecificKeys() throws IOException {
        doTestScanEncodedApplicationPropertiesForSpecificKeys(false);
    }

    @Test
    public void testScanEncodedApplicationPropertiesForSpecificKeysFromStream() throws IOException {
        doTestScanEncodedApplicationPropertiesForSpecificKeys(true);
    }

    private void doTestScanEncodedApplicationPropertiesForSpecificKeys(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        final Collection<String> searchDomain = new ArrayList<>();
        searchDomain.add("key-2");
        searchDomain.add("key-7");

        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final AtomicInteger matchFound = new AtomicInteger();

        final ApplicationPropertiesTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<String> context = ApplicationPropertiesTypeDecoder.createStreamScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanProperties(stream, streamDecoderState, context, (k, v) -> {
                matchFound.incrementAndGet();
                if (!"key-2".equals(k) && !"key-7".equals(k)) {
                    fail("Should not find any matches");
                }
            });
        } else {
            final ScanningContext<String> context = ApplicationPropertiesTypeDecoder.createScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanProperties(buffer, decoderState, context, (k, v) -> {
                matchFound.incrementAndGet();
                if (!"key-2".equals(k) && !"key-7".equals(k)) {
                    fail("Should not find any matches");
                }
            });
        }

        assertEquals(2, matchFound.get());
    }

    @Test
    public void testScanEncodedApplicationPropertiesNotTriggeredOnNearMatch() throws IOException {
        doTestScanEncodedApplicationPropertiesNotTriggeredOnNearMatchFromStream(false);
    }

    @Test
    public void testScanEncodedApplicationPropertiesNotTriggeredOnNearMatchFromStream() throws IOException {
        doTestScanEncodedApplicationPropertiesNotTriggeredOnNearMatchFromStream(true);
    }

    private void doTestScanEncodedApplicationPropertiesNotTriggeredOnNearMatchFromStream(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-21", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        final Collection<String> searchDomain = new ArrayList<>();
        searchDomain.add("key-2");

        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final ApplicationPropertiesTypeDecoder result;
        if (fromStream) {
            final StreamScanningContext<String> context = ApplicationPropertiesTypeDecoder.createStreamScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanProperties(stream, streamDecoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
        } else {
            final ScanningContext<String> context = ApplicationPropertiesTypeDecoder.createScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanProperties(buffer, decoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
        }
    }

    @Test
    public void testScanMultipleEncodedApplicationPropertiesForSpecificKey() throws IOException {
        doTestScanMultipleEncodedApplicationPropertiesForSpecificKey(false);
    }

    @Test
    public void testScanMultipleEncodedApplicationPropertiesForSpecificKeyFromStream() throws IOException {
        doTestScanMultipleEncodedApplicationPropertiesForSpecificKey(true);
    }

    private void doTestScanMultipleEncodedApplicationPropertiesForSpecificKey(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        final Collection<String> searchDomain = new ArrayList<>();
        searchDomain.add("key-2");

        encoder.writeObject(buffer, encoderState, properties);
        encoder.writeObject(buffer, encoderState, properties);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        final AtomicInteger matchFound = new AtomicInteger();

        ApplicationPropertiesTypeDecoder result;

        if (fromStream) {
            final StreamScanningContext<String> context = ApplicationPropertiesTypeDecoder.createStreamScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanProperties(stream, streamDecoderState, context, (k, v) -> {
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanProperties(stream, streamDecoderState, context, (k, v) -> {
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
        } else {
            final ScanningContext<String> context = ApplicationPropertiesTypeDecoder.createScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanProperties(buffer, decoderState, context, (k, v) -> {
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanProperties(buffer, decoderState, context, (k, v) -> {
                assertEquals("2", v);
                matchFound.incrementAndGet();
            });
        }

        assertEquals(2, matchFound.get());
    }

    @Test
    public void testScanEncodedApplicationPropertiesWithNoMatchConsumesEncoding() throws IOException {
        doTestScanEncodedApplicationPropertiesWithNoMatchConsumesEncoding(false);
    }

    @Test
    public void testScanEncodedApplicationPropertiesWithNoMatchConsumesEncodingFromStream() throws IOException {
        doTestScanEncodedApplicationPropertiesWithNoMatchConsumesEncoding(true);
    }

    private void doTestScanEncodedApplicationPropertiesWithNoMatchConsumesEncoding(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Map<String, Object> propertiesMap1 = new LinkedHashMap<>();
        ApplicationProperties properties1 = new ApplicationProperties(propertiesMap1);
        propertiesMap1.put("key-1", "1");
        propertiesMap1.put("key-2", "2");
        propertiesMap1.put("key-3", "3");
        Map<String, Object> propertiesMap2 = new LinkedHashMap<>();
        ApplicationProperties properties2 = new ApplicationProperties(propertiesMap2);
        propertiesMap2.put("key-4", "4");
        propertiesMap2.put("key-5", "5");
        propertiesMap2.put("key-6", "6");

        final Collection<String> searchDomain = new ArrayList<>();
        searchDomain.add("key-9");

        encoder.writeObject(buffer, encoderState, properties1);
        encoder.writeObject(buffer, encoderState, properties2);

        final InputStream stream;

        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        ApplicationPropertiesTypeDecoder result;

        if (fromStream) {
            final StreamScanningContext<String> context = ApplicationPropertiesTypeDecoder.createStreamScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            result.scanProperties(stream, streamDecoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
            result = (ApplicationPropertiesTypeDecoder) streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertNotNull(result);
            ApplicationProperties decoded = result.readValue(stream, streamDecoderState);
            assertEquals(propertiesMap2, decoded.getValue());
        } else {
            final ScanningContext<String> context = ApplicationPropertiesTypeDecoder.createScanContext(searchDomain);
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            result.scanProperties(buffer, decoderState, context, (k, v) -> {
                fail("Should not find any matches");
            });
            result = (ApplicationPropertiesTypeDecoder) decoder.readNextTypeDecoder(buffer, decoderState);
            assertNotNull(result);
            ApplicationProperties decoded = result.readValue(buffer, decoderState);
            assertEquals(propertiesMap2, decoded.getValue());
        }
    }
}
