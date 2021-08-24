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
import java.util.LinkedHashMap;
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt(0);
        buffer.writeInt(0);

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
    public void testDecodeWithNullBodyUsingDescriptorCode() throws IOException {
        doTestDecodeWithNullBodyUsingDescriptorCode(false);
    }

    @Test
    public void testDecodeWithNullBodyUsingDescriptorCodeFromStream() throws IOException {
        doTestDecodeWithNullBodyUsingDescriptorCode(true);
    }

    private void doTestDecodeWithNullBodyUsingDescriptorCode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        final Footer result;
        if (fromStream) {
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SYM8);
        buffer.writeByte(Footer.DESCRIPTOR_SYMBOL.getLength());
        Footer.DESCRIPTOR_SYMBOL.writeTo(buffer);
        buffer.writeByte(EncodingCodes.NULL);

        final Footer result;
        if (fromStream) {
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Map<Symbol, Object> map = new HashMap<>();
        map.put(Symbol.valueOf("one"), 1);
        map.put(Symbol.valueOf("two"), Boolean.TRUE);
        map.put(Symbol.valueOf("three"), "test");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new Footer(map));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

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

        encoder.writeObject(buffer, encoderState, new Footer(null));

        final Object result;
        if (fromStream) {
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Footer.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

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
        assertTrue(result instanceof Footer);

        Footer readAnnotations = (Footer) result;

        Map<Symbol, Object> resultMap = readAnnotations.getValue();

        assertEquals(annotations.getValue().size(), resultMap.size());
        assertEquals(resultMap.get(SYMBOL_1), stringKeyedMap);
        assertEquals(resultMap.get(SYMBOL_2), symbolKeyedMap);
    }
}
