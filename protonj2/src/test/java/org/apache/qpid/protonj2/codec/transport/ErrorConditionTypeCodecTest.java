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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.ErrorConditionTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.ErrorConditionTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.junit.jupiter.api.Test;

public class ErrorConditionTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(ErrorCondition.class, new ErrorConditionTypeDecoder().getTypeClass());
        assertEquals(ErrorCondition.class, new ErrorConditionTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(ErrorCondition.DESCRIPTOR_CODE, new ErrorConditionTypeDecoder().getDescriptorCode());
        assertEquals(ErrorCondition.DESCRIPTOR_CODE, new ErrorConditionTypeEncoder().getDescriptorCode());
        assertEquals(ErrorCondition.DESCRIPTOR_SYMBOL, new ErrorConditionTypeDecoder().getDescriptorSymbol());
        assertEquals(ErrorCondition.DESCRIPTOR_SYMBOL, new ErrorConditionTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testEncodeAndDecode() throws IOException {
        doTestEncodeAndDecode(false);
    }

    @Test
    public void testEncodeAndDecodeFromStream() throws IOException {
        doTestEncodeAndDecode(true);
    }

    private void doTestEncodeAndDecode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Map<Symbol, Object> infoMap = new LinkedHashMap<>();
        infoMap.put(Symbol.valueOf("1"), true);
        infoMap.put(Symbol.valueOf("2"), "string");

        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "Something bad", infoMap);

        encoder.writeObject(buffer, encoderState, error);

        final ErrorCondition result;
        if (fromStream) {
            result = (ErrorCondition) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (ErrorCondition) decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertNotNull(result.getCondition());
        assertNotNull(result.getDescription());
        assertNotNull(result.getInfo());

        assertEquals(Symbol.valueOf("amqp-error"), result.getCondition());
        assertEquals("Something bad", result.getDescription());
        assertEquals(infoMap, result.getInfo());
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

        Map<Symbol, Object> infoMap = new LinkedHashMap<>();
        infoMap.put(Symbol.valueOf("1"), true);
        infoMap.put(Symbol.valueOf("2"), "string");

        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "Something bad", infoMap);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, error);
        }

        error = new ErrorCondition(Symbol.valueOf("amqp-error-2"), "Something bad also", null);

        encoder.writeObject(buffer, encoderState, error);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(ErrorCondition.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(ErrorCondition.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof ErrorCondition);

        ErrorCondition value = (ErrorCondition) result;
        assertEquals(Symbol.valueOf("amqp-error-2"), value.getCondition());
        assertEquals("Something bad also", value.getDescription());
        assertNull(value.getInfo());
    }

    @Test
    public void testEqualityOfNewlyConstructed() {
        ErrorCondition new1 = new ErrorCondition(null, null, null);
        ErrorCondition new2 = new ErrorCondition(null, null, null);
        assertErrorConditionsEqual(new1, new2);
    }

    @Test
    public void testSameObject() {
        ErrorCondition error = new ErrorCondition(null, null, null);
        assertErrorConditionsEqual(error, error);
    }

    @Test
    public void testConditionEquality() {
        String symbolValue = "symbol";

        ErrorCondition same1 = new ErrorCondition(Symbol.valueOf(symbolValue), null);
        ErrorCondition same2 = new ErrorCondition(Symbol.valueOf(symbolValue), null);

        assertErrorConditionsEqual(same1, same2);

        ErrorCondition different = new ErrorCondition(Symbol.getSymbol("other"), null);

        assertErrorConditionsNotEqual(same1, different);
    }

    @Test
    public void testConditionAndDescriptionEquality() {
        String symbolValue = "symbol";
        String descriptionValue = "description";

        ErrorCondition same1 = new ErrorCondition(Symbol.getSymbol(new String(symbolValue)), new String(descriptionValue));
        ErrorCondition same2 = new ErrorCondition(Symbol.getSymbol(new String(symbolValue)), new String(descriptionValue));

        assertErrorConditionsEqual(same1, same2);

        ErrorCondition different = new ErrorCondition(Symbol.getSymbol(symbolValue), "other");

        assertErrorConditionsNotEqual(same1, different);
    }

    @Test
    public void testConditionDescriptionInfoEquality() {
        String symbolValue = "symbol";
        String descriptionValue = "description";

        ErrorCondition same1 = new ErrorCondition(
            Symbol.getSymbol(new String(symbolValue)), new String(descriptionValue), Collections.singletonMap(Symbol.getSymbol("key"), "value"));
        ErrorCondition same2 = new ErrorCondition(
            Symbol.getSymbol(new String(symbolValue)), new String(descriptionValue), Collections.singletonMap(Symbol.getSymbol("key"), "value"));

        assertErrorConditionsEqual(same1, same2);

        ErrorCondition different = new ErrorCondition(
            Symbol.getSymbol(symbolValue), new String(descriptionValue), Collections.singletonMap(Symbol.getSymbol("other"), "value"));

        assertErrorConditionsNotEqual(same1, different);
    }

    private void assertErrorConditionsNotEqual(ErrorCondition error1, ErrorCondition error2) {
        assertThat(error1, is(not(error2)));
        assertThat(error2, is(not(error1)));
    }

    private void assertErrorConditionsEqual(ErrorCondition error1, ErrorCondition error2) {
        assertEquals(error1, error2);
        assertEquals(error2, error1);
        assertEquals(error1.hashCode(), error2.hashCode());
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
        buffer.writeByte(ErrorCondition.DESCRIPTOR_CODE.byteValue());
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
            assertEquals(ErrorCondition.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(ErrorCondition.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(ErrorCondition.DESCRIPTOR_CODE.byteValue());
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

        ErrorCondition[] array = new ErrorCondition[3];

        array[0] = new ErrorCondition(AmqpError.DECODE_ERROR, "1");
        array[1] = new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "2");
        array[2] = new ErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED, "3");

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(ErrorCondition.class, result.getClass().getComponentType());

        ErrorCondition[] resultArray = (ErrorCondition[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof ErrorCondition);
            assertEquals(array[i], resultArray[i]);
        }
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList8() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST8, false);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList32() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST32, false);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList0FromStream() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST0, true);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList8FromStream() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeWithNotEnoughListEntriesList32FromStream() throws IOException {
        doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST32, true);
    }

    private void doTestDecodeWithNotEnoughListEntriesList32(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(ErrorCondition.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else if (listType == EncodingCodes.LIST8) {
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.LIST0);
        }

        if (fromStream) {
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        }
    }

    @Test
    public void testDecodeWithToManyListEntriesList8() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST8, false);
    }

    @Test
    public void testDecodeWithToManyListEntriesList32() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST32, false);
    }

    @Test
    public void testDecodeWithToManyListEntriesList8FromStream() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST8, true);
    }

    @Test
    public void testDecodeWithToManyListEntriesList32FromStream() throws IOException {
        doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST32, true);
    }

    private void doTestDecodeWithToManyListEntriesList32(byte listType, boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(ErrorCondition.DESCRIPTOR_CODE.byteValue());
        if (listType == EncodingCodes.LIST32) {
            buffer.writeByte(EncodingCodes.LIST32);
            buffer.writeInt(128);  // Size
            buffer.writeInt(127);  // Count
        } else if (listType == EncodingCodes.LIST8) {
            buffer.writeByte(EncodingCodes.LIST8);
            buffer.writeByte((byte) 128);  // Size
            buffer.writeByte((byte) 127);  // Count
        }

        if (fromStream) {
            try {
                streamDecoder.readObject(stream, streamDecoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        } else {
            try {
                decoder.readObject(buffer, decoderState);
                fail("Should not decode type with invalid min entries");
            } catch (DecodeException ex) {}
        }
    }
}
