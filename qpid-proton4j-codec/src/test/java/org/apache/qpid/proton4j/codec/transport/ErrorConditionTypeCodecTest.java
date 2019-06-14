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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.ErrorConditionTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transport.ErrorConditionTypeEncoder;
import org.junit.Test;

public class ErrorConditionTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(ErrorCondition.class, new ErrorConditionTypeDecoder().getTypeClass());
        assertEquals(ErrorCondition.class, new ErrorConditionTypeEncoder().getTypeClass());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Object, Object> infoMap = new LinkedHashMap<>();
        infoMap.put("1", true);
        infoMap.put("2", "string");

        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "Something bad", infoMap);

        encoder.writeObject(buffer, encoderState, error);

        final ErrorCondition result = (ErrorCondition) decoder.readObject(buffer, decoderState);

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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Object, Object> infoMap = new LinkedHashMap<>();
        infoMap.put("1", true);
        infoMap.put("2", "string");

        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), "Something bad", infoMap);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, error);
        }

        error = new ErrorCondition(Symbol.valueOf("amqp-error-2"), "Something bad also", null);

        encoder.writeObject(buffer, encoderState, error);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(ErrorCondition.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

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

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(ErrorCondition.class, typeDecoder.getTypeClass());

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

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (IOException ex) {}
    }
}
