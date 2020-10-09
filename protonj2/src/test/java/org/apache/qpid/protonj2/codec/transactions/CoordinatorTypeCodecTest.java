/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.codec.transactions;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.CoordinatorTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.CoordinatorTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.junit.jupiter.api.Test;

/**
 * Test for handling Coordinator serialization
 */
public class CoordinatorTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Coordinator.class, new CoordinatorTypeDecoder().getTypeClass());
        assertEquals(Coordinator.class, new CoordinatorTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        CoordinatorTypeDecoder decoder = new CoordinatorTypeDecoder();
        CoordinatorTypeEncoder encoder = new CoordinatorTypeEncoder();

        assertEquals(Coordinator.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(Coordinator.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(Coordinator.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(Coordinator.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeCoordinatorType() throws Exception {
        testEncodeDecodeCoordinatorType(false);
    }

    @Test
    public void testEncodeDecodeCoordinatorTypeFromStream() throws Exception {
        testEncodeDecodeCoordinatorType(true);
    }

    private void testEncodeDecodeCoordinatorType(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Symbol[] capabilities = new Symbol[] { Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2") };

        Coordinator input = new Coordinator();
        input.setCapabilities(capabilities);

        encoder.writeObject(buffer, encoderState, input);

        final Coordinator result;
        if (fromStream) {
            result = (Coordinator) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Coordinator) decoder.readObject(buffer, decoderState);
        }

        assertArrayEquals(capabilities, result.getCapabilities());
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

        Coordinator coordinator = new Coordinator();

        coordinator.setCapabilities(Symbol.valueOf("skip"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, coordinator);
        }

        coordinator.setCapabilities(Symbol.valueOf("read"));

        encoder.writeObject(buffer, encoderState, coordinator);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Coordinator.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Coordinator result;
        if (fromStream) {
            result = (Coordinator) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Coordinator) decoder.readObject(buffer, decoderState);
        }

        assertNotNull(result);
        assertTrue(result instanceof Coordinator);

        Coordinator value = result;
        assertEquals(1, value.getCapabilities().length);
        assertEquals(Symbol.valueOf("read"), value.getCapabilities()[0]);
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
        buffer.writeByte(Coordinator.DESCRIPTOR_CODE.byteValue());
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
            assertEquals(Coordinator.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Coordinator.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Coordinator.DESCRIPTOR_CODE.byteValue());
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
            if (fromStream) {
                streamDecoder.readObject(stream, streamDecoderState);
            } else {
                decoder.readObject(buffer, decoderState);
            }
            fail("Should have failed to decode the Coordinator");
        } catch (DecodeException ex) {
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

        Coordinator[] array = new Coordinator[3];

        array[0] = new Coordinator();
        array[1] = new Coordinator();
        array[2] = new Coordinator();

        array[0].setCapabilities(Symbol.valueOf("1"));
        array[1].setCapabilities(Symbol.valueOf("1"), Symbol.valueOf("2"));
        array[2].setCapabilities(Symbol.valueOf("1"), Symbol.valueOf("2"), Symbol.valueOf("3"));

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Coordinator.class, result.getClass().getComponentType());

        Coordinator[] resultArray = (Coordinator[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Coordinator);
            assertArrayEquals(array[i].getCapabilities(), resultArray[i].getCapabilities());
        }
    }
}
