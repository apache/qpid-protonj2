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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.BeginTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.BeginTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.junit.jupiter.api.Test;

public class BeginTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Begin.class, new BeginTypeDecoder().getTypeClass());
        assertEquals(Begin.class, new BeginTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Begin.DESCRIPTOR_CODE, new BeginTypeDecoder().getDescriptorCode());
        assertEquals(Begin.DESCRIPTOR_CODE, new BeginTypeEncoder().getDescriptorCode());
        assertEquals(Begin.DESCRIPTOR_SYMBOL, new BeginTypeDecoder().getDescriptorSymbol());
        assertEquals(Begin.DESCRIPTOR_SYMBOL, new BeginTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testCannotEncodeEmptyPerformative() throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Begin input = new Begin();

        try {
            encoder.writeObject(buffer, encoderState, input);
            fail("Cannot omit required fields.");
        } catch (EncodeException encEx) {
        }
    }

    @Test
    public void testEncodeDecodeType() throws IOException {
        doTestEncodeAndDecode(false);
    }

    @Test
    public void testEncodeDecodeTypeFromStream() throws IOException {
        doTestEncodeAndDecode(true);
    }

    private void doTestEncodeAndDecode(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Symbol[] offeredCapabilities = new Symbol[] { Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2") };
        Symbol[] desiredCapabilities = new Symbol[] { Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4") };
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("property"), "value");

        final Random random = new Random();
        random.setSeed(System.nanoTime());

        final int randomChannel = random.nextInt(65535);
        final int randomeNextOutgoingId = random.nextInt();
        final int randomeNextIncomingWindow = random.nextInt();
        final int randomeNextOutgoingWindow = random.nextInt();
        final int randomeHandleMax = random.nextInt();

        Begin input = new Begin();

        input.setRemoteChannel(randomChannel);
        input.setNextOutgoingId(randomeNextOutgoingId);
        input.setIncomingWindow(randomeNextIncomingWindow);
        input.setOutgoingWindow(randomeNextOutgoingWindow);
        input.setHandleMax(randomeHandleMax);
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setProperties(properties);

        encoder.writeObject(buffer, encoderState, input);

        final Begin result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = (Begin) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Begin) decoder.readObject(buffer, decoderState);
        }

        assertEquals(randomChannel, result.getRemoteChannel());
        assertEquals(Integer.toUnsignedLong(randomeNextOutgoingId), result.getNextOutgoingId());
        assertEquals(Integer.toUnsignedLong(randomeNextIncomingWindow), result.getIncomingWindow());
        assertEquals(Integer.toUnsignedLong(randomeNextOutgoingWindow), result.getOutgoingWindow());
        assertEquals(Integer.toUnsignedLong(randomeHandleMax), result.getHandleMax());
        assertNotNull(result.getProperties());
        assertEquals(1, properties.size());
        assertTrue(properties.containsKey(Symbol.valueOf("property")));
        assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
        assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }

    @Test
    public void testEncodeDecodeFailsOnMissingIncomingWindow() throws IOException {
        testEncodeDecodeFailsOnMissingIncomingWindow(false);
    }

    @Test
    public void testEncodeDecodeFailsOnMissingIncomingWindowFromStream() throws IOException {
        testEncodeDecodeFailsOnMissingIncomingWindow(true);
    }

    private void testEncodeDecodeFailsOnMissingIncomingWindow(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        final Random random = new Random();
        random.setSeed(System.nanoTime());

        final int randomChannel = random.nextInt(65535);
        final int randomeNextOutgoingId = random.nextInt();
        final int randomeNextOutgoingWindow = random.nextInt();
        final int randomeHandleMax = random.nextInt();

        Begin input = new Begin();

        input.setRemoteChannel(randomChannel);
        input.setNextOutgoingId(randomeNextOutgoingId);
        input.setOutgoingWindow(randomeNextOutgoingWindow);
        input.setHandleMax(randomeHandleMax);

        encoder.writeObject(buffer, encoderState, input);

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
    public void testEncodeUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};
        Map<Symbol, Object> properties = new LinkedHashMap<>();
        properties.put(Symbol.valueOf("property"), "value");

        Begin input = new Begin();

        input.setRemoteChannel(16);
        input.setNextOutgoingId(24);
        input.setIncomingWindow(32);
        input.setOutgoingWindow(12);
        input.setHandleMax(255);
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setProperties(properties);

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Begin);
        final Begin result = (Begin) decoded;

        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        doTestEncodeUsingLegacyCodecAndDecodeWithNewCodec(false);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodecFromStream() throws Exception {
        doTestEncodeUsingLegacyCodecAndDecodeWithNewCodec(true);
    }

    private void doTestEncodeUsingLegacyCodecAndDecodeWithNewCodec(boolean fromStream) throws Exception {
        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("property"), "value");

        Begin input = new Begin();

        input.setRemoteChannel(16);
        input.setNextOutgoingId(24);
        input.setIncomingWindow(32);
        input.setOutgoingWindow(12);
        input.setHandleMax(255);
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setProperties(properties);

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);

        assertNotNull(buffer);

        final Begin result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState, Begin.class);
        } else {
            result = decoder.readObject(buffer, decoderState, Begin.class);
        }

        assertNotNull(result);

        assertTypesEqual(input, result);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Begin begin = new Begin();

        begin.setRemoteChannel(1);
        begin.setNextOutgoingId(0);
        begin.setIncomingWindow(1024);
        begin.setOutgoingWindow(1024);
        begin.setHandleMax(25);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, begin);
        }

        begin.setRemoteChannel(2);
        begin.setNextOutgoingId(0);
        begin.setIncomingWindow(1024);
        begin.setOutgoingWindow(1024);
        begin.setHandleMax(50);

        encoder.writeObject(buffer, encoderState, begin);

        final InputStream stream;
        if (fromStream) {
            stream = new ProtonBufferInputStream(buffer);
        } else {
            stream = null;
        }

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Begin.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Begin.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Begin);

        Begin value = (Begin) result;
        assertEquals(2, value.getRemoteChannel());
        assertEquals(50, value.getHandleMax());
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Begin.DESCRIPTOR_CODE.byteValue());
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
            InputStream stream = new ProtonBufferInputStream(buffer);
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Begin.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Begin.class, typeDecoder.getTypeClass());

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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Begin.DESCRIPTOR_CODE.byteValue());
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
    public void testEncodeDecodeArray() throws IOException {
        testEncodeDecodeArray(false);
    }

    @Test
    public void testEncodeDecodeArrayFromStream() throws IOException {
        testEncodeDecodeArray(true);
    }

    private void testEncodeDecodeArray(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        Begin[] array = new Begin[3];

        array[0] = new Begin();
        array[1] = new Begin();
        array[2] = new Begin();

        array[0].setNextOutgoingId(0).setRemoteChannel(0).setIncomingWindow(0).setOutgoingWindow(0);
        array[1].setNextOutgoingId(1).setRemoteChannel(1).setIncomingWindow(1).setOutgoingWindow(1);
        array[2].setNextOutgoingId(2).setRemoteChannel(2).setIncomingWindow(2).setOutgoingWindow(2);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            InputStream stream = new ProtonBufferInputStream(buffer);
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Begin.class, result.getClass().getComponentType());

        Begin[] resultArray = (Begin[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Begin);
            assertEquals(array[i].getNextOutgoingId(), resultArray[i].getNextOutgoingId());
            assertEquals(array[i].getOutgoingWindow(), resultArray[i].getOutgoingWindow());
            assertEquals(array[i].getIncomingWindow(), resultArray[i].getIncomingWindow());
            assertEquals(array[i].getRemoteChannel(), resultArray[i].getRemoteChannel());
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Begin.DESCRIPTOR_CODE.byteValue());
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
            InputStream stream = new ProtonBufferInputStream(buffer);
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
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Begin.DESCRIPTOR_CODE.byteValue());
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
            InputStream stream = new ProtonBufferInputStream(buffer);
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
