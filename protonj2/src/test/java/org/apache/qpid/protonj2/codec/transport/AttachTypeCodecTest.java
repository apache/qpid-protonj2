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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.AttachTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.AttachTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.Terminus;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.junit.jupiter.api.Test;

public class AttachTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Attach.class, new AttachTypeDecoder().getTypeClass());
        assertEquals(Attach.class, new AttachTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Attach.DESCRIPTOR_CODE, new AttachTypeDecoder().getDescriptorCode());
        assertEquals(Attach.DESCRIPTOR_CODE, new AttachTypeEncoder().getDescriptorCode());
        assertEquals(Attach.DESCRIPTOR_SYMBOL, new AttachTypeDecoder().getDescriptorSymbol());
        assertEquals(Attach.DESCRIPTOR_SYMBOL, new AttachTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testCannotEncodeEmptyPerformative() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Attach input = new Attach();

        try {
            encoder.writeObject(buffer, encoderState, input);
            fail("Cannot omit required fields.");
        } catch (EncodeException encEx) {
        }
    }

    @Test
    public void testEncodeDecodeTypeWithTarget() throws Exception {
        doTestEncodeDecodeType(new Target(), false);
    }

    @Test
    public void testEncodeDecodeTypeWithCoordinator() throws Exception {
        doTestEncodeDecodeType(new Coordinator(), false);
    }

    @Test
    public void testEncodeDecodeTypeWithTargetFromStream() throws Exception {
        doTestEncodeDecodeType(new Target(), true);
    }

    @Test
    public void testEncodeDecodeTypeWithCoordinatorFromStream() throws Exception {
        doTestEncodeDecodeType(new Coordinator(), true);
    }

    private void doTestEncodeDecodeType(Terminus target, boolean fromStream) throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
       Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

       final Random random = new Random();
       random.setSeed(System.nanoTime());

       final int randomHandle = random.nextInt();
       final int randomInitialDeliveryCount = random.nextInt();

       Attach input = new Attach();

       input.setName("name");
       input.setOfferedCapabilities(offeredCapabilities);
       input.setDesiredCapabilities(desiredCapabilities);
       input.setHandle(randomHandle);
       input.setRole(Role.RECEIVER);
       input.setSenderSettleMode(SenderSettleMode.UNSETTLED);
       input.setReceiverSettleMode(ReceiverSettleMode.SECOND);
       input.setSource(new Source());
       input.setTarget(target);
       input.setIncompleteUnsettled(false);
       input.setInitialDeliveryCount(randomInitialDeliveryCount);
       input.setMaxMessageSize(UnsignedLong.valueOf(1024));

       encoder.writeObject(buffer, encoderState, input);

       final Attach result;
       if (fromStream) {
           result = (Attach) streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = (Attach) decoder.readObject(buffer, decoderState);
       }

       assertEquals("name", result.getName());
       assertEquals(Integer.toUnsignedLong(randomHandle), result.getHandle());
       assertEquals(Role.RECEIVER, result.getRole());
       assertEquals(SenderSettleMode.UNSETTLED, result.getSenderSettleMode());
       assertEquals(ReceiverSettleMode.SECOND, result.getReceiverSettleMode());
       assertEquals(Integer.toUnsignedLong(randomInitialDeliveryCount), result.getInitialDeliveryCount());
       assertEquals(UnsignedLong.valueOf(1024), result.getMaxMessageSize());
       assertNotNull(result.getSource());
       assertNotNull(result.getTarget());
       assertFalse(result.getIncompleteUnsettled());
       assertNull(result.getUnsettled());
       assertNull(result.getProperties());
       assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
       assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }

    @Test
    public void testEncodeUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Attach input = new Attach();

        input.setName("name");
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setHandle(64);
        input.setRole(Role.RECEIVER);
        input.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        input.setReceiverSettleMode(ReceiverSettleMode.SECOND);
        input.setSource(new Source());
        input.setTarget(new Target());
        input.setIncompleteUnsettled(false);
        input.setInitialDeliveryCount(10);
        input.setMaxMessageSize(UnsignedLong.valueOf(1024));

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Attach);
        final Attach result = (Attach) decoded;
        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        testEncodeUsingLegacyCodecAndDecodeWithNewCodec(false);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodecFromStream() throws Exception {
        testEncodeUsingLegacyCodecAndDecodeWithNewCodec(true);
    }

    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec(boolean fromStream) throws Exception {
        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Attach input = new Attach();

        input.setName("name");
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);
        input.setHandle(64);
        input.setRole(Role.RECEIVER);
        input.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        input.setReceiverSettleMode(ReceiverSettleMode.SECOND);
        input.setSource(new Source());
        input.setTarget(new Target());
        input.setIncompleteUnsettled(false);
        input.setInitialDeliveryCount(10);
        input.setMaxMessageSize(UnsignedLong.valueOf(1024));

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        InputStream stream = new ProtonBufferInputStream(buffer);

        assertNotNull(buffer);

        final Attach result;
        if (fromStream) {
            result = (Attach) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (Attach) decoder.readObject(buffer, decoderState);
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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        Attach attach = new Attach();

        attach.setHandle(1);
        attach.setRole(Role.RECEIVER);
        attach.setName("skip");

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, attach);
        }

        attach.setHandle(2);
        attach.setRole(Role.SENDER);
        attach.setName("test");

        encoder.writeObject(buffer, encoderState, attach);

        for (int i = 0; i < 10; ++i) {
            if (fromStream) {
                StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
                assertEquals(Attach.class, typeDecoder.getTypeClass());
                typeDecoder.skipValue(stream, streamDecoderState);
            } else {
                TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
                assertEquals(Attach.class, typeDecoder.getTypeClass());
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
        assertTrue(result instanceof Attach);

        Attach value = (Attach) result;
        assertEquals(Role.SENDER, value.getRole());
        assertEquals(2, value.getHandle());
        assertEquals("test", value.getName());
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
        buffer.writeByte(Attach.DESCRIPTOR_CODE.byteValue());
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
            assertEquals(Attach.class, typeDecoder.getTypeClass());

            try {
                typeDecoder.skipValue(stream, streamDecoderState);
                fail("Should not be able to skip type with invalid encoding");
            } catch (DecodeException ex) {}
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Attach.class, typeDecoder.getTypeClass());

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
        buffer.writeByte(Attach.DESCRIPTOR_CODE.byteValue());
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

        Attach[] array = new Attach[3];

        array[0] = new Attach();
        array[1] = new Attach();
        array[2] = new Attach();

        array[0].setHandle(0).setName("0").setInitialDeliveryCount(0).setRole(Role.SENDER);
        array[1].setHandle(1).setName("1").setInitialDeliveryCount(1).setRole(Role.SENDER);
        array[2].setHandle(2).setName("2").setInitialDeliveryCount(2).setRole(Role.SENDER);

        encoder.writeObject(buffer, encoderState, array);

        final Object result;
        if (fromStream) {
            result = streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = decoder.readObject(buffer, decoderState);
        }

        assertTrue(result.getClass().isArray());
        assertEquals(Attach.class, result.getClass().getComponentType());

        Attach[] resultArray = (Attach[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Attach);
            assertEquals(array[i].getHandle(), resultArray[i].getHandle());
            assertEquals(array[i].getName(), resultArray[i].getName());
            assertEquals(array[i].getInitialDeliveryCount(), resultArray[i].getInitialDeliveryCount());
        }
    }
}
