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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.DispositionTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transport.DispositionTypeEncoder;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DispositionTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Disposition.class, new DispositionTypeDecoder().getTypeClass());
        assertEquals(Disposition.class, new DispositionTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Disposition.DESCRIPTOR_CODE, new DispositionTypeDecoder().getDescriptorCode());
        assertEquals(Disposition.DESCRIPTOR_CODE, new DispositionTypeEncoder().getDescriptorCode());
        assertEquals(Disposition.DESCRIPTOR_SYMBOL, new DispositionTypeDecoder().getDescriptorSymbol());
        assertEquals(Disposition.DESCRIPTOR_SYMBOL, new DispositionTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testEncodeAndDecodeWithNullState() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition input = new Disposition();

        input.setFirst(1);
        input.setRole(Role.RECEIVER);
        input.setBatchable(false);
        input.setSettled(true);
        input.setState(null);

        encoder.writeObject(buffer, encoderState, input);

        final Disposition result = (Disposition) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getFirst());
        assertEquals(Role.RECEIVER, result.getRole());
        assertEquals(false, result.getBatchable());
        assertEquals(true, result.getSettled());
        assertNull(result.getState());
    }

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition input = new Disposition();

        input.setFirst(1);
        input.setRole(Role.RECEIVER);
        input.setBatchable(false);
        input.setSettled(true);
        input.setState(Accepted.getInstance());

        encoder.writeObject(buffer, encoderState, input);

        final Disposition result = (Disposition) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getFirst());
        assertEquals(Role.RECEIVER, result.getRole());
        assertEquals(false, result.getBatchable());
        assertEquals(true, result.getSettled());
        assertSame(Accepted.getInstance(), result.getState());
    }

    @Disabled("Need to decide how and when to validate mandatory fields")
    @Test
    public void testDecodeEnforcesFirstValueRequired() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition input = new Disposition();

        input.setRole(Role.RECEIVER);
        input.setSettled(true);
        input.setState(Accepted.getInstance());

        // TODO - Probably should throw here too
        encoder.writeObject(buffer, encoderState, input);

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not encode when no First value is set");
        } catch (Exception ex) {
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition disposition = new Disposition();

        disposition.setFirst(1);
        disposition.setLast(2);
        disposition.setRole(Role.RECEIVER);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, disposition);
        }

        disposition.setFirst(2);
        disposition.setLast(3);
        disposition.setRole(Role.SENDER);
        disposition.setState(Accepted.getInstance());

        encoder.writeObject(buffer, encoderState, disposition);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Disposition.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Disposition);

        Disposition value = (Disposition) result;
        assertEquals(2, value.getFirst());
        assertEquals(3, value.getLast());
        assertEquals(Role.SENDER, value.getRole());
        assertSame(Accepted.getInstance(), value.getState());
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
        buffer.writeByte(Disposition.DESCRIPTOR_CODE.byteValue());
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
        assertEquals(Disposition.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
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
        buffer.writeByte(Disposition.DESCRIPTOR_CODE.byteValue());
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
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition[] array = new Disposition[3];

        array[0] = new Disposition();
        array[1] = new Disposition();
        array[2] = new Disposition();

        array[0].setFirst(0).setRole(Role.SENDER).setSettled(true);
        array[1].setFirst(1).setRole(Role.RECEIVER).setSettled(false);
        array[2].setFirst(2).setRole(Role.SENDER).setSettled(true);

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Disposition.class, result.getClass().getComponentType());

        Disposition[] resultArray = (Disposition[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Disposition);
            assertEquals(array[i].getFirst(), resultArray[i].getFirst());
            assertEquals(array[i].getRole(), resultArray[i].getRole());
            assertEquals(array[i].getSettled(), resultArray[i].getSettled());
        }
    }
}
