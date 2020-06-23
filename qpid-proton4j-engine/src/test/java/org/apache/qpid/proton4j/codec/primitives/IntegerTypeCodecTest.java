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
package org.apache.qpid.proton4j.codec.primitives;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Integer32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Integer8TypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.IntegerTypeEncoder;
import org.junit.Test;

public class IntegerTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readInteger(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}

        try {
            decoder.readInteger(buffer, decoderState, (short) 0);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(42);
        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(44);
        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte(43);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(42, decoder.readInteger(buffer, decoderState).intValue());
        assertEquals(44, decoder.readInteger(buffer, decoderState, 42));
        assertEquals(43, decoder.readInteger(buffer, decoderState, 42));
        assertNull(decoder.readInteger(buffer, decoderState));
        assertEquals(42, decoder.readInteger(buffer, decoderState, 42));
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.INT, (byte) new Integer32TypeDecoder().getTypeCode());
        assertEquals(EncodingCodes.SMALLINT, (byte) new Integer8TypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Integer.class, new IntegerTypeEncoder().getTypeClass());
        assertEquals(Integer.class, new Integer8TypeDecoder().getTypeClass());
        assertEquals(Integer.class, new Integer32TypeDecoder().getTypeClass());
    }

    @Test
    public void testReadIntegerFromEncodingCodeInt() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.INT);
        buffer.writeInt(42);

        assertEquals(42, decoder.readInteger(buffer, decoderState).intValue());
    }

    @Test
    public void testReadIntegerFromEncodingCodeSmallInt() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.SMALLINT);
        buffer.writeByte(42);

        assertEquals(42, decoder.readInteger(buffer, decoderState).intValue());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeInteger(buffer, encoderState, Integer.MAX_VALUE);
            encoder.writeInteger(buffer, encoderState, 16);
        }

        int expected = 42;

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Integer.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Integer.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Integer);

        Integer value = (Integer) result;
        assertEquals(expected, value.intValue());
    }
}
