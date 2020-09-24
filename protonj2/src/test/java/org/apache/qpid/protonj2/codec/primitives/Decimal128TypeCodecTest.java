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
package org.apache.qpid.protonj2.codec.primitives;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Decimal128TypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.Decimal128TypeEncoder;
import org.apache.qpid.protonj2.types.Decimal128;
import org.junit.jupiter.api.Test;

public class Decimal128TypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readDecimal128(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testTypeFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.DECIMAL128);
        buffer.writeLong(42);
        buffer.writeLong(43);
        buffer.writeByte(EncodingCodes.NULL);

        Decimal128 result = decoder.readDecimal128(buffer, decoderState);

        assertEquals(42, result.getMostSignificantBits());
        assertEquals(43, result.getLeastSignificantBits());

        assertNull(decoder.readDecimal128(buffer, decoderState));
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.DECIMAL128, (byte) new Decimal128TypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Decimal128.class, new Decimal128TypeEncoder().getTypeClass());
        assertEquals(Decimal128.class, new Decimal128TypeDecoder().getTypeClass());
    }

    @Test
    public void testReadFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.DECIMAL128);
        buffer.writeLong(42);
        buffer.writeLong(84);

        Decimal128 result = decoder.readDecimal128(buffer, decoderState);

        assertEquals(42, result.getMostSignificantBits());
        assertEquals(84, result.getLeastSignificantBits());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeDecimal128(buffer, encoderState, new Decimal128(Long.MAX_VALUE - i, 42));
            encoder.writeDecimal128(buffer, encoderState, new Decimal128(i, i));
        }

        Decimal128 expected = new Decimal128(42, 42);

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Decimal128.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Decimal128.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Decimal128);

        Decimal128 value = (Decimal128) result;
        assertEquals(expected, value);
    }
}
