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
import org.apache.qpid.proton4j.codec.decoders.primitives.FloatTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.FloatTypeEncoder;
import org.junit.Test;

public class FloatTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readFloat(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}

        try {
            decoder.readFloat(buffer, decoderState, 0f);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.FLOAT);
        buffer.writeFloat(42.0f);
        buffer.writeByte(EncodingCodes.FLOAT);
        buffer.writeFloat(43.0f);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(42f, decoder.readFloat(buffer, decoderState).shortValue(), 0.0f);
        assertEquals(43f, decoder.readFloat(buffer, decoderState, (short) 42), 0.0f);
        assertNull(decoder.readFloat(buffer, decoderState));
        assertEquals(43f, decoder.readFloat(buffer, decoderState, 43f), 0.0f);
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.FLOAT, (byte) new FloatTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Float.class, new FloatTypeEncoder().getTypeClass());
        assertEquals(Float.class, new FloatTypeDecoder().getTypeClass());
    }

    @Test
    public void testReadFloatFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.FLOAT);
        buffer.writeFloat(42);

        assertEquals(42, decoder.readFloat(buffer, decoderState).intValue());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeFloat(buffer, encoderState, Float.MAX_VALUE);
            encoder.writeFloat(buffer, encoderState, 16.1f);
        }

        float expected = 42;

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Float.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Float.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Float);

        Float value = (Float) result;
        assertEquals(expected, value.floatValue(), 0.1f);
    }
}
