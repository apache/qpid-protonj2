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
import org.apache.qpid.proton4j.codec.decoders.primitives.DoubleTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.DoubleTypeEncoder;
import org.junit.Test;

public class DoubleTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);
        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readDouble(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}

        try {
            decoder.readDouble(buffer, decoderState, 0.0);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testReadUByteFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.DOUBLE);
        buffer.writeDouble(42.0);
        buffer.writeByte(EncodingCodes.DOUBLE);
        buffer.writeDouble(43.0);
        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertEquals(42.0, decoder.readDouble(buffer, decoderState).shortValue(), 0.0);
        assertEquals(43.0, decoder.readDouble(buffer, decoderState, 42.0), 0.0);
        assertNull(decoder.readDouble(buffer, decoderState));
        assertEquals(43.0, decoder.readDouble(buffer, decoderState, 43.0), 0.0);
    }

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.DOUBLE, (byte) new DoubleTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Double.class, new DoubleTypeEncoder().getTypeClass());
        assertEquals(Double.class, new DoubleTypeDecoder().getTypeClass());
    }

    @Test
    public void testReadDoubleFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.DOUBLE);
        buffer.writeDouble(42);

        assertEquals(42, decoder.readDouble(buffer, decoderState).intValue());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeDouble(buffer, encoderState, Double.MAX_VALUE);
            encoder.writeDouble(buffer, encoderState, 16.1);
        }

        double expected = 42;

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Double.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Double.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Double);

        Double value = (Double) result;
        assertEquals(expected, value.doubleValue(), 0.1f);
    }
}
