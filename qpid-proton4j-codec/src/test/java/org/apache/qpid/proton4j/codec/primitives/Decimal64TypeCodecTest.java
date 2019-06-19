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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal64TypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal64TypeEncoder;
import org.junit.Test;

public class Decimal64TypeCodecTest extends CodecTestSupport {

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.DECIMAL64, (byte) new Decimal64TypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Decimal64.class, new Decimal64TypeEncoder().getTypeClass());
        assertEquals(Decimal64.class, new Decimal64TypeDecoder().getTypeClass());
    }

    @Test
    public void testReadFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.DECIMAL64);
        buffer.writeLong(42);

        assertEquals(42, decoder.readDecimal64(buffer, decoderState).getBits());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeDecimal64(buffer, encoderState, new Decimal64(Long.MAX_VALUE - i));
            encoder.writeDecimal64(buffer, encoderState, new Decimal64(i));
        }

        Decimal64 expected = new Decimal64(42);

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Decimal64.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Decimal64.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Decimal64);

        Decimal64 value = (Decimal64) result;
        assertEquals(expected, value);
    }
}
