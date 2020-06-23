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
package org.apache.qpid.proton4j.codec.decoders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.junit.Test;

public class ProtonDecoderTest extends CodecTestSupport {

    @Test
    public void testReadNullFromReadObjectForNullEncodng() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);
        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readObject(buffer, decoderState));
        assertNull(decoder.readObject(buffer, decoderState, UUID.class));
    }

    @Test
    public void testErrorOnReadOfUnknownEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(255);

        assertNull(decoder.peekNextTypeDecoder(buffer, decoderState));

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should throw if no type decoder exists for given type");
        } catch (DecodeException ioe) {}
    }

    @Test
    public void testReadFromNullEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        try {
            decoder.readObject(buffer, decoderState, String.class);
            fail("Should not allow for conversion to String type");
        } catch (ClassCastException cce) {
        }
    }

    @Test
    public void testReadMultipleFromNullEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readMultiple(buffer, decoderState, UUID.class));
    }

    @Test
    public void testReadMultipleFromSingleEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        UUID[] result = decoder.readMultiple(buffer, decoderState, UUID.class);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals(value, result[0]);
    }

    @Test
    public void testReadMultipleRequestsWrongTypeForArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID value = UUID.randomUUID();

        buffer.writeByte(EncodingCodes.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());

        try {
            decoder.readMultiple(buffer, decoderState, String.class);
            fail("Should not be able to convert to wrong resulting array type");
        } catch (ClassCastException cce) {}
    }

    @Test
    public void testReadMultipleRequestsWrongTypeForArrayEncoding() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final UUID[] value = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };

        encoder.writeArray(buffer, encoderState, value);

        try {
            decoder.readMultiple(buffer, decoderState, String.class);
            fail("Should not be able to convert to wrong resulting array type");
        } catch (ClassCastException cce) {}
    }
}
