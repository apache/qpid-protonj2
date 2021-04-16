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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.NullTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.primitives.NullTypeEncoder;
import org.junit.jupiter.api.Test;

public class NullTypeCodecTest extends CodecTestSupport {

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.NULL, new NullTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Void.class, new NullTypeEncoder().getTypeClass());
        assertEquals(Void.class, new NullTypeDecoder().getTypeClass());
    }

    @Test
    public void testWriteOfArrayThrowsException() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);

        try {
            new NullTypeEncoder().writeArray(buffer, encoderState, new Object[1]);
            fail("Null encoder cannot write array types");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testWriteRawOfArrayThrowsException() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);

        try {
            new NullTypeEncoder().writeRawArray(buffer, encoderState, new Object[1]);
            fail("Null encoder cannot write array types");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testReadNullDoesNotTouchBuffer() throws IOException {
        testReadNullDoesNotTouchBuffer(false);
    }

    @Test
    public void testReadNullDoesNotTouchBufferFS() throws IOException {
        testReadNullDoesNotTouchBuffer(true);
    }

    private void testReadNullDoesNotTouchBuffer(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertNull(streamDecoder.readObject(stream, streamDecoderState));
        } else {
            assertNull(decoder.readObject(buffer, decoderState));
        }
    }

    @Test
    public void testSkipNullDoesNotTouchBuffer() throws IOException {
        doTestSkipNullDoesNotTouchBuffer(false);
    }

    @Test
    public void testSkipNullDoesNotTouchStream() throws IOException {
        doTestSkipNullDoesNotTouchBuffer(true);
    }

    private void doTestSkipNullDoesNotTouchBuffer(boolean fromStream) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
            assertEquals(Void.class, typeDecoder.getTypeClass());
            int index = buffer.getReadIndex();
            typeDecoder.skipValue(stream, streamDecoderState);
            assertEquals(index, buffer.getReadIndex());
        } else {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Void.class, typeDecoder.getTypeClass());
            int index = buffer.getReadIndex();
            typeDecoder.skipValue(buffer, decoderState);
            assertEquals(index, buffer.getReadIndex());
        }
    }
}
