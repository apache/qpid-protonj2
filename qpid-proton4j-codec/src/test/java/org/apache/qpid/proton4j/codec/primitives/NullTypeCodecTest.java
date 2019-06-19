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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.NullTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.NullTypeEncoder;
import org.junit.Test;

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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);

        buffer.writeByte(EncodingCodes.NULL);

        assertNull(decoder.readObject(buffer, decoderState));
    }

    @Test
    public void testSkipNullDoesNotTouchBuffer() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.NULL);

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(Void.class, typeDecoder.getTypeClass());

        int index = buffer.getReadIndex();
        typeDecoder.skipValue(buffer, decoderState);
        assertEquals(index, buffer.getReadIndex());
    }
}
