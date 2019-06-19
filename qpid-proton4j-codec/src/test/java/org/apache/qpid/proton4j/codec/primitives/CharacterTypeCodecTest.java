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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.CharacterTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.CharacterTypeEncoder;
import org.junit.Test;

public class CharacterTypeCodecTest extends CodecTestSupport {

    @Test
    public void testGetTypeCode() {
        assertEquals(EncodingCodes.CHAR, (byte) new CharacterTypeDecoder().getTypeCode());
    }

    @Test
    public void testGetTypeClass() {
        assertEquals(Character.class, new CharacterTypeEncoder().getTypeClass());
        assertEquals(Character.class, new CharacterTypeDecoder().getTypeClass());
    }

    @Test
    public void testReadCharFromEncodingCode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.CHAR);
        buffer.writeInt(42);

        assertEquals(42, decoder.readCharacter(buffer, decoderState).charValue());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeCharacter(buffer, encoderState, Character.MAX_VALUE);
            encoder.writeCharacter(buffer, encoderState, (char) 16);
        }

        char expected = 42;

        encoder.writeObject(buffer, encoderState, expected);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Character.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
            typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Character.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Character);

        Character value = (Character) result;
        assertEquals(expected, value.charValue());
    }
}
