/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton4j.codec.transactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DeclareTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DeclareTypeEncoder;
import org.junit.Test;

/**
 * Test for handling Declare serialization
 */
public class DeclareTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Declare.class, new DeclareTypeDecoder().getTypeClass());
        assertEquals(Declare.class, new DeclareTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        DeclareTypeDecoder decoder = new DeclareTypeDecoder();
        DeclareTypeEncoder encoder = new DeclareTypeEncoder();

        assertEquals(Declare.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(Declare.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(Declare.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(Declare.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Declare input = new Declare();

        encoder.writeObject(buffer, encoderState, input);

        final Declare result = (Declare) decoder.readObject(buffer, decoderState);

        assertNull(result.getGlobalId());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Declare declare = new Declare();

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, declare);
        }

        encoder.writeObject(buffer, encoderState, declare);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Declare.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Declare);

        Declare value = (Declare) result;
        assertNull(value.getGlobalId());
    }
}
