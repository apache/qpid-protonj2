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
package org.apache.qpid.proton4j.codec.security;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.security.SaslCode;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.decoders.security.SaslOutcomeTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton4j.codec.encoders.security.SaslOutcomeTypeEncoder;
import org.junit.Before;
import org.junit.Test;

public class SaslOutcomeTypeCodecTest extends CodecTestSupport {

    @Override
    @Before
    public void setUp() {
        decoder = ProtonDecoderFactory.createSasl();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.createSasl();
        encoderState = encoder.newEncoderState();
    }

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(SaslOutcome.class, new SaslOutcomeTypeDecoder().getTypeClass());
        assertEquals(SaslOutcome.class, new SaslOutcomeTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        SaslOutcomeTypeDecoder decoder = new SaslOutcomeTypeDecoder();
        SaslOutcomeTypeEncoder encoder = new SaslOutcomeTypeEncoder();

        assertEquals(SaslOutcome.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(SaslOutcome.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(SaslOutcome.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(SaslOutcome.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        byte[] data = new byte[] { 1, 2, 3, 4 };
        SaslCode code = SaslCode.AUTH;

        SaslOutcome input = new SaslOutcome();
        input.setAdditionalData(new Binary(data));
        input.setCode(code);

        encoder.writeObject(buffer, encoderState, input);

        final SaslOutcome result = (SaslOutcome) decoder.readObject(buffer, decoderState);

        assertEquals(code, result.getCode());
        assertArrayEquals(data, result.getAdditionalData().getArray());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        SaslOutcome outcome = new SaslOutcome();

        outcome.setAdditionalData(new Binary(new byte[] {0}));
        outcome.setCode(SaslCode.AUTH);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, outcome);
        }

        outcome.setAdditionalData(new Binary(new byte[] {1, 2}));
        outcome.setCode(SaslCode.SYS_TEMP);

        encoder.writeObject(buffer, encoderState, outcome);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(SaslOutcome.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof SaslOutcome);

        SaslOutcome value = (SaslOutcome) result;
        assertArrayEquals(new byte[] {1, 2}, value.getAdditionalData().getArray());
        assertEquals(SaslCode.SYS_TEMP, value.getCode());
    }
}
