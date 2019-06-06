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

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.decoders.security.SaslChallengeTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton4j.codec.encoders.security.SaslChallengeTypeEncoder;
import org.junit.Before;
import org.junit.Test;

public class SaslChallengeTypeCodecTest extends CodecTestSupport {

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
        assertEquals(SaslChallenge.class, new SaslChallengeTypeDecoder().getTypeClass());
        assertEquals(SaslChallenge.class, new SaslChallengeTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        SaslChallengeTypeDecoder decoder = new SaslChallengeTypeDecoder();
        SaslChallengeTypeEncoder encoder = new SaslChallengeTypeEncoder();

        assertEquals(SaslChallenge.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(SaslChallenge.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(SaslChallenge.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(SaslChallenge.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        byte[] challenge = new byte[] { 1, 2, 3, 4 };

        SaslChallenge input = new SaslChallenge();
        input.setChallenge(new Binary(challenge));

        encoder.writeObject(buffer, encoderState, input);

        final SaslChallenge result = (SaslChallenge) decoder.readObject(buffer, decoderState);

        assertArrayEquals(challenge, result.getChallenge().getArray());
    }
}
