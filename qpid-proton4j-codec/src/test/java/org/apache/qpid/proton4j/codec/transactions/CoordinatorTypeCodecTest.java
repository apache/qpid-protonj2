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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.decoders.transactions.CoordinatorTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.CoordinatorTypeEncoder;
import org.junit.Test;

/**
 * Test for handling Coordinator serialization
 */
public class CoordinatorTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Coordinator.class, new CoordinatorTypeDecoder().getTypeClass());
        assertEquals(Coordinator.class, new CoordinatorTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        CoordinatorTypeDecoder decoder = new CoordinatorTypeDecoder();
        CoordinatorTypeEncoder encoder = new CoordinatorTypeEncoder();

        assertEquals(Coordinator.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(Coordinator.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(Coordinator.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(Coordinator.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeCoordinatorType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] capabilities = new Symbol[] { Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2") };

        Coordinator input = new Coordinator();
        input.setCapabilities(capabilities);

        encoder.writeObject(buffer, encoderState, input);

        final Coordinator result = (Coordinator) decoder.readObject(buffer, decoderState);

        assertArrayEquals(capabilities, result.getCapabilities());
    }
}
