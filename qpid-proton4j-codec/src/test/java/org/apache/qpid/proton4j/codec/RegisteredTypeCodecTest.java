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
package org.apache.qpid.proton4j.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.util.NoLocalType;
import org.apache.qpid.proton4j.codec.util.NoLocalTypeDecoder;
import org.apache.qpid.proton4j.codec.util.NoLocalTypeEncoder;
import org.junit.Test;

/**
 * Test for handling of type when the Decoder / Encoder is registered
 */
public class RegisteredTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeRegistredType() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        // Register the codec pair.
        encoder.registerTypeEncoder(new NoLocalTypeEncoder());
        decoder.registerTypeDecoder(new NoLocalTypeDecoder());

        encoder.writeObject(buffer, encoderState, NoLocalType.NO_LOCAL);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof NoLocalType);
        NoLocalType resultTye = (NoLocalType) result;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }
}
