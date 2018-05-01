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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

/**
 * Test the Binary codec for correctness
 */
public class BinaryTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeEmptyArrayBinary() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Binary input = new Binary(new byte[0]);

        encoder.writeBinary(buffer, encoderState, input);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(0, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
    }

    @Test
    public void testEncodeDecodeBinary() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        Binary input = new Binary(new byte[] {0, 1, 2, 3, 4});

        encoder.writeBinary(buffer, encoderState, input);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Binary);
        Binary output = (Binary) result;

        assertEquals(5, output.getLength());
        assertEquals(0, output.getArrayOffset());
        assertNotNull(output.getArray());
    }
}
