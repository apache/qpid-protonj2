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
package org.apache.qpid.protonj2.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class ProtonBufferInputStreamTest {

    @Test
    public void testBufferWappedExposesAvailableBytes() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());

        stream.close();
    }

    @Test
    public void testMarkReadIndex() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertTrue(stream.markSupported());
        assertEquals(payload.length, stream.available());
        assertEquals(payload[0], stream.readByte());

        stream.mark(100);

        assertEquals(payload[1], stream.readByte());

        stream.reset();

        assertEquals(payload[1], stream.readByte());
        assertEquals(payload[2], stream.readByte());

        stream.close();
    }
}
