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
package org.apache.qpid.proton4j.codec.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class FlowTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Flow input = new Flow();

        input.setNextIncomingId(10);
        input.setIncomingWindow(20);
        input.setNextOutgoingId(30);
        input.setOutgoingWindow(40);
        input.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        input.setDeliveryCount(50);
        input.setLinkCredit(60);
        input.setAvailable(70);
        input.setDrain(true);
        input.setEcho(true);

        encoder.writeObject(buffer, encoderState, input);

        final Flow result = (Flow) decoder.readObject(buffer, decoderState);

        assertEquals(10, result.getNextIncomingId());
        assertEquals(20, result.getIncomingWindow());
        assertEquals(30, result.getNextOutgoingId());
        assertEquals(40, result.getOutgoingWindow());
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), result.getHandle());
        assertEquals(50, result.getDeliveryCount());
        assertEquals(60, result.getLinkCredit());
        assertEquals(70, result.getAvailable());
        assertTrue(input.getDrain());
        assertTrue(input.getEcho());
        assertNull(input.getProperties());
    }
}
