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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class BeginTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeType() throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
       Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};
       Map<Object, Object> properties = new HashMap<>();
       properties.put("property", "value");

       Begin input = new Begin();

       input.setRemoteChannel(UnsignedShort.valueOf((short) 16));
       input.setNextOutgoingId(UnsignedInteger.valueOf(24));
       input.setIncomingWindow(UnsignedInteger.valueOf(32));
       input.setOutgoingWindow(UnsignedInteger.valueOf(12));
       input.setHandleMax(UnsignedInteger.valueOf(255));
       input.setOfferedCapabilities(offeredCapabilities);
       input.setDesiredCapabilities(desiredCapabilities);
       input.setProperties(properties);

       encoder.writeObject(buffer, encoderState, input);

       final Begin result = (Begin) decoder.readObject(buffer, decoderState);

       assertEquals(UnsignedShort.valueOf((short) 16), result.getRemoteChannel());
       assertEquals(UnsignedInteger.valueOf(24), result.getNextOutgoingId());
       assertEquals(UnsignedInteger.valueOf(32), result.getIncomingWindow());
       assertEquals(UnsignedInteger.valueOf(12), result.getOutgoingWindow());
       assertEquals(UnsignedInteger.valueOf(255), result.getHandleMax());
       assertNotNull(result.getProperties());
       assertEquals(1, properties.size());
       assertTrue(properties.containsKey("property"));
       assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
       assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }
}
