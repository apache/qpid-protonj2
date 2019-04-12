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

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class OpenTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setChannelMax(UnsignedShort.valueOf(512).intValue());
        input.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        input.setIdleTimeOut(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        encoder.writeObject(buffer, encoderState, input);

        final Open result = (Open) decoder.readObject(buffer, decoderState);

        assertEquals("test", result.getContainerId());
        assertEquals("localhost", result.getHostname());
        assertEquals(UnsignedShort.valueOf(512).intValue(), result.getChannelMax());
        assertEquals(UnsignedInteger.ONE.longValue(), result.getMaxFrameSize());
        assertEquals(UnsignedInteger.ZERO.longValue(), result.getIdleTimeOut());
        assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
        assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }

    @Test
    public void testEncodeUsingNewCodecAndDecodeWithLegacyCodec() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        input.setIdleTimeOut(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        encoder.writeObject(buffer, encoderState, input);
        Object decoded = legacyCodec.decodeLegacyType(buffer);
        assertTrue(decoded instanceof Open);
        final Open result = (Open) decoded;

        assertNotNull(result);
        assertTypesEqual(input, result);
    }

    @Test
    public void testEncodeUsingLegacyCodecAndDecodeWithNewCodec() throws Exception {
        Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
        Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

        Open input = new Open();

        input.setContainerId("test");
        input.setHostname("localhost");
        input.setMaxFrameSize(UnsignedInteger.ONE.longValue());
        input.setIdleTimeOut(UnsignedInteger.ZERO.longValue());
        input.setOfferedCapabilities(offeredCapabilities);
        input.setDesiredCapabilities(desiredCapabilities);

        ProtonBuffer buffer = legacyCodec.encodeUsingLegacyEncoder(input);
        assertNotNull(buffer);

        final Open result = (Open) decoder.readObject(buffer, decoderState);
        assertNotNull(result);

        assertTypesEqual(input, result);
    }
}
