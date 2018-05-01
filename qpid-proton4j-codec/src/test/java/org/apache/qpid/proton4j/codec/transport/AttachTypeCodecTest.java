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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class AttachTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeType() throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Symbol[] offeredCapabilities = new Symbol[] {Symbol.valueOf("Cap-1"), Symbol.valueOf("Cap-2")};
       Symbol[] desiredCapabilities = new Symbol[] {Symbol.valueOf("Cap-3"), Symbol.valueOf("Cap-4")};

       Attach input = new Attach();

       input.setName("name");
       input.setOfferedCapabilities(offeredCapabilities);
       input.setDesiredCapabilities(desiredCapabilities);
       input.setHandle(UnsignedInteger.valueOf(64));
       input.setRole(Role.RECEIVER);
       input.setSndSettleMode(SenderSettleMode.UNSETTLED);
       input.setRcvSettleMode(ReceiverSettleMode.SECOND);
       input.setSource(new Source());
       input.setTarget(new Target());
       input.setIncompleteUnsettled(false);
       input.setInitialDeliveryCount(UnsignedInteger.valueOf(10));
       input.setMaxMessageSize(UnsignedLong.valueOf(1024));

       encoder.writeObject(buffer, encoderState, input);

       final Attach result = (Attach) decoder.readObject(buffer, decoderState);

       assertEquals("name", result.getName());
       assertEquals(UnsignedInteger.valueOf(64), result.getHandle());
       assertEquals(Role.RECEIVER, result.getRole());
       assertEquals(SenderSettleMode.UNSETTLED, result.getSndSettleMode());
       assertEquals(ReceiverSettleMode.SECOND, result.getRcvSettleMode());
       assertEquals(UnsignedInteger.valueOf(10), result.getInitialDeliveryCount());
       assertEquals(UnsignedLong.valueOf(1024), result.getMaxMessageSize());
       assertNotNull(result.getSource());
       assertNotNull(result.getTarget());
       assertFalse(result.getIncompleteUnsettled());
       assertNull(result.getUnsettled());
       assertNull(result.getProperties());
       assertArrayEquals(offeredCapabilities, result.getOfferedCapabilities());
       assertArrayEquals(desiredCapabilities, result.getDesiredCapabilities());
    }
}
