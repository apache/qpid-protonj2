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
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class TransferTypeCodeTest extends CodecTestSupport {

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Binary tag = new Binary(new byte[] {0, 1, 2});

        Transfer input = new Transfer();

        input.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        input.setDeliveryId(10);
        input.setDeliveryTag(tag);
        input.setMessageFormat(0);
        input.setSettled(false);
        input.setBatchable(false);

        encoder.writeObject(buffer, encoderState, input);

        final Transfer result = (Transfer) decoder.readObject(buffer, decoderState);

        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), result.getHandle());
        assertEquals(10, result.getDeliveryId());
        assertEquals(tag, result.getDeliveryTag());
        assertEquals(0, result.getMessageFormat());
        assertFalse(result.getSettled());
        assertFalse(result.getBatchable());
    }
}
