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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Ignore;
import org.junit.Test;

public class DispositionTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition input = new Disposition();

        input.setFirst(1);
        input.setRole(Role.RECEIVER);
        input.setBatchable(false);
        input.setSettled(true);
        input.setState(Accepted.getInstance());

        encoder.writeObject(buffer, encoderState, input);

        final Disposition result = (Disposition) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getFirst());
        assertEquals(Role.RECEIVER, result.getRole());
        assertEquals(false, result.getBatchable());
        assertEquals(true, result.getSettled());
        assertSame(Accepted.getInstance(), result.getState());
    }

    @Ignore("Need to decide how and when to validate mandatory fields")
    @Test
    public void testDecodeEnforcesFirstValueRequired() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Disposition input = new Disposition();

        input.setRole(Role.RECEIVER);
        input.setSettled(true);
        input.setState(Accepted.getInstance());

        // TODO - Probably should throw here too
        encoder.writeObject(buffer, encoderState, input);

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not encode when no First value is set");
        } catch (Exception ex) {
        }
    }
}
