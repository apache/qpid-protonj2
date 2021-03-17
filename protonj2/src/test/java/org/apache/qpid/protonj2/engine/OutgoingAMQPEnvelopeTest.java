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
package org.apache.qpid.protonj2.engine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.transport.Open;
import org.apache.qpid.protonj2.types.transport.Performative.PerformativeHandler;
import org.junit.jupiter.api.Test;

class OutgoingAMQPEnvelopeTest {

    @Test
    void testCreateFromDefaultNoPool() {
        OutgoingAMQPEnvelope envelope = new OutgoingAMQPEnvelope();

        assertThrows(IllegalArgumentException.class, () -> envelope.handlePayloadToLarge());

        envelope.setPayloadToLargeHandler(null);

        assertThrows(IllegalArgumentException.class, () -> envelope.handlePayloadToLarge());

        envelope.setPayloadToLargeHandler((perf) -> {});

        assertDoesNotThrow(() -> envelope.handlePayloadToLarge());

        envelope.handleOutgoingFrameWriteComplete();
        envelope.release();

        assertNotNull(envelope.toString());
    }

    @Test
    void testInvokeHandlerOnPerformative() {
        OutgoingAMQPEnvelope envelope = new OutgoingAMQPEnvelope();
        envelope.initialize(new Open(), 0, null);

        final AtomicBoolean signal = new AtomicBoolean();

        envelope.invoke(new PerformativeHandler<OutgoingAMQPEnvelope>() {

            @Override
            public void handleOpen(Open open, ProtonBuffer payload, int channel, OutgoingAMQPEnvelope context) {
                signal.set(true);
            }

        }, envelope);

        assertTrue(signal.get());
    }
}
