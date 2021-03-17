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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.types.transport.Transfer;
import org.junit.jupiter.api.Test;

class ProtocolFramePoolTest {

    @Test
    void testCreateIncomingFramePool() {
        AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.incomingEnvelopePool();

        assertNotNull(pool);
        assertEquals(AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE, pool.getMaxPoolSize());

        IncomingAMQPEnvelope frame1 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame1);
        IncomingAMQPEnvelope frame2 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame2);

        assertNotSame(frame1, frame2);
    }

    @Test
    void testCreateIncomingFramePoolWithConfiguredMaxSize() {
        AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.incomingEnvelopePool(AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE + 10);

        assertEquals(AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE + 10, pool.getMaxPoolSize());

        IncomingAMQPEnvelope frame1 = pool.take(new Transfer(), 0, null);

        frame1.release();

        IncomingAMQPEnvelope frame2 = pool.take(new Transfer(), 0, null);

        assertSame(frame1, frame2);
    }

    @Test
    void testIncomingPoolRecyclesReleasedFrames() {
        AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.incomingEnvelopePool();
        IncomingAMQPEnvelope frame1 = pool.take(new Transfer(), 0, null);

        frame1.release();

        IncomingAMQPEnvelope frame2 = pool.take(new Transfer(), 0, null);

        assertSame(frame1, frame2);
    }

    @Test
    void testInomingPoolClearsReleasedFramePayloads() {
        AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.incomingEnvelopePool();
        IncomingAMQPEnvelope frame1 = pool.take(new Transfer(), 2, ProtonByteBufferAllocator.DEFAULT.allocate());

        frame1.release();

        assertNull(frame1.getBody());
        assertNull(frame1.getPayload());
        assertNotEquals(2, frame1.getChannel());
    }

    @Test
    void testCreateOutgoingFramePool() {
        AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.outgoingEnvelopePool();

        assertNotNull(pool);
        assertEquals(AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE, pool.getMaxPoolSize());

        OutgoingAMQPEnvelope frame1 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame1);
        OutgoingAMQPEnvelope frame2 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame2);

        assertNotEquals(frame1, frame2);
    }

    @Test
    void testCreateOutgoingFramePoolWithConfiguredMaxSize() {
        AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.outgoingEnvelopePool(AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE + 10);

        assertEquals(AMQPPerformativeEnvelopePool.DEFAULT_MAX_POOL_SIZE + 10, pool.getMaxPoolSize());

        OutgoingAMQPEnvelope frame1 = pool.take(new Transfer(), 0, null);

        frame1.release();

        OutgoingAMQPEnvelope frame2 = pool.take(new Transfer(), 0, null);

        assertSame(frame1, frame2);
    }

    @Test
    void testOutgoingPoolRecyclesReleasedFrames() {
        AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.outgoingEnvelopePool();
        OutgoingAMQPEnvelope frame1 = pool.take(new Transfer(), 0, null);

        frame1.release();

        OutgoingAMQPEnvelope frame2 = pool.take(new Transfer(), 0, null);

        assertSame(frame1, frame2);
    }

    @Test
    void testOutgoingPoolClearsReleasedFramePayloads() {
        AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> pool = AMQPPerformativeEnvelopePool.outgoingEnvelopePool();
        OutgoingAMQPEnvelope frame1 = pool.take(new Transfer(), 2, ProtonByteBufferAllocator.DEFAULT.allocate());

        frame1.release();

        assertNull(frame1.getBody());
        assertNull(frame1.getPayload());
        assertNotEquals(2, frame1.getChannel());
    }
}
