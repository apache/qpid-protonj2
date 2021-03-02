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
        ProtocolFramePool<IncomingProtocolFrame> pool = ProtocolFramePool.incomingFramePool();

        assertNotNull(pool);
        assertEquals(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE, pool.getMaxPoolSize());

        IncomingProtocolFrame frame1 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame1);
        IncomingProtocolFrame frame2 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame2);

        assertNotSame(frame1, frame2);
    }

    @Test
    void testCreateIncomingFramePoolWithConfiguredMaxSize() {
        ProtocolFramePool<IncomingProtocolFrame> pool = ProtocolFramePool.incomingFramePool(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE + 10);

        assertEquals(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE + 10, pool.getMaxPoolSize());
    }

    @Test
    void testIncomingPoolRecyclesReleasedFrames() {
        ProtocolFramePool<IncomingProtocolFrame> pool = ProtocolFramePool.incomingFramePool();
        IncomingProtocolFrame frame1 = pool.take(new Transfer(), 0, null);

        frame1.release();

        IncomingProtocolFrame frame2 = pool.take(new Transfer(), 0, null);

        assertSame(frame1, frame2);
    }

    @Test
    void testInomingPoolClearsReleasedFramePayloads() {
        ProtocolFramePool<IncomingProtocolFrame> pool = ProtocolFramePool.incomingFramePool();
        IncomingProtocolFrame frame1 = pool.take(new Transfer(), 2, ProtonByteBufferAllocator.DEFAULT.allocate());

        frame1.release();

        assertNull(frame1.getBody());
        assertNull(frame1.getPayload());
        assertNotEquals(2, frame1.getChannel());
    }

    @Test
    void testCreateOutgoingFramePool() {
        ProtocolFramePool<OutgoingProtocolFrame> pool = ProtocolFramePool.outgoingFramePool();

        assertNotNull(pool);
        assertEquals(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE, pool.getMaxPoolSize());

        OutgoingProtocolFrame frame1 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame1);
        OutgoingProtocolFrame frame2 = pool.take(new Transfer(), 0, null);
        assertNotNull(frame2);

        assertNotEquals(frame1, frame2);
    }

    @Test
    void testCreateOutgoingFramePoolWithConfiguredMaxSize() {
        ProtocolFramePool<OutgoingProtocolFrame> pool = ProtocolFramePool.outgoingFramePool(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE + 10);

        assertEquals(ProtocolFramePool.DEFAULT_MAX_POOL_SIZE + 10, pool.getMaxPoolSize());
    }

    @Test
    void testOutgoingPoolRecyclesReleasedFrames() {
        ProtocolFramePool<OutgoingProtocolFrame> pool = ProtocolFramePool.outgoingFramePool();
        OutgoingProtocolFrame frame1 = pool.take(new Transfer(), 0, null);

        frame1.release();

        OutgoingProtocolFrame frame2 = pool.take(new Transfer(), 0, null);

        assertSame(frame1, frame2);
    }

    @Test
    void testOutgoingPoolClearsReleasedFramePayloads() {
        ProtocolFramePool<OutgoingProtocolFrame> pool = ProtocolFramePool.outgoingFramePool();
        OutgoingProtocolFrame frame1 = pool.take(new Transfer(), 2, ProtonByteBufferAllocator.DEFAULT.allocate());

        frame1.release();

        assertNull(frame1.getBody());
        assertNull(frame1.getPayload());
        assertNotEquals(2, frame1.getChannel());
    }
}
