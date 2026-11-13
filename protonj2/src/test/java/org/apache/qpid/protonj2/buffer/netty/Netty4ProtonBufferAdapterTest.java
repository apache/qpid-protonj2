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

package org.apache.qpid.protonj2.buffer.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

/**
 * Test the Netty 4 to {@link ProtonBuffer} adapter
 */
class Netty4ProtonBufferAdapterTest extends NettyBufferAdapterTestBase {

    @Override
    public ProtonBufferAllocator createTestCaseAllocator() {
        return new Netty4ProtonBufferAllocator(UnpooledByteBufAllocator.DEFAULT);
    }

    /**
     * The test here overrides the base test because netty allocates its buffers using
     * an algorithm that we cannot fully control without significant effort which is not
     * generally worth the small behavioral difference. This test capture the behavior as
     * it stands now and will fail in the future if we choose to attempt to address this
     * difference.
     */
    @Test
    @Override
    public void testEnsureWritableCanGrowBeyondImplicitCapacityLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(8)) {

            buf.writeLong(0x0102030405060708L);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));

            buf.ensureWritable(8);

            // The resize rounds up well past the request, and the extra room is usable, but the
            // limit itself is retained and imposed again once the extra room is used up.
            assertTrue(buf.capacity() > 16, "netty resized the buffer to capacity " + buf.capacity());
            assertEquals(8, buf.implicitGrowthLimit());

            buf.writeLong(0x0807060504030201L);

            while (buf.getWritableBytes() > 0) {
                buf.writeByte((byte) 2);
            }

            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));
            assertEquals(8, buf.implicitGrowthLimit());
        }
    }

    @Test
    public void testBufferCloseReleasesRefCount() {
        try (ProtonBufferAllocator netty = createTestCaseAllocator();
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            ByteBuf buffer = (ByteBuf) nettyBuffer.unwrap();

            assertEquals(1, ReferenceCountUtil.refCnt(buffer));
            nettyBuffer.close();
            assertEquals(0, ReferenceCountUtil.refCnt(buffer));
        }
    }
}
