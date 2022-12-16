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

    @Override
    public void testEnsureWritableCanGrowBeyondImplicitCapacityLimit() {
        // Netty 4 buffer allocates more than needed in ensureWritable
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
