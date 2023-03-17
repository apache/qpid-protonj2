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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.junit.jupiter.api.Test;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;

/**
 * Test the Netty 5 to {@link ProtonBuffer} adapter
 */
public class Netty5ProtonBufferAdapterTest extends NettyBufferAdapterTestBase {

    @Override
    public ProtonBufferAllocator createTestCaseAllocator() {
        return new Netty5ProtonBufferAllocator(BufferAllocator.onHeapUnpooled());
    }

    @Test
    public void testBufferExposesNativeAddressValues() {
        try (ProtonBufferAllocator netty = new Netty5ProtonBufferAllocator(BufferAllocator.offHeapUnpooled());
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            nettyBuffer.writeLong(Long.MAX_VALUE);
            nettyBuffer.readByte();

            try (ProtonBufferComponentAccessor accessor = nettyBuffer.componentAccessor()) {
                for (ProtonBufferComponent component : accessor.components()) {
                    assertTrue(component.getNativeAddress() != 0);
                    assertTrue(component.getNativeReadAddress() != 0);
                    assertTrue(component.getNativeWriteAddress() != 0);
                }
            }
        }
    }

    @Test
    public void testBufferCloseReleasesBuffer() {
        try (ProtonBufferAllocator netty = createTestCaseAllocator();
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            Buffer buffer = (Buffer) nettyBuffer.unwrap();

            assertTrue(buffer.isAccessible());
            nettyBuffer.close();
            assertFalse(buffer.isAccessible());
        }
    }
}
