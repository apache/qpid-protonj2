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

package org.apache.qpid.protonj2.buffer.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.qpid.protonj2.buffer.ProtonAbstractBufferTest;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.junit.jupiter.api.Test;

/**
 * Test the byte array backed proton buffer
 */
public class ProtonByteArrayBufferTest extends ProtonAbstractBufferTest {

    @Override
    public ProtonBufferAllocator createTestCaseAllocator() {
        return new ProtonByteArrayBufferAllocator();
    }

    @Test
    public void testBufferExposesNativeAddressValues() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(16)) {

            buffer.writeLong(Long.MAX_VALUE);
            buffer.readByte();

            try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
                for (ProtonBufferComponent component : accessor.components()) {
                    assertEquals(0, component.getNativeAddress());
                    assertEquals(0, component.getNativeReadAddress());
                    assertEquals(0, component.getNativeWriteAddress());
                }
            }
        }
    }
}
