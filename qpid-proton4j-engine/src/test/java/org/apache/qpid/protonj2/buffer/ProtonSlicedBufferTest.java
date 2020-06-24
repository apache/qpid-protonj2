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
package org.apache.qpid.protonj2.buffer;

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBuffer;
import org.apache.qpid.protonj2.buffer.ProtonNettyByteBuffer;
import org.apache.qpid.protonj2.buffer.ProtonNioByteBuffer;

import io.netty.buffer.Unpooled;

/**
 * Tests that cover the usages of the sliced proton buffer class.
 */
public class ProtonSlicedBufferTest extends ProtonAbstractBufferTest {

    @Override
    protected boolean canBufferCapacityBeChanged() {
        return false;
    }

    @Override
    protected boolean canAllocateDirectBackedBuffers() {
        return true;
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity) {
        return new ProtonByteBuffer(initialCapacity).setWriteIndex(initialCapacity).slice().clear();
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity) {
        return new ProtonNioByteBuffer(ByteBuffer.allocateDirect(initialCapacity), 0).setWriteIndex(initialCapacity).slice().clear();
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonByteBuffer(initialCapacity, maxCapacity).setWriteIndex(initialCapacity).slice().clear();
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.directBuffer(initialCapacity, maxCapacity)).setWriteIndex(initialCapacity).slice().clear();
    }

    @Override
    protected ProtonBuffer wrapBuffer(byte[] array) {
        return new ProtonByteBuffer(array).slice();
    }
}
