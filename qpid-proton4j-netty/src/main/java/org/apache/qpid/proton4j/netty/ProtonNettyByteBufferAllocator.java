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
package org.apache.qpid.proton4j.netty;

import java.nio.ByteBuffer;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A default {@link ProtonBufferAllocator} that creates wrapped Netty {@link ByteBuf} buffers.
 */
public class ProtonNettyByteBufferAllocator implements ProtonBufferAllocator {

    // TODO - Create only ByteBuf backed buffers for IO work, others would be better as proton types.

    public static final ProtonNettyByteBufferAllocator DEFAULT = new ProtonNettyByteBufferAllocator();

    @Override
    public ProtonBuffer outputBuffer(int initialCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.buffer(initialCapacity));
    }

    @Override
    public ProtonBuffer outputBuffer(int initialCapacity, int maximumCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.buffer(initialCapacity, maximumCapacity));
    }

    @Override
    public ProtonBuffer allocate() {
        return new ProtonNettyByteBuffer(Unpooled.buffer());
    }

    @Override
    public ProtonBuffer allocate(int initialCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.buffer(initialCapacity));
    }

    @Override
    public ProtonBuffer allocate(int initialCapacity, int maximumCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.buffer(initialCapacity, maximumCapacity));
    }

    @Override
    public ProtonBuffer wrap(byte[] array) {
        return new ProtonNettyByteBuffer(Unpooled.wrappedBuffer(array));
    }

    @Override
    public ProtonBuffer wrap(byte[] array, int offset, int length) {
        return new ProtonNettyByteBuffer(Unpooled.wrappedBuffer(array, offset, length));
    }

    @Override
    public ProtonBuffer wrap(ByteBuffer buffer) {
        return new ProtonNettyByteBuffer(Unpooled.wrappedBuffer(buffer));
    }
}
