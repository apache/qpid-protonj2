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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * Proton managed Netty 4 {@link ByteBufAllocator} wrapper.
 */
public final class Netty4ProtonBufferAllocator implements ProtonBufferAllocator {

    public static ProtonBufferAllocator POOLED = new Netty4ProtonBufferAllocator(PooledByteBufAllocator.DEFAULT);

    public static ProtonBufferAllocator UNPOOLED = new Netty4ProtonBufferAllocator(PooledByteBufAllocator.DEFAULT);

    private final ByteBufAllocator allocator;

    private boolean closed;

    public Netty4ProtonBufferAllocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void close() {
        closed = true;
    }

    public ByteBufAllocator allocator() {
        return allocator;
    }

    /**
     * Creates a {@link ProtonBuffer} wrapper around the given Netty 4 based
     * ByteBuf instance. The method wraps a ByteBuf and assumes ownership of it
     * which means that once the wrapper buffer is closed it will release the
     * {@link ByteBuf} which if there are no other references will render the
     * wrapped buffer closed and recyclable if pooled.  Care should be take
     * when supplying the buffer to add a reference depending on the source of
     * the buffer.
     *
     * @param buffer
     * 		The buffer instance to wrap.
     *
     * @return A ProtonBuffer instance that wraps the given netty buffer.
     */
    public Netty4ToProtonBufferAdapter wrap(ByteBuf buffer) {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, buffer);
    }

    @Override
    public Netty4ToProtonBufferAdapter outputBuffer(int initialCapacity) {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, allocator.ioBuffer(initialCapacity));
    }

    @Override
    public Netty4ToProtonBufferAdapter allocate() {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, allocator.heapBuffer());
    }

    @Override
    public Netty4ToProtonBufferAdapter allocate(int initialCapacity) {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, allocator.heapBuffer(initialCapacity));
    }

    @Override
    public Netty4ToProtonBufferAdapter allocateHeapBuffer() {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, Unpooled.buffer());
    }

    @Override
    public Netty4ToProtonBufferAdapter allocateHeapBuffer(int initialCapacity) {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, Unpooled.buffer(initialCapacity));
    }

    @Override
    public Netty4ToProtonBufferAdapter copy(byte[] array, int offset, int length) {
        checkClosed();
        return new Netty4ToProtonBufferAdapter(this, Unpooled.copiedBuffer(array, offset, length));
    }

    @Override
    public ProtonCompositeBuffer composite() {
        checkClosed();
        return ProtonCompositeBuffer.create(this);
    }

    @Override
    public ProtonCompositeBuffer composite(ProtonBuffer buffer) {
        checkClosed();
        return ProtonCompositeBuffer.create(this, buffer);
    }

    @Override
    public ProtonCompositeBuffer composite(ProtonBuffer[] buffers) {
        checkClosed();
        return ProtonCompositeBuffer.create(this, buffers);
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("This allocator instance is closed");
        }
    }
}

