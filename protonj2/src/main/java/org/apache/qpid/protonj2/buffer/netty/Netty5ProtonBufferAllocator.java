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

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;

/**
 * Proton managed Netty 5 {@link BufferAllocator} wrapper.
 */
public final class Netty5ProtonBufferAllocator implements ProtonBufferAllocator {

    public static int DEFAULT_CAPACITY = 1024;

    public static ProtonBufferAllocator POOLED = new Netty5ProtonBufferAllocator(BufferAllocator.onHeapPooled());

    public static ProtonBufferAllocator UNPOOLED = new Netty5ProtonBufferAllocator(BufferAllocator.onHeapUnpooled());

    private boolean closed;

    private final BufferAllocator allocator;

    public Netty5ProtonBufferAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    public BufferAllocator allocator() {
        return allocator;
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Creates a {@link ProtonBuffer} wrapper around the given Netty 5 based
     * Buffer instance. The method wraps a Buffer and assumes ownership of it
     * which means that once the wrapper buffer is closed it will release the
     * {@link Buffer} which if there are no other references will render the
     * wrapped buffer closed and recyclable if pooled.  Care should be take
     * when supplying the buffer to add a reference depending on the source of
     * the buffer.
     *
     * @param buffer
     * 		The buffer instance to wrap.
     *
     * @return A ProtonBuffer instance that wraps the given netty buffer.
     */
    public Netty5ToProtonBufferAdapter wrap(Buffer buffer) {
        checkClosed();
        return new Netty5ToProtonBufferAdapter(this, buffer);
    }

    @Override
    public Netty5ToProtonBufferAdapter outputBuffer(int initialCapacity) {
        return allocate(initialCapacity);
    }

    @Override
    public Netty5ToProtonBufferAdapter allocate() {
        return allocate(DEFAULT_CAPACITY);
    }

    @Override
    public Netty5ToProtonBufferAdapter allocate(int initialCapacity) {
        checkClosed();
        return new Netty5ToProtonBufferAdapter(this, allocator.allocate(initialCapacity));
    }

    @Override
    public Netty5ToProtonBufferAdapter allocateHeapBuffer() {
        return allocateHeapBuffer(DEFAULT_CAPACITY);
    }

    @Override
    public Netty5ToProtonBufferAdapter allocateHeapBuffer(int initialCapacity) {
        checkClosed();
        return new Netty5ToProtonBufferAdapter(this, BufferAllocator.onHeapUnpooled().allocate(initialCapacity));
    }

    @Override
    public Netty5ToProtonBufferAdapter copy(byte[] array) {
        checkClosed();
        return new Netty5ToProtonBufferAdapter(this, allocator.copyOf(array));
    }

    @Override
    public Netty5ToProtonBufferAdapter copy(byte[] array, int offset, int length) {
        checkClosed();
        return new Netty5ToProtonBufferAdapter(this, allocator.copyOf(ByteBuffer.wrap(array, offset, length)));
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

