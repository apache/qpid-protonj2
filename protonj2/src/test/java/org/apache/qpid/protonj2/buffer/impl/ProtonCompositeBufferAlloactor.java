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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;

/**
 * Test based buffer allocator that always creates composite buffers
 */
public class ProtonCompositeBufferAlloactor implements ProtonBufferAllocator {

    private boolean closed;

    @Override
    public void close() {
        this.closed = true;
    }

    private void checkClosed() {
        if (closed) {
            throw new ProtonBufferClosedException("The allocator has been closed");
        }
    }

    @Override
    public ProtonCompositeBuffer outputBuffer(int initialCapacity) {
        return allocate(initialCapacity);
    }

    @Override
    public ProtonCompositeBuffer allocate() {
        checkClosed();
        return ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator());
    }

    @Override
    public ProtonCompositeBuffer allocate(int initialCapacity) {
        checkClosed();

        final ProtonBuffer initial = ProtonBufferAllocator.defaultAllocator().allocate(initialCapacity);
        final ProtonCompositeBuffer composite = ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator());

        return composite.append(initial);
    }

    @Override
    public ProtonBuffer allocateHeapBuffer() {
        return allocate();
    }

    @Override
    public ProtonBuffer allocateHeapBuffer(int initialCapacity) {
        return allocate(initialCapacity);
    }

    @Override
    public ProtonCompositeBuffer composite() {
        return allocate();
    }

    @Override
    public ProtonCompositeBuffer composite(ProtonBuffer buffer) {
        return ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator(), buffer);
    }

    @Override
    public ProtonCompositeBuffer composite(ProtonBuffer[] buffers) {
        return ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator(), buffers);
    }

    @Override
    public ProtonCompositeBuffer copy(byte[] array, int offset, int length) {
        checkClosed();

        final ProtonBuffer copy = ProtonBufferAllocator.defaultAllocator().copy(array, offset, length);
        final ProtonCompositeBuffer composite = ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator());

        return composite.append(copy).setWriteOffset(length);
    }
}
