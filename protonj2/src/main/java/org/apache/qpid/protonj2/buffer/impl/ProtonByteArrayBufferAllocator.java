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
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;

/**
 * A Proton built in byte array based buffer allocator
 */
public final class ProtonByteArrayBufferAllocator implements ProtonBufferAllocator {

    /**
     * A default instance of the {@link ProtonByteArrayBufferAllocator} that uses default configurations.
     */
    private static final ProtonBufferAllocator DEFAULT = ProtonBufferUtils.unclosable(new ProtonByteArrayBufferAllocator());

    private boolean closed;

    @Override
    public void close() {
        closed = true;
    }

    public static final ProtonBufferAllocator allocator() {
        return DEFAULT;
    }

    @Override
    public ProtonBuffer outputBuffer(int initialCapacity) {
        checkClosed();
        return new ProtonByteArrayBuffer(initialCapacity);
    }

    @Override
    public ProtonBuffer allocate() {
        checkClosed();
        return new ProtonByteArrayBuffer();
    }

    @Override
    public ProtonBuffer allocate(int initialCapacity) {
        checkClosed();
        return new ProtonByteArrayBuffer(initialCapacity);
    }


    @Override
    public ProtonBuffer allocateHeapBuffer() {
        checkClosed();
        return new ProtonByteArrayBuffer();
    }

    @Override
    public ProtonBuffer allocateHeapBuffer(int initialCapacity) {
        checkClosed();
        return new ProtonByteArrayBuffer(initialCapacity);
    }

    @SuppressWarnings("resource")
    @Override
    public ProtonBuffer copy(byte[] array, int offset, int length) {
        checkClosed();
        final byte[] copy = new byte[length];
        System.arraycopy(array, offset, copy, 0, length);
        return new ProtonByteArrayBuffer(copy, 0, length).setWriteOffset(length);
    }

    /**
     * Shallow copy of the given array segment used when the caller knows that
     * they will not share the bytes wrapped with any other application code.
     *
     * @param array The array that should be wrapped
     * @param offset The offset into the array where the wrapper starts
     * @param length The number of bytes that will be represented in the span.
     *
     * @return A {@link ProtonBuffer} that wraps the given array bytes.
     */
    @SuppressWarnings("resource")
    public ProtonBuffer wrap(byte[] array, int offset, int length) {
        checkClosed();
        return new ProtonByteArrayBuffer(array, offset, length, ProtonByteArrayBuffer.DEFAULT_MAXIMUM_CAPACITY).setWriteOffset(length);
    }

    @Override
    public ProtonCompositeBuffer composite() {
        checkClosed();
        return ProtonCompositeBuffer.create(this);
    }

    @Override
    public ProtonCompositeBuffer composite(ProtonBuffer buffer) {
        return ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator(), buffer);
    }

    @Override
    public ProtonCompositeBuffer composite(ProtonBuffer[] buffers) {
        return ProtonCompositeBuffer.create(ProtonBufferAllocator.defaultAllocator(), buffers);
    }

    /**
     * Shallow copy of the given array segment used when the caller knows that
     * they will not share the bytes wrapped with any other application code.
     *
     * @param array The array that should be wrapped
     *
     * @return A {@link ProtonBuffer} that wraps the given array bytes.
     */
    @SuppressWarnings("resource")
    public static ProtonBuffer wrapped(byte[] array) {
        return new ProtonByteArrayBuffer(array, 0, array.length, ProtonByteArrayBuffer.DEFAULT_MAXIMUM_CAPACITY).setWriteOffset(array.length);
    }

    /**
     * Shallow copy of the given array segment used when the caller knows that
     * they will not share the bytes wrapped with any other application code.
     *
     * @param array The array that should be wrapped
     * @param offset The offset into the array where the wrapper starts
     * @param length The number of bytes that will be represented in the span.
     *
     * @return A {@link ProtonBuffer} that wraps the given array bytes.
     */
    @SuppressWarnings("resource")
    public static ProtonBuffer wrapped(byte[] array, int offset, int length) {
        return new ProtonByteArrayBuffer(array, offset, length, ProtonByteArrayBuffer.DEFAULT_MAXIMUM_CAPACITY).setWriteOffset(length);
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("This allocator instance is closed");
        }
    }
}
