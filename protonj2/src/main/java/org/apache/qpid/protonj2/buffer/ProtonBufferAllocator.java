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

import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBufferAllocator;

/**
 * Interface for a ProtonBuffer allocator object that can be used by Proton
 * objects to create memory buffers using the preferred type of the application
 * or library that embeds the Proton engine.
 */
public interface ProtonBufferAllocator extends AutoCloseable {

    @Override
    default public void close() {
        // Default has no action to take
    }

    /**
     * Gets an allocator from the proton internal buffer allocator which can be a
     * default version or may have been configured to ensure all allocations use
     * a specific allocator instance.
     *
     * @return a {@link ProtonBufferAllocator} for use in the application.
     */
    static ProtonBufferAllocator defaultAllocator() {
        return ProtonByteArrayBufferAllocator.allocator();
    }

    /**
     * Create a new output ProtonBuffer instance with the given initial capacity and the
     * implicit growth capacity should be that of the underlying buffer implementations limit.
     * The buffer implementation should support growing the buffer on an as needed basis to allow
     * writes without the user needing to code extra capacity and buffer reallocation checks.
     * <p>
     * The returned buffer will be used for frame output from the Proton engine and
     * can be a pooled buffer which the IO handler will then need to release once
     * the buffer has been written.
     *
     * @param initialCapacity
     *      The initial capacity to use when creating the new ProtonBuffer.
     *
     * @return a new ProtonBuffer instance with the given initial capacity.
     */
    ProtonBuffer outputBuffer(int initialCapacity);

    /**
     * Create a new ProtonBuffer instance with default initial capacity.  The buffer
     * implementation should support growing the buffer on an as needed basis to allow
     * writes without the user needing to code extra capacity and buffer reallocation
     * checks.
     * <p>
     * Proton buffers are closable resources and their life-span requires that they
     * be closed upon reaching their determined end of life.
     *
     * @return a new ProtonBuffer instance with default initial capacity.
     */
    ProtonBuffer allocate();

    /**
     * Create a new ProtonBuffer instance with the given initial capacity and the
     * implicit growth limit should be that of the underlying buffer implementations
     * maximum capacity limit.
     * <p>
     * Proton buffers are closable resources and their life-span requires that they
     * be closed upon reaching their determined end of life.
     *
     * @param initialCapacity
     *      The initial capacity to use when creating the new ProtonBuffer.
     *
     * @return a new ProtonBuffer instance with the given initial capacity.
     */
    ProtonBuffer allocate(int initialCapacity);

    /**
     * Create a new ProtonBuffer instance with default initial capacity.  The buffer
     * implementation should support growing the buffer on an as needed basis to allow
     * writes without the user needing to code extra capacity and buffer reallocation
     * checks. The buffer allocated must be a heap buffer for cases where the buffered
     * resource may not be easily closed and must revert to GC reclaim semantics.
     * <p>
     * Proton buffers are closable resources and their life-span requires that they
     * be closed upon reaching their determined end of life.
     *
     * @return a new ProtonBuffer instance with default initial capacity.
     */
    ProtonBuffer allocateHeapBuffer();

    /**
     * Create a new ProtonBuffer instance with the given initial capacity and the
     * implicit growth limit should be that of the underlying buffer implementations
     * maximum capacity limit. The buffer allocated must be a heap buffer for cases
     * where the buffered resource may not be easily closed and must revert to GC
     * reclaim semantics.
     * <p>
     * Proton buffers are closable resources and their life-span requires that they
     * be closed upon reaching their determined end of life.
     *
     * @param initialCapacity
     *      The initial capacity to use when creating the new ProtonBuffer.
     *
     * @return a new ProtonBuffer instance with the given initial capacity.
     */
    ProtonBuffer allocateHeapBuffer(int initialCapacity);

    /**
     * Creates a new composite buffer instance that uses this allocator to create new
     * backing space when the buffer writes exceed capacity or the ensure writable space
     * API is used. The created buffer will be empty and can be expanded with the normal
     * buffer API or extended with the addition of buffers.
     *
     * @return a new empty composite buffer instance.
     */
    ProtonCompositeBuffer composite();

    /**
     * Creates a new composite buffer instance that uses this allocator to create new
     * backing space when the buffer writes exceed capacity or the ensure writable space
     * API is used. The created buffer will be composed of the given sequence of buffers.
     *
     * @param buffer the buffers to compose
     *
     * @return a new composite buffer instance.
     */
    ProtonCompositeBuffer composite(ProtonBuffer buffer);

    /**
     * Creates a new composite buffer instance that uses this allocator to create new
     * backing space when the buffer writes exceed capacity or the ensure writable space
     * API is used. The created buffer will be composed of the given sequence of buffers.
     *
     * @param buffers the array of buffers to compose
     *
     * @return a new composite buffer instance.
     */
    ProtonCompositeBuffer composite(ProtonBuffer[] buffers);

    /**
     * Create a new ProtonBuffer that copies the given byte array.
     * <p>
     * The capacity for the resulting ProtonBuffer should equal to the length of the
     * copied array and the returned array read offset is zero  The returned buffer
     * can be expanded using the normal write or expand methods and its write offset
     * will be set to the buffer capacity.
     * <p>
     * Changes to the input buffer after calling this method will not affect the contents
     * of the returned buffer copy.
     *
     * @param array
     *      the byte array to copy.
     *
     * @return a new ProtonBuffer that is a copy of the given array.
     */
    default ProtonBuffer copy(byte[] array) {
        return copy(array, 0, array.length);
    }

    /**
     * Create a new ProtonBuffer that copies the given byte array using the provided
     * offset and length values to confine the view of that array.
     * <p>
     * The initial capacity of the buffer should be that of the length of the wrapped array.
     * The returned buffer can be expanded using the normal write or expand methods. The write
     * offset of the returned buffer will be set to the capacity.
     * <p>
     * Changes to the input buffer after calling this method will not affect the contents
     * of the returned buffer copy.
     *
     * @param array
     *      the byte array to copy.
     * @param offset
     *      the offset into the array where the view begins.
     * @param length
     *      the number of bytes in the array to expose
     *
     * @return a new {@link ProtonBuffer} that is a copy of the given array.
     */
    ProtonBuffer copy(byte[] array, int offset, int length);

}
