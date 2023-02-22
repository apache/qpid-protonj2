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

import org.apache.qpid.protonj2.buffer.impl.ProtonCompositeBufferImpl;

/**
 * Defines the API for a specialized buffer type that is a composite of
 * other buffer instances which is presented as a single buffer view.
 */
public interface ProtonCompositeBuffer extends ProtonBuffer {

    /**
     * Create an empty composite buffer that will use the provided allocator to
     * create new buffer capacity if writes are performed and insufficient space
     * remains (unless the capacity limit is reached).
     *
     * @param allocator
     * 		The allocator to use when adding new buffer capacity automatically.
     *
     * @return a new empty composite buffer instance.
     */
    static ProtonCompositeBuffer create(ProtonBufferAllocator allocator) {
        return ProtonCompositeBufferImpl.create(allocator);
    }

    /**
     * Create an composite buffer with the given buffer instance as the initial buffer
     * payload. The provided buffer allocator will use the provided allocator to create
     * new buffer capacity if writes are performed and insufficient space remains (unless
     * the capacity limit is reached).
     *
     * @param allocator
     * 		The allocator to use when adding new buffer capacity automatically.
     * @param buffer
     * 		The initial buffer to append as the body of the returned composite buffer.
     *
     * @return a new composite buffer instance that wraps the given buffer.
     */
    static ProtonCompositeBuffer create(ProtonBufferAllocator allocator, ProtonBuffer buffer) {
        return ProtonCompositeBufferImpl.create(allocator, buffer);
    }

    /**
     * Create an composite buffer with the given array of buffers as the initial buffer
     * payload. The provided buffer allocator will use the provided allocator to create
     * new buffer capacity if writes are performed and insufficient space remains (unless
     * the capacity limit is reached).
     *
     * @param allocator
     * 		The allocator to use when adding new buffer capacity automatically.
     * @param buffers
     * 		The initial buffers to append as the body of the returned composite buffer.
     *
     * @return a new composite buffer instance that wraps the given buffers.
     */
    static ProtonCompositeBuffer create(ProtonBufferAllocator allocator, ProtonBuffer[] buffers) {
        return ProtonCompositeBufferImpl.create(allocator, buffers);
    }

    /**
     * Checks if the given buffer is an composite buffer instance or not.
     *
     * @param buffer
     * 		the buffer instance to check.
     *
     * @return true if the buffer given is a composite buffer or false if not.
     */
    static boolean isComposite(ProtonBuffer buffer) {
        return buffer instanceof ProtonCompositeBuffer;
    }

    /**
     * Appends the given buffer to this composite buffer if all the constraints on
     * buffer composites are met, otherwise thrown an exception.
     *
     * @param buffer
     * 		The buffer to append to this composite collection of buffers.
     *
     * @return this composite buffer instance.
     */
    ProtonCompositeBuffer append(ProtonBuffer buffer);

    /**
     * Splits the composite buffer up into a collection of buffers that comprise it and
     * leaves this buffer in what is effectively a closed state.  The returned buffers are
     * now the property of the caller who must ensure they are closed when no longer in use..
     *
     * @return the collection of buffers that comprised this composite buffer.
     */
    Iterable<ProtonBuffer> decomposeBuffer();

    /**
     * Split this buffer returning a composite that consists of all the buffer components that come before
     * the buffer that contains the split offset, no buffer components are split. This allows for operations
     * such as reclaiming read bytes from a buffer to reduce overall memory use.
     *
     * @param splitOffset The index into the buffer where the split should be performed.
     *
     * @return A {@link ProtonCompositeBuffer} that owns all buffers that come before the buffer that holds given index.
     */
    ProtonCompositeBuffer splitComponentsFloor(int splitOffset);

    /**
     * Split this buffer returning a composite that consists of all the buffer components up to and including
     * the buffer that contains the split offset, no buffer components are split.
     *
     * @param splitOffset The index into the buffer where the split should be performed.
     *
     * @return A {@link ProtonCompositeBuffer} that owns all buffers up to and including the buffer that holds given index.
     */
    ProtonCompositeBuffer splitComponentsCeil(int splitOffset);

    @Override
    ProtonCompositeBuffer fill(byte value);

    @Override
    ProtonCompositeBuffer setReadOffset(int value);

    @Override
    default ProtonCompositeBuffer advanceReadOffset(int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.advanceReadOffset(length);
    }

    @Override
    ProtonCompositeBuffer setWriteOffset(int value);

    @Override
    default ProtonCompositeBuffer advanceWriteOffset(int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.advanceWriteOffset(length);
    }

    @Override
    ProtonCompositeBuffer implicitGrowthLimit(int limit);

    @Override
    default ProtonCompositeBuffer ensureWritable(int amount) throws IndexOutOfBoundsException, IllegalArgumentException {
        return (ProtonCompositeBuffer) ProtonBuffer.super.ensureWritable(amount);
    }

    @Override
    ProtonCompositeBuffer ensureWritable(int size, int minimumGrowth, boolean allowCompaction) throws IndexOutOfBoundsException, IllegalArgumentException;

    @Override
    ProtonCompositeBuffer convertToReadOnly();

    @Override
    default ProtonCompositeBuffer copy() {
        return (ProtonCompositeBuffer) ProtonBuffer.super.copy();
    }

    @Override
    default ProtonCompositeBuffer copy(int index, int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.copy(index, length);
    }

    @Override
    default ProtonCompositeBuffer copy(boolean readOnly) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.copy(readOnly);
    }

    @Override
    ProtonCompositeBuffer copy(int index, int length, boolean readOnly) throws IllegalArgumentException;

    @Override
    ProtonCompositeBuffer compact();

    @Override
    default ProtonCompositeBuffer readSplit(int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.readSplit(length);
    }

    @Override
    default ProtonCompositeBuffer writeSplit(int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.writeSplit(length);
    }

    @Override
    default ProtonCompositeBuffer split() {
        return (ProtonCompositeBuffer) ProtonBuffer.super.split();
    }

    @Override
    ProtonCompositeBuffer split(int splitOffset);

    @Override
    default ProtonCompositeBuffer writeBytes(ProtonBuffer source) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.writeBytes(source);
    }

    @Override
    default ProtonCompositeBuffer writeBytes(byte[] source) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.writeBytes(source);
    }

    @Override
    default ProtonCompositeBuffer writeBytes(byte[] source, int offset, int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.writeBytes(source, offset, length);
    }

    @Override
    default ProtonCompositeBuffer writeBytes(ByteBuffer source) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.writeBytes(source);
    }

    @Override
    default ProtonCompositeBuffer readBytes(ByteBuffer destination) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.readBytes(destination);
    }

    @Override
    default ProtonCompositeBuffer readBytes(byte[] destination, int offset, int length) {
        return (ProtonCompositeBuffer) ProtonBuffer.super.readBytes(destination, offset, length);
    }
}
