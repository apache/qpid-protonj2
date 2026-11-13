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

/**
 * A buffer component represents a single component of the memory backing
 * a {@link ProtonBuffer} which could be a buffer that inherently made up of more
 * than one section of memory or it could be a intentional composite buffer
 * that collect individual buffers each with their own component backing.
 */
public interface ProtonBufferComponent {

    /**
     * Unwraps the backing resource of this buffer component which can be
     * an external buffer type or other managed resource.
     *
     * @return the resource backing this component or null if none exists.
     */
    Object unwrap();

    /**
     * Returns the number of readable bytes in the buffer regardless of the
     * actual memory backing this component.
     *
     * @return the number of readable bytes in this buffer component
     */
    int getReadableBytes();

    /**
     * @return true if the component is backed by a byte array that has a readable portion.
     */
    boolean hasReadbleArray();

    /**
     * Advances the internally maintained read offset for this component by the
     * given amount. If the amount to advance is greater than the available readable
     * bytes an exception is thrown.
     *
     * @param amount
     * 		The amount to advance the read offset of this component by.
     *
     * @return this {@link ProtonBufferComponent} instance.
     *
     * @throws ProtonBufferClosedException if the buffer is closed
     * @throws IndexOutOfBoundsException if the request results in an advance that violates the component boundaries.
     * @throws IllegalArgumentException if the given amount is negative
     */
    ProtonBufferComponent advanceReadOffset(int amount);

    /**
     * Returns the readable array if one exists or throws an exception.
     *
     * @return the readable array that backs this buffer component.
     *
     * @throws ProtonBufferClosedException if the buffer is closed
     * @throws UnsupportedOperationException if the component is not backed by an array or has no readable bytes.
     */
    byte[] getReadableArray();

    /**
     * @return the offset into the readable array where the readable bytes begin, or zero when
     *         {@link #hasReadbleArray()} is false.
     */
    int getReadableArrayOffset();

    /**
     * @return the length of the readable array from the offset that is owned by this component, or
     *         zero when {@link #hasReadbleArray()} is false.
     */
    int getReadableArrayLength();

    /**
     * Returns a {@link ByteBuffer} that covers the readable bytes of this component.
     *
     * @return the readable buffer that backs this buffer component.
     *
     * @throws ProtonBufferClosedException if the buffer is closed
     */
    ByteBuffer getReadableBuffer();

    /**
     * Returns the number of writable bytes in the buffer regardless of the
     * actual memory backing this component.
     *
     * @return the number of writable bytes in this buffer component
     */
    int getWritableBytes();

    /**
     * Advances the internally maintained write offset for this component by the
     * given amount. If the amount to advance is greater than the available writable
     * bytes an exception is thrown.
     *
     * @param amount
     * 		The amount to advance the write offset of this component by.
     *
     * @return this {@link ProtonBufferComponent} instance.
     *
     * @throws ProtonBufferReadOnlyException if the buffer is a read-only buffer
     * @throws ProtonBufferClosedException if the buffer is closed
     * @throws IndexOutOfBoundsException if the request results in an advance that violates the component boundaries
     * @throws IllegalArgumentException if the given amount is negative
     */
    ProtonBufferComponent advanceWriteOffset(int amount);

    /**
     * @return true if the component is backed by a byte array that has writable bytes.
     */
    boolean hasWritableArray();

    /**
     * Returns the writable array if one exists or throws an exception.
     *
     * @return the writable array that backs this buffer component.
     *
     * @throws ProtonBufferClosedException if the buffer is closed
     * @throws UnsupportedOperationException if the component is not backed by an array or has no writable bytes.
     */
    byte[] getWritableArray();

    /**
     * @return the offset into the writable array where the writable bytes begin, or zero when
     *         {@link #hasWritableArray()} is false.
     */
    int getWritableArrayOffset();

    /**
     * @return the length of the writable array from the offset that is owned by this component, or
     *         zero when {@link #hasWritableArray()} is false.
     */
    int getWritableArrayLength();

    /**
     * Returns a {@link ByteBuffer} that covers the writable bytes of this component.
     *
     * @return the writable buffer that backs this buffer component.
     *
     * @throws ProtonBufferClosedException if the buffer is closed
     */
    ByteBuffer getWritableBuffer();

    /**
     * Creates and returns a new {@link ProtonBufferIterator} that iterates from the current
     * read offset and continues until all readable bytes have been traversed. The source buffer
     * read and write offsets are not modified by an iterator instance.
     * <p>
     * The caller must ensure that the source buffer lifetime extends beyond the lifetime of
     * the returned {@link ProtonBufferIterator}.
     *
     * @return a new buffer iterator that iterates over the readable bytes.
     */
    public ProtonBufferIterator bufferIterator();

    /**
     * Gets the native address of the backing buffer if one exists otherwise returns 0.
     * <p>
     * The returned address is the base address for the memory region that back this buffer which
     * should not be used for reads and write as the backing region could be larger and not all portions
     * of the backing region might be owned by this buffer component. The caller should use the methods
     * {@link #getNativeReadAddress()} and {@link #getNativeWriteAddress()} which each use this method
     * to compute where these value actually fall in the native memory region.
     *
     * @return The native memory address for the region backing this buffer, if any, otherwise 0.
     */
    long getNativeAddress();

    /**
     * Gets the native address where reads from this buffer component should start.
     *
     * @return The native memory address where reads start, if any, otherwise 0.
     */
    long getNativeReadAddress();

    /**
     * Gets the native address where write to this buffer component should start.
     *
     * @return The native memory address where writing should start, if any, otherwise 0.
     */
    long getNativeWriteAddress();

}
