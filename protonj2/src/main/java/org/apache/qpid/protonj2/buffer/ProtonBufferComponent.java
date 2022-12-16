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
     * Returns the number of readable bytes in the buffer safely regards of
     * the actual memory backing this component.
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
     * @throws UnsupportedOperationException if the component is not backed by a readable array.
     */
    ProtonBufferComponent advanceReadOffset(int amount);

    /**
     * Returns the readable array if one exists or throws an exception.
     *
     * @return the readable array that backs this buffer component.
     *
     * @throws UnsupportedOperationException if the component is not backed by an array or has no readable bytes.
     */
    byte[] getReadableArray();

    /**
     * @return the offset into the readable array where the readable bytes begin.
     *
     * @throws UnsupportedOperationException if the component is not backed by a readable array.
     */
    int getReadableArrayOffset();

    /**
     * @return the length of the readable array from the offset that is owned by this component..
     *
     * @throws UnsupportedOperationException if the component is not backed by a readable array.
     */
    int getReadableArrayLength();

    /**
     * Returns the readable array if one exists or throws an exception.
     *
     * @return the readable array that backs this buffer component.
     *
     * @throws UnsupportedOperationException if the component does not contain a readable portion.
     */
    ByteBuffer getReadableBuffer();

    /**
     * Returns the number of writable bytes in the buffer safely regards of
     * the actual memory backing this component.
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
     * @throws UnsupportedOperationException if the component is not backed by a writable array.
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
     * @throws UnsupportedOperationException if the component is not backed by an writable array.
     */
    byte[] getWritableArray();

    /**
     * @return the offset into the writable array where the writable bytes begin.
     *
     * @throws UnsupportedOperationException if the component is not backed by a writable array.
     */
    int getWritableArrayOffset();

    /**
     * @return the length of the writable array from the offset that is owned by this component..
     *
     * @throws UnsupportedOperationException if the component is not backed by a writable array.
     */
    int getWritableArrayLength();

    /**
     * Returns the writable {@link ByteBuffer} if one exists or throws an exception.
     *
     * @return the writable buffer that backs this buffer component.
     *
     * @throws UnsupportedOperationException if the component does not contain a writable portion.
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

}
