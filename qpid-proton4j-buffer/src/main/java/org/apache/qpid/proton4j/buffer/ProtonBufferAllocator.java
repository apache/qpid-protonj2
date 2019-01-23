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
package org.apache.qpid.proton4j.buffer;

import java.nio.ByteBuffer;

/**
 * Interface for a ProtonBuffer allocator object that can be used by Proton
 * objects to create memory buffers using the preferred type of the application
 * or library that embeds the Proton engine.
 */
public interface ProtonBufferAllocator {

    /**
     * Create a new output ProtonBuffer instance with the given initial capacity and the
     * maximum capacity should be that of the underlying buffer implementations limit.
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
     * Create a new output ProtonBuffer instance with the given initial capacity and the
     * maximum capacity should that of the value specified by the caller.
     * <p>
     * The returned buffer will be used for frame output from the Proton engine and
     * can be a pooled buffer which the IO handler will then need to release once
     * the buffer has been written.
     *
     * @param initialCapacity
     *      The initial capacity to use when creating the new ProtonBuffer.
     * @param maximumCapacity
     *      The largest amount of bytes the new ProtonBuffer is allowed to grow to.
     *
     * @return a new ProtonBuffer instance with the given initial capacity.
     */
    ProtonBuffer outputBuffer(int initialCapacity, int maximumCapacity);

    // TODO - For these we should document if the assumption is that
    //        these buffers are never allowed to be pooled since we
    //        are not going to have defined release points.

    /**
     * Create a new ProtonBuffer instance with default initial capacity.
     *
     * @return a new ProtonBuffer instance with default initial capacity.
     */
    ProtonBuffer allocate();

    /**
     * Create a new ProtonBuffer instance with the given initial capacity and the
     * maximum capacity should be that of the underlying buffer implementations
     * limit.
     *
     * @param initialCapacity
     *      The initial capacity to use when creating the new ProtonBuffer.
     *
     * @return a new ProtonBuffer instance with the given initial capacity.
     */
    ProtonBuffer allocate(int initialCapacity);

    /**
     * Create a new ProtonBuffer instance with the given initial capacity and the
     * maximum capacity should that of the value specified by the caller.
     *
     * @param initialCapacity
     *      The initial capacity to use when creating the new ProtonBuffer.
     * @param maximumCapacity
     *      The largest amount of bytes the new ProtonBuffer is allowed to grow to.
     *
     * @return a new ProtonBuffer instance with the given initial capacity.
     */
    ProtonBuffer allocate(int initialCapacity, int maximumCapacity);

    /**
     * Create a new ProtonBuffer that wraps the given byte array.
     * <p>
     * The capacity and maximum capacity for the resulting ProtonBuffer should equal
     * to the length of the wrapped array and the returned array offset is zero.
     *
     * @param array
     *      the byte array to wrap.
     *
     * @return a new ProtonBuffer that wraps the given array.
     */
    ProtonBuffer wrap(byte[] array);

    /**
     * Create a new ProtonBuffer that wraps the given byte array using the provided
     * offset and length values to confine the view of that array.
     * <p>
     * The capacity and maximum capacity for the resulting ProtonBuffer should equal
     * to the length of the wrapped array and the returned array offset is zero.
     *
     * @param array
     *      the byte array to wrap.
     * @param offset
     *      the offset into the array where the view begins.
     * @param length
     *      the number of bytes in the array to expose
     *
     * @return a new ProtonBuffer that wraps the given array.
     */
    ProtonBuffer wrap(byte[] array, int offset, int length);

    /**
     * Create a new ProtonBuffer that wraps the given ByteBuffer.
     * <p>
     * The capacity and maximum capacity of the returned ProtonBuffer will be the
     * same as that of the underlying ByteBuffer.  The ProtonBuffer will return true
     * from the {@link ProtonBuffer#hasArray()} method only when the wrapped ByteBuffer
     * reports that it is backed by an array.
     *
     * @param buffer
     *      the ByteBuffer to wrap.
     *
     * @return a new ProtonBuffer that wraps the given ByteBuffer.
     */
    ProtonBuffer wrap(ByteBuffer buffer);

    // TODO - Methods that could offer distinctions on what can or cannot be pulled

    // Buffer used to house the delivery portion of an incoming transport which can be
    // pooled if we have defined lifetime for these buffers.
    // ProtonBuffer deliveryBuffer(int initialCapacity);

    // Buffer used for outbound frame encoding, can be pooled if frame offers release point.
    // ProtonBuffer frameBuffer(int initialCapacity);

    // Buffer used in decoding operations for message sections which shouldn't be pooled
    // ProtonBuffer unpooled(int initialCapacity);

    // Buffer used in short term operations like buffering partial frames ?
    // ProtonBuffer poolable(int initialCapacity);

}
