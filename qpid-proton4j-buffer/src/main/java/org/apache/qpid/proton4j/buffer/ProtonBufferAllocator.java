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
     * Create a new ProtonBuffer instance with the given initial capacity.
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
     *
     * @param array
     *      the byte array to wrap.
     *
     * @return a new ProtonBuffer that wraps the given array.
     */
    ProtonBuffer wrap(byte[] array);

    /**
     * Create a new ProtonBuffer that wraps the given ByteBuffer.
     *
     * @param buffer
     *      the ByteBuffer to wrap.
     *
     * @return a new ProtonBuffer that wraps the given ByteBuffer.
     */
    ProtonBuffer wrap(ByteBuffer buffer);

}
