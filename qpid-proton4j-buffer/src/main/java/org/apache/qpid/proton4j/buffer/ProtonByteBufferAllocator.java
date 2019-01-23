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
 * Allocator for the default buffer type in Proton
 */
public final class ProtonByteBufferAllocator implements ProtonBufferAllocator {

    public static final ProtonByteBufferAllocator DEFAULT = new ProtonByteBufferAllocator();

    @Override
    public ProtonByteBuffer allocate() {
        return new ProtonByteBuffer();
    }

    @Override
    public ProtonByteBuffer allocate(int initialCapacity) {
        return new ProtonByteBuffer(initialCapacity);
    }

    @Override
    public ProtonByteBuffer allocate(int initialCapacity, int maximumCapacity) {
        return new ProtonByteBuffer(initialCapacity, maximumCapacity);
    }

    @Override
    public ProtonBuffer outputBuffer(int initialCapacity) {
        return allocate(initialCapacity);
    }

    @Override
    public ProtonBuffer outputBuffer(int initialCapacity, int maximumCapacity) {
        return allocate(initialCapacity, maximumCapacity);
    }

    @Override
    public ProtonBuffer wrap(byte[] array) {
        return new ProtonByteBuffer(array, array.length).slice();
    }

    @Override
    public ProtonBuffer wrap(byte[] array, int offset, int length) {
        return new ProtonByteBuffer(array, array.length).slice(offset, length);
    }

    @Override
    public ProtonBuffer wrap(ByteBuffer buffer) {
        if (buffer.isReadOnly() || buffer.isDirect()) {
            throw new UnsupportedOperationException("Cannot wrap direct or read-only buffers");
        }

        if (buffer.hasArray()) {
            return wrap(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            throw new UnsupportedOperationException("Cannot wrap buffer that are not array backed");
        }
    }
}
