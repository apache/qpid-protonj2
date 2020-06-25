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
package org.apache.qpid.protonj2.buffer.util;

import org.apache.qpid.protonj2.buffer.ProtonByteBuffer;

/**
 * Used in testing of ProtonBuffers
 */
public class ProtonTestByteBuffer extends ProtonByteBuffer {

    private boolean hasArray;

    public ProtonTestByteBuffer() {
        this(ProtonByteBuffer.DEFAULT_CAPACITY, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, true);
    }

    public ProtonTestByteBuffer(boolean hasArray) {
        this(ProtonByteBuffer.DEFAULT_CAPACITY, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, hasArray);
    }

    public ProtonTestByteBuffer(int initialCapacity) {
        this(initialCapacity, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, true);
    }

    public ProtonTestByteBuffer(int initialCapacity, boolean hasArray) {
        this(initialCapacity, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, hasArray);
    }

    public ProtonTestByteBuffer(int initialCapacity, int maximumCapacity) {
        super(initialCapacity, maximumCapacity);
    }

    public ProtonTestByteBuffer(int initialCapacity, int maximumCapacity, boolean hasArray) {
        super(initialCapacity, maximumCapacity);

        this.hasArray = hasArray;
    }

    public ProtonTestByteBuffer(byte[] array) {
        super(array);
    }

    public ProtonTestByteBuffer(byte[] array, int maximumCapacity) {
        super(array, maximumCapacity);
    }

    public ProtonTestByteBuffer(byte[] array, int maximumCapacity, int writeIndex) {
        super(array, maximumCapacity, writeIndex);
    }

    @Override
    public boolean hasArray() {
        return hasArray ? hasArray() : false;
    }

    @Override
    public
    byte[] getArray() {
        if (!hasArray) {
            throw new UnsupportedOperationException();
        }

        return getArray();
    }

    @Override
    public int getArrayOffset() {
        if (!hasArray) {
            throw new UnsupportedOperationException();
        }

        return getArrayOffset();
    }
}
