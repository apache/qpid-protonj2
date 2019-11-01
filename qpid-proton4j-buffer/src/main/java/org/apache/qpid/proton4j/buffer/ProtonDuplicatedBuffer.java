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
import java.util.Objects;

/**
 * A duplicated buffer wrapper for buffers known to be {@link ProtonAbstractBuffer} instances.
 */
public class ProtonDuplicatedBuffer extends ProtonAbstractBuffer {

    private final ProtonAbstractBuffer buffer;

    /**
     * Wrap the given buffer to present a duplicate buffer with independent
     * read and write index values.
     *
     * @param buffer
     *      The {@link ProtonAbstractBuffer} instance to wrap with this instance.
     */
    public ProtonDuplicatedBuffer(ProtonAbstractBuffer buffer) {
        super(buffer.maxCapacity());

        Objects.requireNonNull(buffer, "The buffer being wrapped by a duplicate must not be null");

        if (buffer instanceof ProtonDuplicatedBuffer) {
            this.buffer = ((ProtonDuplicatedBuffer) buffer).buffer;
        } else {
            this.buffer = buffer;
        }

        setIndex(buffer.getReadIndex(), buffer.getWriteIndex());
        markReadIndex();
        markWriteIndex();
    }

    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }

    @Override
    public byte[] getArray() {
        return buffer.getArray();
    }

    @Override
    public int getArrayOffset() {
        return buffer.getArrayOffset();
    }

    @Override
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        return buffer.capacity(newCapacity);
    }

    @Override
    public ProtonBuffer duplicate() {
        return buffer.duplicate();
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        return buffer.slice(index, length);
    }

    @Override
    public byte getByte(int index) {
        return buffer.getByte(index);
    }

    @Override
    public short getShort(int index) {
        return buffer.getShort(index);
    }

    @Override
    public int getInt(int index) {
        return buffer.getInt(index);
    }

    @Override
    public long getLong(int index) {
        return buffer.getLong(index);
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int destinationIndex, int length) {
        buffer.getBytes(index, destination, destinationIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        buffer.getBytes(index, destination, offset, length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        buffer.getBytes(index, destination);
        return this;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        buffer.setByte(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        buffer.setShort(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        buffer.setInt(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        buffer.setLong(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        buffer.setBytes(index, source, sourceIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        buffer.setBytes(index, source, sourceIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        buffer.setBytes(index, source);
        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        return buffer.copy(index, length);
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        return buffer.toByteBuffer(index, length);
    }
}
