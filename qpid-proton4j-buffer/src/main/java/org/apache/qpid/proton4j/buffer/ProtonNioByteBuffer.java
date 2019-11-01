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
 * ProtonBuffer wrapper around a NIO ByteBuffer instance.
 *
 * TODO - Decide if we want to implement this, many limitations and issues
 *        around this as buffer can be read-only etc.
 */
public class ProtonNioByteBuffer extends ProtonAbstractBuffer {

    private final ByteBuffer buffer;

    protected ProtonNioByteBuffer(ByteBuffer buffer) {
        super(buffer.remaining());

        this.buffer = buffer.slice();
    }

    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }

    @Override
    public byte[] getArray() {
        return buffer.array();
    }

    @Override
    public int getArrayOffset() {
        return buffer.arrayOffset();
    }

    @Override
    public int capacity() {
        return buffer.remaining();
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        throw new UnsupportedOperationException("NIO Buffer wrapper cannot adjust capacity");
    }

    @Override
    public byte getByte(int index) {
        return buffer.get(index);
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
    public ProtonBuffer getBytes(int index, ProtonBuffer dst, int dstIndex, int length) {
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] target, int offset, int length) {
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        return this;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        buffer.put(index, (byte) value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        buffer.putShort(index, (short) value);
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        buffer.putInt(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        buffer.putLong(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] src, int srcIndex, int length) {
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer src) {
        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        ProtonByteBuffer buffer = new ProtonByteBuffer(length);
        getBytes(index, buffer, length);
        return buffer;
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        getBytes(index, buffer);
        return buffer;
    }
}
