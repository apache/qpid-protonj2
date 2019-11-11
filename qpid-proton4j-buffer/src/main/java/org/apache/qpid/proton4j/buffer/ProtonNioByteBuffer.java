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
 */
public class ProtonNioByteBuffer extends ProtonAbstractBuffer {

    // TODO - Operations in this class assume the originating buffer is zero indexed, one alternative is to
    //        slice but that might have unintended consequences.

    private final ByteBuffer buffer;

    public ProtonNioByteBuffer(ByteBuffer buffer) {
        this(buffer, buffer.remaining());
    }

    public ProtonNioByteBuffer(ByteBuffer buffer, int writeIndex) {
        super(buffer.remaining());

        this.buffer = buffer.slice();

        setIndex(0, writeIndex);
    }

    @Override
    public ByteBuffer unwrap() {
        return buffer;
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
        if (newCapacity < 0) {
            throw new IllegalArgumentException("Cannot alter a buffer's capacity to a negative value: " + newCapacity);
        } else {
            throw new UnsupportedOperationException("NIO Buffer wrapper cannot adjust capacity");
        }
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
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int destinationIndex, int length) {
        checkDestinationIndex(index, length, destinationIndex, destination.capacity());
        if (hasArray()) {
            destination.setBytes(destinationIndex, getArray(), getArrayOffset() + index, length);
        } else if (destination.hasArray()) {
            int position = buffer.position();

            buffer.position(index);
            buffer.get(destination.getArray(), destination.getArrayOffset() + destinationIndex, length);
            buffer.position(position);
        } else {
            while (length-- > 0) {
                destination.setByte(destinationIndex++, buffer.get(index++));
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        checkDestinationIndex(index, length, offset, destination.length);
        if (hasArray()) {
            System.arraycopy(getArray(), getArrayOffset() + index, destination, offset, length);
        } else {
            final int position = buffer.position();

            buffer.position(index);
            buffer.get(destination, offset, length);
            buffer.position(position);
        }
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        checkIndex(index, destination.remaining());
        if (destination.hasArray()) {
            final int position = buffer.position();

            buffer.position(index);
            buffer.get(destination.array(), destination.arrayOffset() + destination.position(), destination.remaining());
            buffer.position(position);

            destination.position(destination.limit());
        } else if (hasArray()) {
            destination.put(getArray(), getArrayOffset() + index, destination.remaining());
        } else {
            while (destination.hasRemaining()) {
                destination.put(getByte(index++));
            }
        }

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
        checkSourceIndex(index, length, sourceIndex, source.capacity());
        if (source.hasArray()) {
            final int position = buffer.position();

            buffer.position(index);
            buffer.put(source.getArray(), source.getArrayOffset() + sourceIndex, length);
            buffer.position(position);
        } else if (hasArray()) {
            source.getBytes(sourceIndex, getArray(), getArrayOffset() + index, length);
        } else {
            while (length-- > 0) {
                buffer.put(index++, source.getByte(sourceIndex++));
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.length);

        final int position = buffer.position();

        buffer.position(index);
        buffer.put(source, sourceIndex, length);
        buffer.position(position);

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        checkSourceIndex(index, source.remaining(), source.position(), source.capacity());

        final int position = buffer.position();

        buffer.position(index);
        buffer.put(source);
        buffer.position(position);

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
        checkIndex(index, length);

        int position = buffer.position();
        int limit = buffer.limit();

        buffer.position(index);
        buffer.limit(index + length);

        final ByteBuffer result = buffer.slice();

        buffer.limit(limit);
        buffer.position(position);

        return result;
    }
}
