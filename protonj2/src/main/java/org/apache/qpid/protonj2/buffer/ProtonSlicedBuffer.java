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
 * Presents a sliced view of a {@link ProtonAbstractBuffer}.  The slice wraps the
 * target buffer with a given offset into that buffer and a capped max capacity
 * that limits how far into the wrapped buffer the slice will read or write.
 *
 * A sliced buffer does not allow capacity changes and as such any call to alter
 * the capacity will result in an {@link UnsupportedOperationException}.
 */
public class ProtonSlicedBuffer extends ProtonAbstractBuffer {

    private final ProtonAbstractBuffer buffer;
    private final int indexOffset;

    /**
     * Creates a sliced view of the given {@link ProtonByteBuffer}.
     *
     * @param buffer
     *      The buffer that this slice is a view of.
     * @param offset
     *      The offset into the buffer where this view starts.
     * @param capacity
     *      The amount of the buffer that this view spans.
     */
    protected ProtonSlicedBuffer(ProtonAbstractBuffer buffer, int offset, int capacity) {
        super(capacity);

        checkSliceOutOfBounds(offset, capacity, buffer);

        if (buffer instanceof ProtonSlicedBuffer) {
            this.buffer = ((ProtonSlicedBuffer) buffer).buffer;
            this.indexOffset = ((ProtonSlicedBuffer) buffer).indexOffset + offset;
        } else {
            this.buffer = buffer;
            this.indexOffset = offset;
        }

        setWriteIndex(capacity);
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
        return offset(buffer.getArrayOffset());
    }

    @Override
    public int capacity() {
        return maxCapacity();
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        throw new UnsupportedOperationException("Cannot adjust capacity of a buffer slice.");
    }

    @Override
    public ProtonBuffer duplicate() {
        return buffer.duplicate().setIndex(offset(getReadIndex()), offset(getWriteIndex()));
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        checkIndex(index, length);
        return buffer.slice(offset(index), length);
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        checkIndex(index, length);
        return buffer.copy(offset(index), length);
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        return buffer.toByteBuffer(offset(index), length).slice();
    }

    //----- Overridden absolute get methods ----------------------------------//

    @Override
    public boolean getBoolean(int index) {
        checkIndex(index, 1);
        return buffer.getBoolean(offset(index));
    }

    @Override
    public byte getByte(int index) {
        checkIndex(index, 1);
        return buffer.getByte(offset(index));
    }

    @Override
    public short getUnsignedByte(int index) {
        checkIndex(index, 1);
        return buffer.getUnsignedByte(offset(index));
    }

    @Override
    public char getChar(int index) {
        checkIndex(index, Character.BYTES);
        return buffer.getChar(offset(index));
    }

    @Override
    public short getShort(int index) {
        checkIndex(index, Short.BYTES);
        return buffer.getShort(offset(index));
    }

    @Override
    public int getUnsignedShort(int index) {
        checkIndex(index, Short.BYTES);
        return buffer.getUnsignedShort(offset(index));
    }

    @Override
    public int getInt(int index) {
        checkIndex(index, Integer.BYTES);
        return buffer.getInt(offset(index));
    }

    @Override
    public long getUnsignedInt(int index) {
        checkIndex(index, Integer.BYTES);
        return buffer.getUnsignedInt(offset(index));
    }

    @Override
    public long getLong(int index) {
        checkIndex(index, Long.BYTES);
        return buffer.getLong(offset(index));
    }

    @Override
    public float getFloat(int index) {
        checkIndex(index, Float.BYTES);
        return buffer.getFloat(offset(index));
    }

    @Override
    public double getDouble(int index) {
        checkIndex(index, Double.BYTES);
        return buffer.getDouble(offset(index));
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer dst) {
        checkIndex(index, dst.getWritableBytes());
        buffer.getBytes(offset(index), dst);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer dst, int length) {
        checkIndex(index, length);
        buffer.getBytes(offset(index), dst);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer dst, int dstIndex, int length) {
        checkIndex(index, length);
        buffer.getBytes(offset(index), dst, dstIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] dst) {
        checkIndex(index, dst.length);
        buffer.getBytes(offset(index), dst);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] dst, int offset, int length) {
        checkIndex(index, length);
        buffer.getBytes(offset(index), dst, offset, length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        checkIndex(index, destination.remaining());
        buffer.getBytes(offset(index), destination);
        return this;
    }

    //----- Overridden absolute set methods ----------------------------------//

    @Override
    public ProtonBuffer setByte(int index, int value) {
        checkIndex(index, 1);
        buffer.setByte(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setBoolean(int index, boolean value) {
        checkIndex(index, 1);
        buffer.setBoolean(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, int value) {
        checkIndex(index, Character.BYTES);
        buffer.setChar(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        checkIndex(index, Short.BYTES);
        buffer.setShort(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        checkIndex(index, Integer.BYTES);
        buffer.setInt(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        checkIndex(index, Long.BYTES);
        buffer.setLong(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setFloat(int index, float value) {
        checkIndex(index, Float.BYTES);
        buffer.setFloat(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setDouble(int index, double value) {
        checkIndex(index, Double.BYTES);
        buffer.setDouble(offset(index), value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source) {
        checkIndex(index, source.getReadableBytes());
        buffer.setBytes(offset(index), source);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int length) {
        checkIndex(index, length);
        buffer.setBytes(offset(index), source, length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        checkIndex(index, length);
        buffer.setBytes(offset(index), source, sourceIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source) {
        checkIndex(index, source.length);
        buffer.setBytes(offset(index), source);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] src, int srcIndex, int length) {
        checkIndex(index, length);
        buffer.setBytes(offset(index), src, srcIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        checkIndex(index, source.remaining());
        buffer.setBytes(offset(index), source);
        return this;
    }

    //----- Internal utility methods -----------------------------------------//

    static void checkSliceOutOfBounds(int index, int length, ProtonAbstractBuffer buffer) {
        if (isOutOfBounds(index, length, buffer.capacity())) {
            throw new IndexOutOfBoundsException(buffer + ".slice(" + index + ", " + length + ')');
        }
    }

    private int offset(int index) {
        return index + indexOffset;
    }
}
