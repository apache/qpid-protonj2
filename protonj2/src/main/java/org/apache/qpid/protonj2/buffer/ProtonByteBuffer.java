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
 * Implementation of the ProtonBuffer interface that uses an array backing
 * the buffer that is dynamically resized as bytes are written.
 */
public class ProtonByteBuffer extends ProtonAbstractBuffer {

    /**
     * The default initial capacity used for the underlying byte array.
     */
    public static final int DEFAULT_CAPACITY = 64;

    /**
     * The default maximum capacity that this buffer can grow to.
     */
    public static final int DEFAULT_MAXIMUM_CAPACITY = Integer.MAX_VALUE;

    private byte[] array;

    /**
     * Creates a new {@link ProtonByteBuffer} instance that uses default configuration values for
     * initial capacity and the maximum allowed capacity to which the underlying byte array will
     * grow before errors will be thrown from operations that would expand the storage.
     */
    public ProtonByteBuffer() {
        this(DEFAULT_CAPACITY, DEFAULT_MAXIMUM_CAPACITY);
    }

    /**
     * Creates a new {@link ProtonByteBuffer} with the given initial capacity and uses the default
     * value for the maximum capacity restriction.
     *
     * @param initialCapacity
     * 		The initial size of the backing byte store.
     *
     * @throws IllegalArgumentException if the given value is less than zero.
     */
    public ProtonByteBuffer(int initialCapacity) {
        this(initialCapacity, DEFAULT_MAXIMUM_CAPACITY);
    }

    /**
     * Creates a new {@link ProtonByteBuffer} with the given initial capacity and the given maximum
     * capacity restriction.
     *
     * @param initialCapacity
     * 		The initial size of the backing byte store.
     * @param maximumCapacity
     * 		The maximum size the backing byte store is allowed to grow.
     *
     * @throws IllegalArgumentException if the given value is less than zero or greater than the maximum.
     */
    public ProtonByteBuffer(int initialCapacity, int maximumCapacity) {
        super(maximumCapacity);

        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial capacity cannot be < 0");
        }

        if (initialCapacity > maximumCapacity) {
            throw new IllegalArgumentException("Initial capacity cannot exceed maximum capacity.");
        }

        this.array = new byte[initialCapacity];
    }

    /**
     * Creates a new {@link ProtonByteBuffer} with the given byte array as the backing store to be used
     * initially.  The buffer uses the default value for maximum capacity meaning as the buffer is written
     * to the backing store will eventually be reallocated and no longer wrap the original array.  The
     * resulting buffer will have a read index of zero and a write index set to the size of the backing
     * array.
     *
     * @param array
     * 		The initial array use use as the backing store for this byte buffer.
     */
    public ProtonByteBuffer(byte[] array) {
        this(array, DEFAULT_MAXIMUM_CAPACITY);
    }

    protected ProtonByteBuffer(byte[] array, int maximumCapacity) {
        this(array, maximumCapacity, array.length);
    }

    protected ProtonByteBuffer(byte[] array, int maximumCapacity, int writeIndex) {
        super(maximumCapacity);

        if (array == null) {
            throw new NullPointerException("Array to wrap cannot be null");
        }

        this.array = array;

        setIndex(0, writeIndex);
    }

    @Override
    public int capacity() {
        return array.length;
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        checkNewCapacity(newCapacity);

        int oldCapacity = array.length;
        if (newCapacity > oldCapacity) {
            byte[] newArray = new byte[newCapacity];
            System.arraycopy(array, 0, newArray, 0, array.length);
            array = newArray;
        } else if (newCapacity < oldCapacity) {
            byte[] newArray = new byte[newCapacity];
            int readIndex = getReadIndex();
            if (readIndex < newCapacity) {
                int writeIndex = getWriteIndex();
                if (writeIndex > newCapacity) {
                    setWriteIndex(writeIndex = newCapacity);
                }
                System.arraycopy(array, readIndex, newArray, readIndex, writeIndex - readIndex);
            } else {
                setIndex(newCapacity, newCapacity);
            }

            array = newArray;
        }
        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        checkIndex(index, length);
        byte[] copyOf = new byte[length];
        System.arraycopy(array, index, copyOf, 0, length);
        return new ProtonByteBuffer(copyOf, maxCapacity(), length);
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        return ByteBuffer.wrap(array, index, length).slice();
    }

    @Override
    public boolean hasArray() {
        return true;
    }

    @Override
    public byte[] getArray() {
        return array;
    }

    @Override
    public int getArrayOffset() {
        return 0;
    }

    //----- Direct indexed get methods ---------------------------------------//

    @Override
    public byte getByte(int index) {
        return ProtonByteUtils.readByte(array, index);
    }

    @Override
    public short getShort(int index) {
        return ProtonByteUtils.readShort(array, index);
    }

    @Override
    public int getInt(int index) {
        return ProtonByteUtils.readInt(array, index);
    }

    @Override
    public long getLong(int index) {
        return ProtonByteUtils.readLong(array, index);
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int destinationIndex, int length) {
        checkDestinationIndex(index, length, destinationIndex, destination.capacity());

        if (destination.hasArray()) {
            System.arraycopy(array, index, destination.getArray(), destination.getArrayOffset() + destinationIndex, length);
        } else {
            destination.setBytes(destinationIndex, array, index, length);
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int destinationIndex, int length) {
        checkDestinationIndex(index, length, destinationIndex, destination.length);
        System.arraycopy(array, index, destination, destinationIndex, length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        checkIndex(index, destination.remaining());
        destination.put(array, index, destination.remaining());
        return this;
    }

    //----- Direct indexed set methods ---------------------------------------//

    @Override
    public ProtonBuffer setByte(int index, int value) {
        ProtonByteUtils.writeByte((byte) value, array, index);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        ProtonByteUtils.writeShort((short) value, array, index);
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        ProtonByteUtils.writeInt(value, array, index);
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        ProtonByteUtils.writeLong(value, array, index);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.capacity());
        if (source.hasArray()) {
            System.arraycopy(source.getArray(), source.getArrayOffset() + sourceIndex, array, index, length);
        } else {
            source.getBytes(sourceIndex, array, index, length);
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.length);
        System.arraycopy(source, sourceIndex, array, index, length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer src) {
        src.get(array, index, src.remaining());
        return this;
    }
}
