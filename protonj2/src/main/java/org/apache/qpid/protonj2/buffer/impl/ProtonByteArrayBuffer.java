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

package org.apache.qpid.protonj2.buffer.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.ProtonBufferIterator;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.resource.SharedResource;

/**
 * An array based buffer implementation.
 */
public final class ProtonByteArrayBuffer extends SharedResource<ProtonBuffer> implements ProtonBuffer, ProtonBufferComponent, ProtonBufferComponentAccessor {

    /**
     * The default initial capacity used for the underlying byte array.
     */
    public static final int DEFAULT_CAPACITY = 64;

    /**
     * The default maximum capacity that this buffer can grow to.
     */
    public static final int DEFAULT_MAXIMUM_CAPACITY = Integer.MAX_VALUE - 8;

    private static final int CLOSED_MARKER = -1;

    /**
     * The backing array for this buffer
     */
    private byte[] array;

    /**
     * The offset into the array where this buffers index 0 begins.
     */
    private int arrayOffset;

    /**
     * Tracks the readable capacity of this buffer.  This value is calculated based on
     * the given target array and the offset value at create time plus any create time
     * restriction that limits the usable sub-region of the array to less than the total
     * length minus the offset
     */
    private int readCapacity;

    /**
     * Tracks the read capacity for buffers that are both readable and writable and is
     * set to the closed state for a buffer than has been made read only.
     */
    private int writeCapacity;

    /**
     * The maximum value that the buffer can grow automatically before write operations
     * that would expand capacity will throw exceptions.
     */
    private int implicitGrowthLimit = DEFAULT_MAXIMUM_CAPACITY;

    private int readOffset;
    private int writeOffset;

    private boolean readOnly;
    private boolean closed;

    /**
     * Creates a new {@link ProtonByteArrayBuffer} instance that uses default configuration values for
     * initial capacity and the maximum allowed capacity to which the underlying byte array will
     * grow before errors will be thrown from operations that would expand the storage.
     */
    public ProtonByteArrayBuffer() {
        this(DEFAULT_CAPACITY, DEFAULT_MAXIMUM_CAPACITY);
    }

    /**
     * Creates a new {@link ProtonByteArrayBuffer} with the given initial capacity and uses the default
     * value for the maximum capacity restriction.
     *
     * @param initialCapacity
     * 		The initial size of the backing byte store.
     *
     * @throws IllegalArgumentException if the given value is less than zero.
     */
    public ProtonByteArrayBuffer(int initialCapacity) {
        this(initialCapacity, DEFAULT_MAXIMUM_CAPACITY);
    }

    /**
     * Creates a new {@link ProtonByteArrayBuffer} with the given initial capacity and the given maximum
     * capacity restriction.
     *
     * @param initialCapacity
     * 		The initial size of the backing byte store.
     * @param implicitGrowthLimit
     * 		The maximum size the backing byte store is allowed to grow.
     *
     * @throws IllegalArgumentException if the given value is less than zero or greater than the maximum.
     */
    public ProtonByteArrayBuffer(int initialCapacity, int implicitGrowthLimit) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial capacity cannot be < 0");
        }

        if (initialCapacity > implicitGrowthLimit) {
            throw new IllegalArgumentException("Initial capacity cannot exceed maximum capacity.");
        }

        this.array = new byte[initialCapacity];
        this.arrayOffset = 0;
        this.readCapacity = array.length;
        this.writeCapacity = array.length;
        this.implicitGrowthLimit = implicitGrowthLimit;
    }

    /**
     * Create a new proton byte buffer instance with given backing array whose
     * size determines that largest the buffer can ever be.
     *
     * @param backingArray
     * 		The byte array that will back the buffer instance
     */
    public ProtonByteArrayBuffer(byte[] backingArray) {
        this(backingArray, 0, DEFAULT_MAXIMUM_CAPACITY);
    }

    /**
     * Create a new proton byte buffer instance with given backing array as the
     * starting backing store and uses the provided max capacity value to control
     * how large the buffer could ever grow.
     * @param backingArray
     * 		The byte array that will back the buffer instance
     * @param implicitGrowthLimit
     *      The maximum to which this buffer can grow implicitly (without calls to ensureWritable).
     */
    public ProtonByteArrayBuffer(byte[] backingArray, int implicitGrowthLimit) {
        this(backingArray, 0, implicitGrowthLimit);
    }

    /**
     * Create a new proton byte buffer instance with given backing array as the
     * starting backing store and uses the provided max capacity value to control
     * how large the buffer could ever grow.
     *
     * @param backingArray
     * 		The byte array that will back the buffer instance
     * @param arrayOffset
     * 		The offset into the backing array where the buffer starts.
     * @param implicitGrowthLimit
     *      The maximum to which this buffer can grow implicitly (without calls to ensureWritable).
     */
    public ProtonByteArrayBuffer(byte[] backingArray, int arrayOffset, int implicitGrowthLimit) {
        this(backingArray, arrayOffset, backingArray.length - arrayOffset, implicitGrowthLimit);
    }

    /**
     * Create a new proton byte buffer instance with given backing array as the
     * starting backing store and uses the provided max capacity value to control
     * how large the buffer could ever grow.
     *
     * @param backingArray
     * 		The byte array that will back the buffer instance
     * @param arrayOffset
     * 		The offset into the backing array where the buffer starts.
     * @param capacity
     *      The capacity limit for this view of the provided backing array.
     * @param implicitGrowthLimit
     *      The maximum to which this buffer can grow implicitly (without calls to ensureWritable).
     */
    public ProtonByteArrayBuffer(byte[] backingArray, int arrayOffset, int capacity, int implicitGrowthLimit) {
        if (arrayOffset > backingArray.length) {
            throw new IndexOutOfBoundsException("Array offset cannot exceed the array length");
        }

        if (capacity > backingArray.length - arrayOffset) {
            throw new IndexOutOfBoundsException(
                "Array segment capacity cannot exceed the configured array length minus the offset");
        }

        this.array = backingArray;
        this.arrayOffset = arrayOffset;
        this.readCapacity = capacity;
        this.writeCapacity = capacity;
        this.implicitGrowthLimit = implicitGrowthLimit;
    }

    // For use in transfer to quickly setup the new facade around the array
    private ProtonByteArrayBuffer(byte[] backingArray, int arrayOffset, boolean readOnly) {
        this.array = backingArray;
        this.arrayOffset = arrayOffset;
        this.readOnly = readOnly;
    }

    @Override
    public ProtonBuffer unwrap() {
        return this;
    }

    @Override
    public String toString() {
        return "ProtonByteArrayBuffer" +
               "{ read:" + readOffset +
               ", write: " + writeOffset +
               ", capacity: " + readCapacity + "}";
    }

    @Override
    public boolean isDirect() {
        return false;
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    @Override
    public int componentCount() {
        return 1;
    }

    @Override
    public int readableComponentCount() {
        return isReadable() ? 1 : 0;
    }

    @Override
    public int writableComponentCount() {
        return isWritable() ? 1 : 0;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public ProtonByteArrayBuffer convertToReadOnly() {
        readOnly = true;
        writeCapacity = CLOSED_MARKER;
        return this;
    }

    @Override
    public int capacity() {
        return Math.max(0, readCapacity);
    }

    @Override
    public int getReadableBytes() {
        return writeOffset - readOffset;
    }

    @Override
    public int getWritableBytes() {
        return Math.max(0, writeCapacity - writeOffset);
    }

    @Override
    public int getReadOffset() {
        return readOffset;
    }

    @Override
    public int getWriteOffset() {
        return writeOffset;
    }

    @Override
    public ProtonBuffer setWriteOffset(int value) {
        checkWrite(value, 0, false);
        writeOffset = value;
        return this;
    }

    @Override
    public ProtonBuffer setReadOffset(int value) {
        checkRead(value, 0);
        readOffset = value;
        return this;
    }

    @Override
    public ProtonBuffer fill(byte value) {
        checkSet(0, 1);
        Arrays.fill(array, arrayOffset, arrayOffset + readCapacity, value);
        return this;
    }

    @Override
    public ProtonBuffer split(int splitOffset) {
        ProtonBufferUtils.checkIsNotNegative(splitOffset, "The split offset cannot be negative");

        if (capacity() < splitOffset) {
            throw new IllegalArgumentException(
                "The split offset cannot be greater than the buffer capacity, " +
                "but the split offset was " + splitOffset + ", and capacity is " + capacity() + '.');
        }
        if (isClosed()) {
            throw new ProtonBufferClosedException("Cannot split a closed buffer");
        }

        ProtonByteArrayBuffer front = new ProtonByteArrayBuffer(array, arrayOffset, splitOffset, DEFAULT_MAXIMUM_CAPACITY);
        front.writeOffset = Math.min(writeOffset, splitOffset);
        front.readOffset = Math.min(readOffset, splitOffset);
        if (isReadOnly()) {
            front.convertToReadOnly();
        }

        // This buffer realigned to house only the tail of the split
        arrayOffset += splitOffset;
        readCapacity -= splitOffset;
        writeCapacity = isReadOnly() ? CLOSED_MARKER : readCapacity;
        writeOffset = Math.max(writeOffset, splitOffset) - splitOffset;
        readOffset = Math.max(readOffset, splitOffset) - splitOffset;

        return front;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProtonBuffer && ProtonBufferUtils.equals(this, (ProtonBuffer) o);
    }

    @Override
    public int hashCode() {
        return ProtonBufferUtils.hashCode(this);
    }

    //----- Indexed Get operations

    @Override
    public byte getByte(int index) {
        checkGet(index, Byte.BYTES);
        return ProtonBufferUtils.readByte(array, offset(index));
    }

    @Override
    public char getChar(int index) {
        checkGet(index, Character.BYTES);
        return ProtonBufferUtils.readChar(array, offset(index));
    }

    @Override
    public short getShort(int index) {
        checkGet(index, Short.BYTES);
        return ProtonBufferUtils.readShort(array, offset(index));
    }

    @Override
    public int getInt(int index) {
        checkGet(index, Integer.BYTES);
        return ProtonBufferUtils.readInt(array, offset(index));
    }

    @Override
    public long getLong(int index) {
        checkGet(index, Long.BYTES);
        return ProtonBufferUtils.readLong(array, offset(index));
    }

    //----- Offset based read operations

    @Override
    public byte readByte() {
        checkRead(readOffset, Byte.BYTES);
        final byte result = ProtonBufferUtils.readByte(array, offset(readOffset));
        readOffset += Byte.BYTES;
        return result;
    }

    @Override
    public char readChar() {
        checkRead(readOffset, Character.BYTES);
        final char result = ProtonBufferUtils.readChar(array, offset(readOffset));
        readOffset += Character.BYTES;
        return result;
    }

    @Override
    public short readShort() {
        checkRead(readOffset, Short.BYTES);
        final short result = ProtonBufferUtils.readShort(array, offset(readOffset));
        readOffset += Short.BYTES;
        return result;
    }

    @Override
    public int readInt() {
        checkRead(readOffset, Integer.BYTES);
        final int result = ProtonBufferUtils.readInt(array, offset(readOffset));
        readOffset += Integer.BYTES;
        return result;
    }

    @Override
    public long readLong() {
        checkRead(readOffset, Long.BYTES);
        final long result = ProtonBufferUtils.readLong(array, offset(readOffset));
        readOffset += Long.BYTES;
        return result;
    }

    //----- Indexed Set operations

    @Override
    public ProtonBuffer setByte(int index, byte value) {
        checkSet(index, Byte.BYTES);
        ProtonBufferUtils.writeByte(value, array, offset(index));
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, char value) {
        checkSet(index, Character.BYTES);
        ProtonBufferUtils.writeChar(value, array, offset(index));
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, short value) {
        checkSet(index, Short.BYTES);
        ProtonBufferUtils.writeShort(value, array, offset(index));
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        checkSet(index, Integer.BYTES);
        ProtonBufferUtils.writeInt(value, array, offset(index));
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        checkSet(index, Long.BYTES);
        ProtonBufferUtils.writeLong(value, array, offset(index));
        return this;
    }

    //----- Offset based Write operations

    @Override
    public ProtonBuffer writeByte(byte value) {
        checkWrite(writeOffset, Byte.BYTES, true);
        ProtonBufferUtils.writeByte(value, array, offset(writeOffset++));
        return this;
    }

    @Override
    public ProtonBuffer writeChar(char value) {
        checkWrite(writeOffset, Character.BYTES, true);
        ProtonBufferUtils.writeChar(value, array, offset(writeOffset));
        writeOffset += Character.BYTES;
        return this;
    }

    @Override
    public ProtonBuffer writeShort(short value) {
        checkWrite(writeOffset, Short.BYTES, true);
        ProtonBufferUtils.writeShort(value, array, offset(writeOffset));
        writeOffset += Short.BYTES;
        return this;
    }

    @Override
    public ProtonBuffer writeInt(int value) {
        checkWrite(writeOffset, Integer.BYTES, true);
        ProtonBufferUtils.writeInt(value, array, offset(writeOffset));
        writeOffset += Integer.BYTES;
        return this;
    }

    @Override
    public ProtonBuffer writeLong(long value) {
        checkWrite(writeOffset, Long.BYTES, true);
        ProtonBufferUtils.writeLong(value, array, offset(writeOffset));
        writeOffset += Long.BYTES;
        return this;
    }

    //----- Buffer Copy and Compaction API

    @Override
    public ProtonBuffer copy(int offset, int length, boolean readOnly) throws IllegalArgumentException {
        ProtonBufferUtils.checkLength(length);

        final ProtonByteArrayBuffer result;

        if (readOnly && isReadOnly()) {
            result = new ProtonByteArrayBuffer(array, offset(offset), length, implicitGrowthLimit);
            result.writeOffset = length;
        } else {
            checkGet(offset, length);
            byte[] copyBytes = Arrays.copyOfRange(array, offset(offset), offset(offset) + length);

            result = new ProtonByteArrayBuffer(copyBytes);
            result.writeOffset = length;
        }

        if (readOnly) {
            result.convertToReadOnly();
        }

        return result;
    }

    @Override
    public void copyInto(int offset, byte[] destination, int destOffset, int length) {
        checkCopyIntoArgs(offset, length, destOffset, destination.length);
        System.arraycopy(array, offset(offset), destination, destOffset, length);
    }

    @Override
    public void copyInto(int offset, ByteBuffer destination, int destOffset, int length) {
        if (destination.isReadOnly()) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }
        checkCopyIntoArgs(offset, length, destOffset, destination.capacity());

        if (destination.hasArray()) {
            System.arraycopy(array, offset(offset), destination.array(), destination.arrayOffset() + destOffset, length);
        } else {
            final int oldPos = destination.position();
            try {
                destination.position(destOffset);
                destination.put(array, offset(offset), length);
            } finally {
                destination.position(oldPos);
            }
        }
    }

    @Override
    public void copyInto(int offset, ProtonBuffer destination, int destOffset, int length) {
        ProtonBufferUtils.checkIsClosed(destination);
        ProtonBufferUtils.checkIsReadOnly(destination);

        checkCopyIntoArgs(offset, length, destOffset, destination.capacity());

        final int originalReadOffset = destination.getReadOffset();
        final int originalWriteOffset = destination.getWriteOffset();
        destination.setReadOffset(0);
        destination.setWriteOffset(destOffset);
        try {
            destination.writeBytes(array, offset(offset), length);
        } finally {
            destination.setReadOffset(originalReadOffset);
            destination.setWriteOffset(originalWriteOffset);
        }
    }

    @Override
    public ProtonBuffer writeBytes(byte[] source, int offset, int length) {
        checkWrite(writeOffset, length, true);
        System.arraycopy(source, offset, array, offset(writeOffset), length);
        writeOffset += length;

        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer source) {
        final int length = source.getReadableBytes();
        checkWrite(writeOffset, length, true);
        source.readBytes(array, arrayOffset + writeOffset, length);
        writeOffset += length;

        return this;
    }

    @Override
    public ProtonBuffer readBytes(ByteBuffer destination) {
        final int byteCount = destination.remaining();
        checkCopyIntoArgs(readOffset, byteCount, destination.position(), destination.capacity());
        destination.put(array, offset(readOffset), byteCount);
        readOffset += byteCount;
        return this;
    }

    @Override
    public ProtonBuffer readBytes(byte[] destination, int offset, int length) {
        checkCopyIntoArgs(readOffset, length, offset, destination.length);
        System.arraycopy(array, offset(readOffset), destination, offset, length);
        readOffset += length;

        return this;
    }

    //----- Buffer size management API

    @Override
    public int implicitGrowthLimit() {
        return implicitGrowthLimit;
    }

    @Override
    public ProtonBuffer implicitGrowthLimit(int limit) {
        ProtonBufferUtils.checkImplicitGrowthLimit(limit, capacity());
        this.implicitGrowthLimit = limit;
        return this;
    }

    @Override
    public ProtonBuffer ensureWritable(int size, int minimumGrowth, boolean allowCompaction) throws IndexOutOfBoundsException, IllegalArgumentException {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (size < 0) {
            throw new IllegalArgumentException("Cannot ensure writable for a negative size: " + size + '.');
        }
        if (minimumGrowth < 0) {
            throw new IllegalArgumentException("The minimum growth cannot be negative: " + minimumGrowth + '.');
        }
        if (writeCapacity == CLOSED_MARKER) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }
        if (getWritableBytes() > size) {
            return this;
        }
        if (allowCompaction && getWritableBytes() + getReadOffset() >= size) {
            return compact();
        }

        final long newSize = capacity() + (long) Math.max(size - getWritableBytes(), minimumGrowth);
        ProtonBufferUtils.checkIsNotNegative(newSize, "The buffer cannot be resized to a negative value");
        if (newSize > DEFAULT_MAXIMUM_CAPACITY) {
            throw new IllegalArgumentException(
                "The buffer cannot grow to a size greater than " + DEFAULT_MAXIMUM_CAPACITY + ", requested size was " + newSize);
        }

        byte[] newArray = new byte[(int) newSize];
        copyInto(0, newArray, 0, capacity());

        this.array = newArray;
        this.arrayOffset = 0;
        this.readCapacity = array.length;
        this.writeCapacity = readOnly ? CLOSED_MARKER : array.length;

        return this;
    }

    @Override
    public ProtonBuffer compact() {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (isReadOnly()) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }

        if (readOffset != 0) {
            System.arraycopy(array, arrayOffset + readOffset, array, 0, writeOffset - readOffset);
            writeOffset -= readOffset;
            readOffset = 0;
        }

        return this;
    }

    //----- Buffer IO interoperability handlers

    @Override
    public int transferTo(WritableByteChannel channel, int length) throws IOException {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "TransferTo length cannot be negative: " + length);

        final int writableBytes = Math.min(getReadableBytes(), length);

        checkGet(readOffset, writableBytes);

        if (writableBytes == 0) {
            return 0;
        }

        final ByteBuffer ioWrapper = ByteBuffer.wrap(array, readOffset, writableBytes);
        final int actualWrite = channel.write(ioWrapper);
        readOffset += actualWrite;

        return actualWrite;
    }

    @Override
    public int transferFrom(ReadableByteChannel channel, int length) throws IOException {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsReadOnly(this);

        length = Math.min(getWritableBytes(), length);

        if (length == 0) {
            return 0;
        }

        checkSet(getWriteOffset(), length);
        final ByteBuffer target = getWritableBuffer().limit(length);
        final int bytesRead = channel.read(target.limit(target.position() + length));
        if (bytesRead != -1) {
            writeOffset += bytesRead;
        }

        return bytesRead;
    }

    @Override
    public int transferFrom(FileChannel channel, long position, int length) throws IOException {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsReadOnly(this);

        length = Math.min(getWritableBytes(), length);

        if (length == 0) {
            return 0;
        }

        checkSet(getWriteOffset(), length);
        final ByteBuffer target = getWritableBuffer().limit(length);
        final int bytesRead = channel.read(target.limit(target.position() + length), position);
        if (bytesRead != -1) {
            writeOffset += bytesRead;
        }

        return bytesRead;
    }

    //----- Buff component access

    @Override
    public ProtonBufferComponentAccessor componentAccessor() {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        return (ProtonBufferComponentAccessor) acquire();
    }

    @Override
    public ProtonBufferComponent first() {
        return this;
    }

    @Override
    public ProtonBufferComponent next() {
        return null; // There is never a next.
    }

    //----- Buffer iteration API

    @Override
    public ProtonBufferIterator bufferIterator() {
        return bufferIterator(getReadOffset(), getReadableBytes());
    }

    @Override
    public ProtonBufferIterator bufferIterator(int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkArgumentIsNotNegative(offset, "offset");
        ProtonBufferUtils.checkArgumentIsNotNegative(length, "length");

        checkGet(offset, length);

        return new ProtonByteArrayBufferIterator(array, offset(offset), length);
    }

    @Override
    public ProtonBufferIterator bufferReverseIterator(int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkArgumentIsNotNegative(offset, "offset");
        ProtonBufferUtils.checkArgumentIsNotNegative(length, "length");

        if (offset >= capacity()) {
            throw new IndexOutOfBoundsException(
                "Read offset must be within the bounds of the buffer: offset = " + offset + ", capacity = " + capacity());
        }

        if (offset - length < -1) {
            throw new IndexOutOfBoundsException(
                "Cannot read past start of buffer: offset = " + offset + ", length = " + length);
        }

        return new ProtonByteArrayBufferReverseIterator(array, offset(offset), length);
    }

    //----- Buffer component API

    @Override
    public boolean hasReadbleArray() {
        return isReadable() ? true : false;
    }

    @Override
    public ProtonByteArrayBuffer advanceReadOffset(int amount) {
        return (ProtonByteArrayBuffer) ProtonBuffer.super.advanceReadOffset(amount);
    }

    @Override
    public byte[] getReadableArray() {
        return array;
    }

    @Override
    public int getReadableArrayOffset() {
        return arrayOffset;
    }

    @Override
    public int getReadableArrayLength() {
        return writeOffset - readOffset;
    }

    @Override
    public ByteBuffer getReadableBuffer() {
        return ByteBuffer.wrap(array, arrayOffset + readOffset, readCapacity - readOffset).asReadOnlyBuffer();
    }

    @Override
    public ProtonByteArrayBuffer advanceWriteOffset(int amount) {
        return (ProtonByteArrayBuffer) ProtonBuffer.super.advanceWriteOffset(amount);
    }

    @Override
    public boolean hasWritableArray() {
        return writeCapacity > 0;
    }

    @Override
    public byte[] getWritableArray() {
        return array;
    }

    @Override
    public int getWritableArrayOffset() {
        return arrayOffset + writeOffset;
    }

    @Override
    public int getWritableArrayLength() {
        return getWritableBytes();
    }

    @Override
    public ByteBuffer getWritableBuffer() {
        if (writeCapacity < 0) {
            return ByteBuffer.allocate(0);
        } else {
            return ByteBuffer.wrap(array, arrayOffset + writeOffset, getWritableBytes());
        }
    }

    @Override
    public long getNativeAddress() {
        return 0;
    }

    @Override
    public long getNativeReadAddress() {
        return 0;
    }

    @Override
    public long getNativeWriteAddress() {
        return 0;
    }

    //----- Buffer search API

    @Override
    public int indexOf(byte needle, int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);

        checkIndexOfBounds(offset, length);

        final int end = offset + length;

        if (length > 7) {
            final long pattern = (needle & 0xFFL) * 0x101010101010101L;
            final int stopAfter = offset + (length >>> 3) * Long.BYTES;

            for (; offset < stopAfter; offset += Long.BYTES) {
                final long word = ProtonBufferUtils.readLong(array, offset(offset));

                // Hackers delight chapter six describes this algorithm
                long input = word ^ pattern;
                long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
                tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);

                final int index = Long.numberOfLeadingZeros(tmp) >>> 3;

                if (index < Long.BYTES) {
                    return offset + index;
                }
            }
        }

        for (; offset < end; offset++) {
            if (array[offset(offset)] == needle) {
                return offset;
            }
        }

        return -1;
    }

    //----- Sharable resource API implementation

    @Override
    protected void releaseResourceOwnership() {
        closed = true;
        readOnly = false;
        writeCapacity = CLOSED_MARKER;
        readCapacity = CLOSED_MARKER;
        readOffset = 0;
        writeOffset = 0;
        array = null;
    }

    @Override
    protected ProtonBuffer transferTheResource() {
        ProtonByteArrayBuffer transfer = new ProtonByteArrayBuffer(array, arrayOffset, readOnly);

        // Match transfer state to this buffer
        transfer.readCapacity = readCapacity;
        transfer.writeCapacity = writeCapacity;
        transfer.readOffset = readOffset;
        transfer.writeOffset = writeOffset;

        return transfer;
    }

    @Override
    protected RuntimeException resourceIsClosedException() {
        return ProtonBufferUtils.genericBufferIsClosed(this);
    }

    //----- Private ProtonBuffer APIs

    private int offset(int index) {
       return index + arrayOffset;
    }

    private void checkWrite(int index, int size, boolean allowExpansion) {
        if (index < readOffset || writeCapacity < (index + size)) {
            expandOrThrowError(index, size, allowExpansion);
        }
    }

    private void checkRead(int index, int size) {
        if (index < 0 || writeOffset < index + size || closed) {
            if (closed) {
                throw ProtonBufferUtils.genericBufferIsClosed(this);
            } else {
                throw ProtonBufferUtils.genericOutOfBounds(this, index);
            }
        }
    }

    private void checkGet(int index, int size) {
        if (index < 0 || readCapacity < index + size) {
            if (closed) {
                throw ProtonBufferUtils.genericBufferIsClosed(this);
            } else {
                throw ProtonBufferUtils.genericOutOfBounds(this, index);
            }
        }
    }

    private void checkSet(int index, int size) {
        if (index < 0 || writeCapacity < index + size) {
            expandOrThrowError(index, size, false);
        }
    }

    private void checkIndexOfBounds(int index, int size) {
        if (index < readOffset || writeOffset < index + size) {
            throw new IndexOutOfBoundsException(
                "Search range [read " + index + " length " + size  +
                "] is out of bounds: [read " + readOffset + " length " + getReadableBytes() + "].");
        }
    }

    private void expandOrThrowError(int index, int size, boolean mayExpand) {
        if (readCapacity == CLOSED_MARKER) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        if (readOnly) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }

        int capacity = capacity();
        if (mayExpand && index >= 0 && index <= capacity && writeOffset + size <= implicitGrowthLimit) {
            int minimumGrowth = Math.min(Math.max(capacity * 2, size), implicitGrowthLimit) - capacity;
            ensureWritable(size, minimumGrowth, false);
            checkSet(index, size); // Verify writing is now possible, without recursing.
            return;
        }

        throw ProtonBufferUtils.genericOutOfBounds(this, index);
    }

    private void checkCopyIntoArgs(int srcPos, int length, int destPos, int destLength) {
        if (readCapacity == CLOSED_MARKER) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException("The srcPos cannot be negative: " + srcPos + '.');
        }
        if (length < 0) {
            throw new IndexOutOfBoundsException("The length value cannot be negative " + length + ".");
        }
        if (readCapacity < srcPos + length) {
            throw new IndexOutOfBoundsException("The srcPos + length is beyond the end of the buffer: " +
                    "srcPos = " + srcPos + ", length = " + length + '.');
        }
        if (destPos < 0) {
            throw new IndexOutOfBoundsException("The destPos cannot be negative: " + destPos + '.');
        }
        if (destLength < destPos + length) {
            throw new IndexOutOfBoundsException("The destPos + length is beyond the end of the destination: " +
                    "destPos = " + destPos + ", length = " + length + '.');
        }
    }

    private static final class ProtonByteArrayBufferIterator implements ProtonBufferIterator {

        private final int endPos;
        private final byte[] array;

        private int current;

        public ProtonByteArrayBufferIterator(byte[] array, int offset, int length) {
            this.array = array;
            this.current = offset;
            this.endPos = offset + length; // End position is exclusive
        }

        @Override
        public boolean hasNext() {
            return current != endPos;
        }

        @Override
        public byte next() {
            if (current == endPos) {
                throw new NoSuchElementException("Buffer iteration complete, no additional bytes available");
            }

            return array[current++];
        }

        @Override
        public int remaining() {
            return endPos - current;
        }

        @Override
        public int offset() {
            return current;
        }
    }

    private static final class ProtonByteArrayBufferReverseIterator implements ProtonBufferIterator {

        private final int endPos;
        private final byte[] array;

        private int current;

        public ProtonByteArrayBufferReverseIterator(byte[] array, int offset, int length) {
            this.array = array;
            this.current = offset;
            this.endPos = offset - length; // End position is exclusive
        }

        @Override
        public boolean hasNext() {
            return current != endPos;
        }

        @Override
        public byte next() {
            if (current == endPos) {
                throw new NoSuchElementException("Buffer iteration complete, no additional bytes available");
            }

            return array[current--];
        }

        @Override
        public int remaining() {
            return Math.abs(endPos - current);
        }

        @Override
        public int offset() {
            return current;
        }
    }
}
