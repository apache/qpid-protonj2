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
import java.nio.charset.Charset;

/**
 * Buffer type abstraction used to provide users of the proton library with
 * a means of using their own type of byte buffer types in combination with the
 * library tooling.
 */
public interface ProtonBuffer extends Comparable<ProtonBuffer> {

    /**
     * Return the underlying buffer object that backs this {@link ProtonBuffer} instance, or null
     * if there is no backing object.
     *
     * This method should be overridden in buffer abstraction when access to the underlying backing
     * store is needed such as when wrapping pooled resources that need explicit release calls.
     *
     * @return an underlying buffer object or other backing store for this buffer.
     */
    default Object unwrap() { return null; }

    /**
     * @return true if this buffer has a backing byte array that can be accessed.
     */
    boolean hasArray();

    /**
     * Returns the backing array for this ProtonBuffer instance if there is such an array or
     * throws an exception if this ProtonBuffer implementation has no backing array.
     * <p>
     * Changes to the returned array are visible to other users of this ProtonBuffer.
     *
     * @return the backing byte array for this ProtonBuffer.
     *
     * @throws UnsupportedOperationException if this buffer type has no backing array.
     */
    byte[] getArray();

    /**
     * @return the offset of the first byte in the backing array belonging to this buffer.
     *
     * @throws UnsupportedOperationException if this buffer type has no backing array.
     */
    int getArrayOffset();

    /**
     * @return the number of bytes this buffer can currently contain.
     */
    int capacity();

    /**
     * Adjusts the capacity of this buffer.  If the new capacity is less than the current
     * capacity, the content of this buffer is truncated.  If the new capacity is greater
     * than the current capacity, the buffer is appended with unspecified data whose length is
     * new capacity - current capacity.
     *
     * @param newCapacity
     *      the new maximum capacity value of this buffer.
     *
     * @return this buffer for using in call chaining.
     */
    ProtonBuffer capacity(int newCapacity);

    /**
     * Returns the number of bytes that this buffer is allowed to grow to when write
     * operations exceed the current capacity value.
     *
     * @return the number of bytes this buffer is allowed to grow to.
     */
    int maxCapacity();

    /**
     * Ensures that the requested number of bytes is available for write operations
     * in the current buffer, growing the buffer if needed to meet the requested
     * writable capacity. This method will not alter the write offset but may change
     * the value returned from the capacity method if new buffer space is allocated.
     *
     * @param amount
     *      The number of bytes beyond the current write index needed.
     *
     * @return this buffer for using in call chaining.
     *
     * @throws IllegalArgumentException if the amount given is less than zero.
     * @throws IndexOutOfBoundsException if the amount given would result in the buffer
     *         exceeding the maximum capacity for this buffer.
     */
    ProtonBuffer ensureWritable(int amount) throws IndexOutOfBoundsException, IllegalArgumentException;

    /**
     * Create a duplicate of this ProtonBuffer instance that shares the same backing
     * data store and but maintains separate position index values.  Changes to one buffer
     * are visible in any of its duplicates.  This method does not copy the read or write
     * markers to the new buffer instance.
     *
     * @return a new ProtonBuffer instance that shares the backing data as this one.
     */
    ProtonBuffer duplicate();

    /**
     * Create a new ProtonBuffer whose contents are a subsequence of the contents of this
     * {@link ProtonBuffer}.
     * <p>
     * The starting point of the new buffer starts at this buffer's current position, the
     * marks and limits of the new buffer will be independent of this buffer however changes
     * to the data backing the buffer will be visible in this buffer.
     *
     * @return a new {@link ProtonBuffer} whose contents are a subsequence of this buffer.
     */
    ProtonBuffer slice();

    /**
     * Create a new ProtonBuffer whose contents are a subsequence of the contents of this
     * {@link ProtonBuffer}.
     * <p>
     * The starting point of the new buffer starts at given index into this buffer and spans
     * the number of bytes given by the length.  Changes to the contents of this buffer or to
     * the produced slice buffer are visible in the other.
     *
     * @param index
     *      The index in this buffer where the slice should begin.
     * @param length
     *      The number of bytes to make visible to the new buffer from this one.
     *
     * @return a new {@link ProtonBuffer} whose contents are a subsequence of this buffer.
     */
    ProtonBuffer slice(int index, int length);

    /**
     * Create a deep copy of the readable bytes of this ProtonBuffer, the returned buffer can
     * be modified without affecting the contents or position markers of this instance.
     *
     * @return a deep copy of this ProtonBuffer instance.
     */
    ProtonBuffer copy();

    /**
     * Returns a copy of this buffer's sub-region.  Modifying the content of
     * the returned buffer or this buffer does not affect each other at all.
     * This method does not modify the value returned from {@link #getReadIndex()}
     * or {@link #getWriteIndex()} of this buffer.
     *
     * @param index
     *      The index in this buffer where the copy should begin
     * @param length
     *      The number of bytes to copy to the new buffer from this one.
     *
     * @return a new ProtonBuffer instance containing the copied bytes.
     */
    ProtonBuffer copy(int index, int length);

    /**
     * Reset the read and write offsets to zero and clears the position markers if
     * set previously, this method is not required to reset the data previously
     * written to this buffer.
     *
     * @return this buffer for using in call chaining.
     */
    ProtonBuffer clear();

    /**
     * Returns a ByteBuffer that represents the readable bytes contained in this buffer.
     * <p>
     * This method should attempt to return a ByteBuffer that shares the backing data store
     * with this buffer however if that is not possible it is permitted that the returned
     * ByteBuffer contain a copy of the readable bytes of this ProtonBuffer.
     *
     * @return a ByteBuffer that represents the readable bytes of this buffer.
     */
    ByteBuffer toByteBuffer();

    /**
     * Returns a ByteBuffer that represents the given span of bytes from the readable portion
     * of this buffer.
     * <p>
     * This method should attempt to return a ByteBuffer that shares the backing data store
     * with this buffer however if that is not possible it is permitted that the returned
     * ByteBuffer contain a copy of the readable bytes of this ProtonBuffer.
     *
     * @param index
     *      The starting index in this where the ByteBuffer view should begin.
     * @param length
     *      The number of bytes to include in the ByteBuffer view.
     *
     * @return a ByteBuffer that represents the given view of this buffers readable bytes.
     */
    ByteBuffer toByteBuffer(int index, int length);

    /**
     * Returns a String created from the buffer's underlying bytes using the specified
     * {@link java.nio.charset.Charset} for the newly created String.
     *
     * @param charset
     *      the {@link java.nio.charset.Charset} to use to construct the new string.
     *
     * @return a string created from the buffer's underlying bytes using the given {@link java.nio.charset.Charset}.
     */
    String toString(Charset charset);

    /**
     * @return the number of bytes available for reading from this buffer.
     */
    int getReadableBytes();

    /**
     * @return the number of bytes that can be written to this buffer before the limit is hit.
     */
    int getWritableBytes();

    /**
     * Gets the current maximum number of bytes that can be written to this buffer.  This is
     * the same value that can be computed by subtracting the current write index from the
     * maximum buffer capacity.
     *
     * @return the maximum number of bytes that can be written to this buffer before the limit is hit.
     */
    int getMaxWritableBytes();

    /**
     * @return the current value of the read index for this buffer.
     */
    int getReadIndex();

    /**
     * Sets the read index for this buffer.
     *
     * @param value The index into the buffer where the read index should be positioned.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the value given is greater than the write index or negative.
     */
    ProtonBuffer setReadIndex(int value);

    /**
     * @return the current value of the write index for this buffer.
     */
    int getWriteIndex();

    /**
     * Sets the write index for this buffer.
     *
     * @param value The index into the buffer where the write index should be positioned.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the value less than the read index or greater than the capacity.
     */
    ProtonBuffer setWriteIndex(int value);

    /**
     * Used to set the read index and the write index in one call.  This methods allows for an update
     * to the read index and write index to values that could not be set using simple setReadIndex and
     * setWriteIndex call where the values would violate the constraints placed on them by the value
     * of the other index.
     *
     * @param readIndex
     *      The new read index to assign to this buffer.
     * @param writeIndex
     *      The new write index to assign to this buffer.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the values violate the basic tenants of readIndex and writeIndex
     */
    ProtonBuffer setIndex(int readIndex, int writeIndex);

    /**
     * Marks the current read index so that it can later be restored by a call to
     * {@link ProtonBuffer#resetReadIndex}, the initial mark value is 0.
     *
     * @return this buffer for use in chaining.
     */
    ProtonBuffer markReadIndex();

    /**
     * Resets the current read index to the previously marked value.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the current write index is less than the marked read index.
     */
    ProtonBuffer resetReadIndex();

    /**
     * Marks the current write index so that it can later be restored by a call to
     * {@link ProtonBuffer#resetWriteIndex}, the initial mark value is 0.
     *
     * @return this buffer for use in chaining.
     */
    ProtonBuffer markWriteIndex();

    /**
     * Resets the current write index to the previously marked value.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the current read index is greater than the marked write index.
     */
    ProtonBuffer resetWriteIndex();

    /**
     * @return true if the read index is less than the write index.
     */
    boolean isReadable();

    /**
     * Check if the given number of bytes can be read from the buffer.
     *
     * @param size
     *      the size that is desired in readable bytes
     *
     * @return true if the buffer has at least the given number of readable bytes remaining.
     */
    boolean isReadable(int size);

    /**
     * Compares the remaining content of the current buffer with the remaining content of the
     * given buffer, which must not be null. Each byte is compared in turn as an unsigned value,
     * returning upon the first difference. If no difference is found before the end of one
     * buffer, the shorter buffer is considered less than the other, or else if the same length
     * then they are considered equal.
     *
     * @return  a negative, zero, or positive integer when this buffer is less than, equal to,
     *          or greater than the given buffer.
     * @see Comparable#compareTo(Object)
     */
    @Override int compareTo(ProtonBuffer buffer);

    /**
     * Gets a boolean from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    boolean getBoolean(int index);

    /**
     * Gets a byte from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    byte getByte(int index);

    /**
     * Gets a unsigned byte from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    short getUnsignedByte(int index);

    /**
     * Gets a 2-byte char from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    char getChar(int index);

    /**
     * Gets a short from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    short getShort(int index);

    /**
     * Gets a unsigned short from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    int getUnsignedShort(int index);

    /**
     * Gets a int from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    int getInt(int index);

    /**
     * Gets a unsigned int from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    long getUnsignedInt(int index);

    /**
     * Gets a long from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    long getLong(int index);

    /**
     * Gets a float from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    float getFloat(int index);

    /**
     * Gets a double from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    double getDouble(int index);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index} until the destination becomes
     * non-writable.  This method is basically same with
     * {@link #getBytes(int, ProtonBuffer, int, int)}, except that this
     * method increases the {@code writeIndex} of the destination by the
     * number of the transferred bytes while
     * {@link #getBytes(int, ProtonBuffer, int, int)} does not.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * the source buffer (i.e. {@code this}).
     *
     * @param index
     *      The index into the buffer where the value should be read.
     * @param destination
     *      the destination buffer for the bytes to be read
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0} or
     *         if {@code index + dst.writableBytes} is greater than
     *            {@code this.capacity}
     */
    ProtonBuffer getBytes(int index, ProtonBuffer destination);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.  This method is basically same
     * with {@link #getBytes(int, ProtonBuffer, int, int)}, except that this
     * method increases the {@code writeIndex} of the destination by the
     * number of the transferred bytes while
     * {@link #getBytes(int, ProtonBuffer, int, int)} does not.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * the source buffer (i.e. {@code this}).
     *
     * @param index
     *      the index in the buffer to start the read from
     * @param destination
     *      the destination buffer for the bytes to be read
     * @param length
     *      the number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0},
     *         if {@code index + length} is greater than
     *            {@code this.capacity}, or
     *         if {@code length} is greater than {@code dst.writableBytes}
     */
    ProtonBuffer getBytes(int index, ProtonBuffer destination, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readIndex} or {@code writeIndex}
     * of both the source (i.e. {@code this}) and the destination.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     * @param destination
     *      The buffer where the bytes read will be written to
     * @param offset
     *      The offset into the destination where the write starts
     * @param length
     *      The number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0},
     *         if the specified {@code dstIndex} is less than {@code 0},
     *         if {@code index + length} is greater than
     *            {@code this.capacity}, or
     *         if {@code dstIndex + length} is greater than
     *            {@code dst.capacity}
     */
    ProtonBuffer getBytes(int index, ProtonBuffer destination, int offset, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * this buffer
     *
     * @param index
     *      The index into the buffer where the value should be read.
     * @param destination
     *      The buffer where the bytes read will be written to
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0} or
     *         if {@code index + dst.length} is greater than
     *            {@code this.capacity}
     */
    ProtonBuffer getBytes(int index, byte[] destination);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code #getReadIndex()} or {@code #getWriteIndex()}
     * of this buffer.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     * @param destination
     *      The buffer where the bytes read will be written to
     * @param offset
     *      the offset into the destination to begin writing the bytes.
     * @param length
     *      the number of bytes to transfer from this buffer to the target buffer.
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0},
     *         if the specified {@code offset} is less than {@code 0},
     *         if {@code index + length} is greater than
     *            {@code this.capacity}, or
     *         if {@code offset + length} is greater than
     *            {@code target.length}
     */
    ProtonBuffer getBytes(int index, byte[] destination, int offset, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index} until the destination's position
     * reaches its limit.
     * This method does not modify {@code #getReadIndex()} or {@code #getWriteIndex()} of
     * this buffer while the destination's {@code position} will be increased.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     * @param destination
     *      The buffer where the bytes read will be written to
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0} or
     *         if {@code index + destination.remaining()} is greater than
     *            {@code #capacity()}
     */
    ProtonBuffer getBytes(int index, ByteBuffer destination);

    /**
     * Sets the byte value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setByte(int index, int value);

    /**
     * Sets the boolean value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setBoolean(int index, boolean value);

    /**
     * Sets the char value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setChar(int index, int value);

    /**
     * Sets the short value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setShort(int index, int value);

    /**
     * Sets the int value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setInt(int index, int value);

    /**
     * Sets the long value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setLong(int index, long value);

    /**
     * Sets the float value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setFloat(int index, float value);

    /**
     * Sets the double value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setDouble(int index, double value);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index} until the source buffer becomes
     * unreadable.  This method is basically same with
     * {@link #setBytes(int, ProtonBuffer, int, int)}, except that this
     * method increases the {@code readIndex} of the source buffer by
     * the number of the transferred bytes while
     * {@link #setBytes(int, ProtonBuffer, int, int)} does not.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * the source buffer (i.e. {@code this}).
     *
     * @param index
     *      The index in this buffer where the write operation starts.
     * @param source
     *      The source buffer from which the bytes are read.
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0} or
     *         if {@code index + source.readableBytes} is greater than
     *            {@code this.capacity}
     */
    ProtonBuffer setBytes(int index, ProtonBuffer source);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index}.  This method is basically same
     * with {@link #setBytes(int, ProtonBuffer, int, int)}, except that this
     * method increases the {@code readIndex} of the source buffer by
     * the number of the transferred bytes while
     * {@link #setBytes(int, ProtonBuffer, int, int)} does not.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * the source buffer (i.e. {@code this}).
     *
     * @param index
     *      The index in this buffer where the write operation starts.
     * @param source
     *      The source buffer from which the bytes are read.
     * @param length
     *      The number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0},
     *         if {@code index + length} is greater than
     *            {@code this.capacity}, or
     *         if {@code length} is greater than {@code source.readableBytes}
     */
    ProtonBuffer setBytes(int index, ProtonBuffer source, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readIndex} or {@code writeIndex}
     * of both the source (i.e. {@code this}) and the destination.
     *
     * @param index
     *      The index in this buffer where the write operation starts.
     * @param source
     *      The source buffer from which the bytes are read.
     * @param offset
     *      The offset into the source where the set begins.
     * @param length
     *      The number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0},
     *         if the specified {@code sourceIndex} is less than {@code 0},
     *         if {@code index + length} is greater than
     *            {@code this.capacity}, or
     *         if {@code sourceIndex + length} is greater than
     *            {@code source.capacity}
     */
    ProtonBuffer setBytes(int index, ProtonBuffer source, int offset, int length);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * this buffer.
     *
     * @param index
     *      The index in this buffer where the write operation starts.
     * @param source
     *      The source buffer from which the bytes are read.
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0} or
     *         if {@code index + source.length} is greater than
     *            {@code this.capacity}
     */
    ProtonBuffer setBytes(int index, byte[] source);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * this buffer.
     *
     * @param index
     *      The index in this buffer where the write operation starts.
     * @param source
     *      The source buffer from which the bytes are read.
     * @param offset
     *      The offset into the source where the set begins.
     * @param length
     *      The number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0},
     *         if the specified {@code offset} is less than {@code 0},
     *         if {@code index + length} is greater than
     *            {@code this.capacity}, or
     *         if {@code offset + length} is greater than {@code source.length}
     */
    ProtonBuffer setBytes(int index, byte[] source, int offset, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index} until the source buffer's position
     * reaches its limit.
     * This method does not modify {@code readIndex} or {@code writeIndex} of
     * this buffer.
     *
     * @param index
     *      The index in this buffer where the write operation starts.
     * @param source
     *      The source buffer from which the bytes are read.
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code index} is less than {@code 0} or
     *         if {@code index + source.remaining()} is greater than
     *            {@code this.capacity}
     */
    ProtonBuffer setBytes(int index, ByteBuffer source);

    /**
     * Increases the current {@code readIndex} of this buffer by the specified {@code length}.
     *
     * @param length
     *      the number of bytes in this buffer to skip.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException
     *         if {@code length} is greater than {@code this.readableBytes}
     */
    ProtonBuffer skipBytes(int length);

    /**
     * Reads one byte from the buffer and advances the read index by one.
     *
     * @return a single byte from the ProtonBuffer.
     *
     * @throws IndexOutOfBoundsException if there is no readable bytes left in the buffer.
     */
    byte readByte();

    /**
     * Reads bytes from this buffer and writes them into the destination byte array incrementing
     * the read index by the value of the length of the destination array.
     *
     * @param target
     *      The byte array to write into.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the target array is larger than the readable bytes.
     */
    ProtonBuffer readBytes(byte[] target);

    /**
     * Reads bytes from this buffer and writes them into the destination byte array incrementing
     * the read index by the length value passed.
     *
     * @param target
     *      The byte array to write into.
     * @param length
     *      The number of bytes to read into the given array.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the length is larger than the readable bytes, or length is
     *         greater than the length of the target array, or length is negative.
     */
    ProtonBuffer readBytes(byte[] target, int length);

    /**
     * Reads bytes from this buffer and writes them into the destination byte array incrementing
     * the read index by the length value passed, the bytes are read into the given buffer starting
     * from the given offset value.
     *
     * @param target
     *      The byte array to write into.
     * @param offset
     *      The offset into the given array where bytes are written.
     * @param length
     *      The number of bytes to read into the given array.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the offset is negative, or if the length is greater than
     *         the current readable bytes or if the offset + length is great than the size of the target.
     */
    ProtonBuffer readBytes(byte[] target, int offset, int length);

    /**
     * Reads bytes from this buffer and writes them into the destination ProtonBuffer incrementing
     * the read index by the value of the number of bytes written to the target.  The number of bytes
     * written will be the equal to the writable bytes of the target buffer.  The write index of the
     * target buffer will be incremented by the number of bytes written into it.
     *
     * @param target
     *      The ProtonBuffer to write into.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IllegalArgumentException if the target buffer is this buffer.
     * @throws IndexOutOfBoundsException if the target buffer has more writable bytes than this buffer
     *         has readable bytes.
     */
    ProtonBuffer readBytes(ProtonBuffer target);

    /**
     * Reads bytes from this buffer and writes them into the destination ProtonBuffer incrementing
     * the read index by the number of bytes written.  The write index of the target buffer will be
     * incremented by the number of bytes written into it.
     *
     * @param target
     *      The ProtonBuffer to write into.
     * @param length
     *      The number of bytes to read into the given buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the length value is greater than the readable bytes of
     *         this buffer or is greater than the writable bytes of the target buffer..
     */
    ProtonBuffer readBytes(ProtonBuffer target, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readIndex} and increases the {@code readIndex}
     * by the number of the transferred bytes (= {@code length}).  This method
     * does not modify the write index of the target buffer.
     *
     * @param target
     *      The ProtonBuffer to write into.
     * @param offset
     *      The offset into the given buffer where bytes are written.
     * @param length
     *      The number of bytes to read into the given buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the offset is negative, or if the length is greater than
     *         the current readable bytes or if the offset + length is great than the size of the target.
     */
    ProtonBuffer readBytes(ProtonBuffer target, int offset, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readIndex} until the destination's position
     * reaches its limit, and increases the {@code readIndex} by the
     * number of the transferred bytes.
     *
     * @param destination
     *      The target ByteBuffer to write into.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the destination does not have enough capacity.
     */
    ProtonBuffer readBytes(ByteBuffer destination);

    /**
     * Reads a boolean value from the buffer and advances the read index by one.
     *
     * @return boolean value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    boolean readBoolean();

    /**
     * Reads a short value from the buffer and advances the read index by two.
     *
     * @return short value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    short readShort();

    /**
     * Reads a integer value from the buffer and advances the read index by four.
     *
     * @return integer value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    int readInt();

    /**
     * Reads a long value from the buffer and advances the read index by eight.
     *
     * @return long value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    long readLong();

    /**
     * Reads a float value from the buffer and advances the read index by four.
     *
     * @return float value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    float readFloat();

    /**
     * Reads a double value from the buffer and advances the read index by eight.
     *
     * @return double value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    double readDouble();

    /**
     * @return true if the buffer has bytes remaining between the write index and the capacity.
     */
    boolean isWritable();

    /**
     * Check if the requested number of bytes can be written into this buffer.
     *
     * @param size
     *      The number writable bytes that is being checked in this buffer.
     *
     * @return true if the buffer has space left for the given number of bytes to be written.
     */
    boolean isWritable(int size);

    /**
     * Writes a single byte to the buffer and advances the write index by one.
     *
     * @param value
     *      The byte to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeByte(int value);

    /**
     * Writes the contents of the given byte array into the buffer and advances the write index by the
     * length of the given array.
     *
     * @param value
     *      The byte array to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeBytes(byte[] value);

    /**
     * Writes the contents of the given byte array into the buffer and advances the write index by the
     * length value given.
     *
     * @param value
     *      The byte array to write into the buffer.
     * @param length
     *      The number of bytes to write from the given array into this buffer
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeBytes(byte[] value, int length);

    /**
     * Writes the contents of the given byte array into the buffer and advances the write index by the
     * length value given.  The bytes written into this buffer are read starting at the given offset
     * into the passed in byte array.
     *
     * @param value
     *      The byte array to write into the buffer.
     * @param offset
     *      The offset into the given array to start reading from.
     * @param length
     *      The number of bytes to write from the given array into this buffer
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeBytes(byte[] value, int offset, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writeIndex} until the source buffer becomes
     * unreadable, and increases the {@code writeIndex} by the number of
     * the transferred bytes.  This method is basically same with
     * {@link #writeBytes(ProtonBuffer, int, int)}, except that this method
     * increases the {@code readIndex} of the source buffer by the number of
     * the transferred bytes while {@link #writeBytes(ProtonBuffer, int, int)}
     * does not.
     *
     * @param source
     *      The source buffer from which the bytes are read.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException
     *         if {@code source.readableBytes} is greater than
     *            {@code this.writableBytes}
     */
    ProtonBuffer writeBytes(ProtonBuffer source);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writeIndex} and increases the {@code writeIndex}
     * by the number of the transferred bytes (= {@code length}).  This method
     * is basically same with {@link #writeBytes(ProtonBuffer, int, int)},
     * except that this method increases the {@code readIndex} of the source
     * buffer by the number of the transferred bytes (= {@code length}) while
     * {@link #writeBytes(ProtonBuffer, int, int)} does not.
     *
     * @param source
     *      The source buffer from which the bytes are read.
     * @param length
     *      The number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if {@code length} is greater than {@code this.writableBytes} or
     *         if {@code length} is greater then {@code source.readableBytes}
     */
    ProtonBuffer writeBytes(ProtonBuffer source, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writeIndex} and increases the {@code writeIndex}
     * by the number of the transferred bytes (= {@code length}).  This method
     * does not modify the read index of the source buffer.
     *
     * @param source
     *      The source buffer from which the bytes are read.
     * @param offset
     *      The offset in the source buffer to start writing into this buffer.
     * @param length
     *      The number of bytes to transfer
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if the specified {@code offset} is less than {@code 0},
     *         if {@code offset + length} is greater than
     *            {@code source.capacity}, or
     *         if {@code length} is greater than {@code this.writableBytes}
     */
    ProtonBuffer writeBytes(ProtonBuffer source, int offset, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writeIndex} until the source buffer's position
     * reaches its limit, and increases the {@code writeIndex} by the
     * number of the transferred bytes.
     *
     * @param source
     *      The source buffer from which the bytes are read.
     *
     * @return this buffer for chaining
     *
     * @throws IndexOutOfBoundsException
     *         if {@code source.remaining()} is greater than
     *            {@code this.writableBytes}
     */
    ProtonBuffer writeBytes(ByteBuffer source);

    /**
     * Writes a single boolean to the buffer and advances the write index by one.
     *
     * @param value
     *      The boolean to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeBoolean(boolean value);

    /**
     * Writes a single short to the buffer and advances the write index by two.
     *
     * @param value
     *      The short to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeShort(short value);

    /**
     * Writes a single integer to the buffer and advances the write index by four.
     *
     * @param value
     *      The integer to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeInt(int value);

    /**
     * Writes a single long to the buffer and advances the write index by eight.
     *
     * @param value
     *      The long to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeLong(long value);

    /**
     * Writes a single float to the buffer and advances the write index by four.
     *
     * @param value
     *      The float to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeFloat(float value);

    /**
     * Writes a single double to the buffer and advances the write index by eight.
     *
     * @param value
     *      The double to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeDouble(double value);

}
