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

import static org.apache.qpid.protonj2.buffer.ProtonBufferUtils.checkIsNotNegative;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

import org.apache.qpid.protonj2.resource.Resource;

/**
 * Buffer type abstraction used to provide users of the proton library with
 * a means of using their own type of byte buffer types in combination with the
 * library tooling.
 */
public interface ProtonBuffer extends ProtonBufferAccessors, Resource<ProtonBuffer>, Comparable<ProtonBuffer> {

    /**
     * Close the buffer and clean up an resources that are held. The buffer close
     * must not throw an exception.
     */
    @Override
    void close();

    /**
     * @return true if the buffer is backed by native memory.
     */
    boolean isDirect();

    /**
     * Return the underlying buffer object that backs this {@link ProtonBuffer} instance, or self
     * if there is no backing object.
     *
     * This method should be overridden in buffer abstraction when access to the underlying backing
     * store is needed such as when wrapping pooled resources that need explicit release calls.
     *
     * @return an underlying buffer object or other backing store for this buffer.
     */
    default Object unwrap() { return this; }

    /**
     * Converts this buffer instance to a read-only buffer, any write operation that is
     * performed on this buffer following this call will fail.  A buffer cannot be made
     * writable after this call.
     *
     * @return this buffer for use in chaining.
     */
    ProtonBuffer convertToReadOnly();

    /**
     * @return whether this buffer instance is read-only or not.
     */
    boolean isReadOnly();

    /**
     * @return if this buffer has been closed and can no longer be accessed for reads or writes.
     */
    @Override
    boolean isClosed();

    /**
     * @return the number of bytes available for reading from this buffer.
     */
    default int getReadableBytes() {
        return getWriteOffset() - getReadOffset();
    }

    /**
     * @return the current value of the read offset for this buffer.
     */
    int getReadOffset();

    /**
     * Sets the read offset for this buffer.
     *
     * @param value The offset into the buffer where the read offset should be positioned.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the value given is greater than the write offset or negative.
     */
    ProtonBuffer setReadOffset(int value);

    /**
     * Adjusts the current {@link #getReadOffset()} of this buffer by the specified {@code length}.
     *
     * @param length
     *      the number of bytes to advance the read offset by.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IllegalArgumentException if the {#code length} given is negative.
     * @throws IndexOutOfBoundsException
     *         if {@code length} is greater than {@code this.readableBytes}
     */
    default ProtonBuffer advanceReadOffset(int length) {
        checkIsNotNegative(length, "Read offset advance requires positive values");
        setReadOffset(getReadOffset() + length);
        return this;
    }

    /**
     * @return true if the read index is less than the write index.
     */
    default boolean isReadable() {
        return getReadOffset() < getWriteOffset();
    }

    /**
     * @return the number of bytes that can be written to this buffer before the limit is hit.
     */
    default int getWritableBytes() {
        return capacity() - getWriteOffset();
    }

    /**
     * @return the current value of the write offset for this buffer.
     */
    int getWriteOffset();

    /**
     * Sets the write offset for this buffer.
     *
     * @param value The offset into the buffer where the write offset should be positioned.
     *
     * @return this buffer for use in chaining.
     *
     * @throws IndexOutOfBoundsException if the value less than the read offset or greater than the capacity.
     */
    ProtonBuffer setWriteOffset(int value);

    /**
     * Adjusts the current {@link #getWriteOffset()} of this buffer by the specified {@code length}.
     *
     * @param length
     *      the number of bytes to advance the write offset by.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IllegalArgumentException if the {#code length} given is negative.
     * @throws IndexOutOfBoundsException
     *         if {@code length} is greater than the buffer {@link #capacity()}
     */
    default ProtonBuffer advanceWriteOffset(int length) {
        checkIsNotNegative(length, "Write offset advance requires positive values");
        setWriteOffset(getWriteOffset() + length);
        return this;
    }

    /**
     * @return true if the buffer has bytes remaining between the write offset and the capacity.
     */
    default boolean isWritable() {
        return getWriteOffset() < capacity();
    }

    /**
     * Assigns the given value to every byte in the buffer without respect for the
     * buffer read or write offsets.
     *
     * @param value
     * 		The byte value to assign each byte in this buffer.
     *
     * @return this ProtonBuffer for chaining.
     */
    ProtonBuffer fill(byte value);

    /**
     * @return the number of bytes this buffer can currently contain.
     */
    int capacity();

    /**
     * Returns the limit assigned to this buffer if one was set which controls how
     * large the capacity of the buffer can grow implicitly via write calls. Once
     * the limit is hit any write call that requires more capacity than is currently
     * available will throw an exception instead of allocating more space.
     * <p>
     * When a capacity limit is hit the buffer can still be enlarged but must be
     * done explicitly via the ensure writable APIs.
     *
     * @return the number of bytes this buffer can currently grow to..
     */
    int implicitGrowthLimit();

    /**
     * Configures the limit assigned to this buffer if one was set which controls how
     * large the capacity of the buffer can grow implicitly via write calls. Once
     * the limit is hit any write call that requires more capacity than is currently
     * available will throw an exception instead of allocating more space.
     * <p>
     * When a capacity limit is hit the buffer can still be enlarged but must be
     * done explicitly via the ensure writable APIs.
     * <p>
     * The growth limit set applies only to this buffer instance and is not carried
     * over to a copied buffer of the split buffer created from any of the buffer
     * split calls.
     *
     * @param limit
     * 		The limit to assign as the maximum capacity this buffer can grow
     *
     * @return this buffer for using in call chaining.
     */
    ProtonBuffer implicitGrowthLimit(int limit);

    /**
     * Ensures that the requested number of bytes is available for write operations
     * in the current buffer, growing the buffer if needed to meet the requested
     * writable capacity. This method will not alter the write offset but may change
     * the value returned from the capacity method if new buffer space is allocated.
     * <p>
     * This method allows buffer compaction as a strategy to reclaim already read
     * space to make room for additional writes. This implies that a composite buffer
     * can reuse already read buffers to extend the buffer's writable space by moving
     * them to the end of the set of composite buffers and reseting their index values
     * to make them fully writable.
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
    default ProtonBuffer ensureWritable(int amount) throws IndexOutOfBoundsException, IllegalArgumentException {
        return ensureWritable(amount, capacity(), true);
    }

    /**
     * Ensures that the requested number of bytes is available for write operations
     * in the current buffer, growing the buffer if needed to meet the requested
     * writable capacity. This method will not alter the write offset but may change
     * the value returned from the capacity method if new buffer space is allocated.
     * If the buffer cannot create the required number of byte via compaction then
     * the buffer will be grown by either the requested number of bytes or by the
     * minimum allowed value specified.
     * <p>
     * This method allows buffer compaction as a strategy to reclaim already read
     * space to make room for additional writes. This implies that a composite buffer
     * can reuse already read buffers to extend the buffer's writable space by moving
     * them to the end of the set of composite buffers and reseting their index values
     * to make them fully writable.
     *
     * @param amount
     *      The number of bytes beyond the current write index needed.
     * @param minimumGrowth
     * 		The minimum number of byte that the buffer can grow by
     * @param allowCompaction
     * 		Can the buffer use compaction as a strategy to create more writable space.
     *
     * @return this buffer for using in call chaining.
     *
     * @throws IllegalArgumentException if the amount given is less than zero.
     * @throws IndexOutOfBoundsException if the amount given would result in the buffer
     *         exceeding the maximum capacity for this buffer.
     */
    ProtonBuffer ensureWritable(int amount, int minimumGrowth, boolean allowCompaction) throws IndexOutOfBoundsException, IllegalArgumentException;

    /**
     * Create a deep copy of the readable bytes of this ProtonBuffer, the returned buffer can
     * be modified without affecting the contents or position markers of this instance. The
     * returned copy will not be read-only regardless of the read-only state of this buffer
     * instance at the time of copy.
     *
     * @return a deep copy of this ProtonBuffer instance.
     */
    default ProtonBuffer copy() {
        return copy(getReadOffset(), getReadableBytes());
    }

    /**
     * Returns a copy of this buffer's sub-region.  Modifying the content of
     * the returned buffer or this buffer does not affect each other at all.
     * This method does not modify the value returned from {@link #getReadOffset()}
     * or {@link #getWriteOffset()} of this buffer.
     * <p>
     * The returned buffer will not be read-only even if this buffer is and
     * as such the contents will be a deep copy regardless of this buffer's
     * read-only state.
     *
     * @param index
     *      The index in this buffer where the copy should begin
     * @param length
     *      The number of bytes to copy to the new buffer from this one.
     *
     * @return a new ProtonBuffer instance containing the copied bytes.
     */
    default ProtonBuffer copy(int index, int length) {
        return copy(index, length, false);
    }

    /**
     * Returns a copy of this buffer's readable bytes and sets the read-only
     * state of the returned buffer based on the value of the read-only flag.
     * If this buffer is read-only and the flag indicates a read-only copy
     * then the copy may be a shallow copy that references the readable
     * bytes of the source buffer.
     *
     * @param readOnly
     *     Should the returned buffer be read-only or not.
     *
     * @return a new ProtonBuffer instance containing the copied bytes.
     */
    default ProtonBuffer copy(boolean readOnly) {
        return copy(getReadOffset(), getReadableBytes(), readOnly);
    }

    /**
     * Returns a copy of this buffer's sub-region.  Modifying the content of
     * the returned buffer or this buffer does not affect each other at all.
     * This method does not modify the value returned from {@link #getReadOffset()}
     * or {@link #getWriteOffset()} of this buffer.
     * <p>
     * If this buffer is read-only and the requested copy is also read-only the
     * copy may be shallow and allow each buffer to share the same memory.
     *
     * @param index
     *      The index in this buffer where the copy should begin
     * @param length
     *      The number of bytes to copy to the new buffer from this one.
     * @param readOnly
     *     Should the returned buffer be read-only or not.
     *
     * @return a new ProtonBuffer instance containing the copied bytes.
     *
     * @throws IllegalArgumentException if the offset or length given are out of bounds.
     */
    ProtonBuffer copy(int index, int length, boolean readOnly) throws IllegalArgumentException;

    /**
     * Reset the read and write offsets to zero.
     * <p>
     * This method is not required to reset the data previously written to this buffer.
     *
     * @return this buffer for using in call chaining.
     */
    default ProtonBuffer clear() {
        setReadOffset(0);
        if (!isReadOnly()) {
            setWriteOffset(0);
        }
        return this;
    }

    /**
     * Moves the readable portion of the buffer to the beginning of the underlying
     * buffer storage and possibly makes additional bytes available for writes before
     * a buffer expansion would occur via an {@link #ensureWritable(int)} call.
     *
     * @return this buffer for using in call chaining.
     */
    ProtonBuffer compact();

    /**
     * Splits this buffer at the read offset + the length given.
     *
     * @param length
     * 		The number of bytes beyond the read offset where the split should occur
     *
     * @return A new buffer that owns the memory spanning the range given.
     */
    default ProtonBuffer readSplit(int length) {
        return split(getReadOffset() + length);
    }

    /**
     * Splits this buffer at the write offset + the length given.
     *
     * @param length
     * 		The number of bytes beyond the write offset where the split should occur
     *
     * @return A new buffer that owns the memory spanning the range given.
     */
    default ProtonBuffer writeSplit(int length) {
        return split(getWriteOffset() + length);
    }

    /**
     * Splits this buffer at the write offset.
     * <p>
     * This creates two independent buffers that can manage differing views of the same
     * memory region or in the case of a composite buffer two buffers that take ownership
     * of differing sections of the composite buffer range. For a composite buffer a single
     * buffer might be split if the offset lays within its bounds but all others buffers
     * are divided amongst the two split buffers.
     * <p>
     * If this buffer is a read-only buffer then the resulting split buffer will also be
     * read-only.
     *
     * @return A new buffer that owns the memory spanning the range given.
     */
    default ProtonBuffer split() {
        return split(getWriteOffset());
    }

    /**
     * Splits this buffer at the given offset.
     * <p>
     * This creates two independent buffers that can manage differing views of the same
     * memory region or in the case of a composite buffer two buffers that take ownership
     * of differing sections of the composite buffer range. For a composite buffer a single
     * buffer might be split if the offset lays within its bounds but all others buffers
     * are divided amongst the two split buffers.
     * <p>
     * If this buffer is a read-only buffer then the resulting split buffer will also be
     * read-only.
     *
     * @param splitOffset
     * 		The offset in this buffer where the split should occur.
     *
     * @return A new buffer that owns the memory spanning the range given.
     */
    ProtonBuffer split(int splitOffset);

    /**
     * Returns a String created from the buffer's underlying bytes using the specified
     * {@link java.nio.charset.Charset} for the newly created String.
     *
     * @param charset
     *      the {@link java.nio.charset.Charset} to use to construct the new string.
     *
     * @return a string created from the buffer's underlying bytes using the given {@link java.nio.charset.Charset}.
     */
    default String toString(Charset charset) {
        return ProtonBufferUtils.toString(this, charset);
    }

    /**
     * Compares the remaining content of the current buffer with the remaining content of the
     * given buffer, which must not be null. Each byte is compared in turn as an unsigned value,
     * returning upon the first difference. If no difference is found before the end of one
     * buffer, the shorter buffer is considered less than the other, or else if the same length
     * then they are considered equal.
     *
     * @param buffer The buffer to compare to this instance.
     *
     * @return a negative, zero, or positive integer when this buffer is less than, equal to,
     *         or greater than the given buffer.
     *
     * @see Comparable#compareTo(Object)
     */
    @Override
    default int compareTo(ProtonBuffer buffer) {
        return ProtonBufferUtils.compare(this, buffer);
    }

    /**
     * Writes into this buffer, all the bytes from the given {@code source} using the passed
     * {@code charset}. This updates the {@linkplain #getWriteOffset() write offset} of this buffer.
     *
     * @param source
     * 		The {@link CharSequence} to read the bytes from.
     * @param charset
     *  	The {@link Charset} to use for encoding the bytes that will be written.
     *
     * @return this buffer for using in call chaining.
     */
    default ProtonBuffer writeCharSequence(CharSequence source, Charset charset) {
        ProtonBufferUtils.writeCharSequence(source, this, charset);
        return this;
    }

    /**
     * Reads a {@link CharSequence} of the provided {@code length} using the given {@link Charset}.
     * This advances the {@linkplain #getReadOffset()} reader offset} of this buffer.
     *
     * @param length
     * 		The number of bytes to read to create the resulting {@link CharSequence}.
     * @param charset
     * 		The Charset of the bytes to be read and decoded into the resulting {@link CharSequence}.
     *
     * @return {@link CharSequence} read and decoded from bytes in this buffer.
     *
     * @throws IndexOutOfBoundsException if the passed {@code length} is more than the {@linkplain #getReadableBytes()} of
     * 									 this buffer.
     */
    default CharSequence readCharSequence(int length, Charset charset) {
        return ProtonBufferUtils.readCharSequence(this, length, charset);
    }

    /**
     * Copies the given number of bytes from this buffer into the specified target byte array
     * starting at the given offset into this buffer.  The copied region is written into the
     * target starting at the given offset and continues for the specified length of elements.
     *
     * @param offset
     * 		The offset into this buffer where the copy begins from.
     * @param destination
     * 		The destination byte array where the copied bytes are written.
     * @param destOffset
     * 		The offset into the destination to begin writing the copied bytes.
     * @param length
     * 		The number of bytes to copy into the destination.
     *
     * @throws NullPointerException if the destination array is null.
     * @throws IndexOutOfBoundsException if the source or destination positions, or the length, are negative,
     * 		 or if the resulting end positions reaches beyond the end of either this buffer, or the destination array.
     * @throws IllegalStateException if this buffer has already been closed.
     */
    void copyInto(int offset, byte[] destination, int destOffset, int length);

    /**
     * Copies the given number of bytes from this buffer into the specified target {@link ByteBuffer}
     * starting at the given offset into this buffer.  The copied region is written into the
     * target starting at the given offset and continues for the specified length of elements.
     *
     * @param offset
     * 		The offset into this buffer where the copy begins from.
     * @param destination
     * 		The destination {@link ByteBuffer} where the copied bytes are written.
     * @param destOffset
     * 		The offset into the destination to begin writing the copied bytes.
     * @param length
     * 		The number of bytes to copy into the destination.
     *
     * @throws NullPointerException if the destination buffer is null.
     * @throws IndexOutOfBoundsException if the source or destination positions, or the length, are negative,
     * 		 or if the resulting end positions reaches beyond the end of either this buffer, or the destination buffer.
     * @throws IllegalStateException if this buffer has already been closed.
     */
    void copyInto(int offset, ByteBuffer destination, int destOffset, int length);

    /**
     * Copies the given number of bytes from this buffer into the specified target {@link ProtonBuffer}
     * starting at the given offset into this buffer.  The copied region is written into the
     * target starting at the given offset and continues for the specified length of elements.
     *
     * @param offset
     * 		The offset into this buffer where the copy begins from.
     * @param destination
     * 		The destination {@link ProtonBuffer} where the copied bytes are written.
     * @param destOffset
     * 		The offset into the destination to begin writing the copied bytes.
     * @param length
     * 		The number of bytes to copy into the destination.
     *
     * @throws NullPointerException if the destination buffer is null.
     * @throws IndexOutOfBoundsException if the source or destination positions, or the length, are negative,
     * 		 or if the resulting end positions reaches beyond the end of either this buffer, or the destination buffer.
     * @throws IllegalStateException if this buffer has already been closed.
     */
    void copyInto(int offset, ProtonBuffer destination, int destOffset, int length);

    /**
     * Writes into this buffer, all the readable bytes from the given buffer. This updates the
     * {@link #getWriteOffset()} of this buffer, and the {@link #getReadOffset()} of the given buffer.
     *
     * @param source
     * 		The buffer to read from.
     *
     * @return This buffer.
     * @throws NullPointerException If the source buffer is {@code null}.
     */
    default ProtonBuffer writeBytes(ProtonBuffer source) {
        final int size = source.getReadableBytes();
        if (getWritableBytes() < size && getWriteOffset() + size <= implicitGrowthLimit()) {
            ensureWritable(size, 1, false);
        }
        source.copyInto(source.getReadOffset(), this, getWriteOffset(), size);
        source.advanceReadOffset(size);
        advanceWriteOffset(size);
        return this;
    }

    /**
     * Writes into this buffer, all the bytes from the given byte array. This updates the
     * {@linkplain #getWriteOffset()} of this buffer by the length of the array.
     *
     * @param source The byte array to read from.
     *
     * @return this buffer for using in call chaining.
     */
    default ProtonBuffer writeBytes(byte[] source) {
        return writeBytes(source, 0, source.length);
    }

    /**
     * Writes into this buffer, the given number of bytes from the byte array. This updates the
     * {@linkplain #getWriteOffset()} of this buffer by the length argument. Implementations are
     * recommended to specialize this method and provide a more efficient version.
     *
     * @param source The byte array to read from.
     * @param offset The position in the {@code source} from where bytes should be written to this buffer.
     * @param length The number of bytes to copy.
     *
     * @return this buffer for using in call chaining.
     */
    default ProtonBuffer writeBytes(byte[] source, int offset, int length) {
        final int writeOffset = getWriteOffset();
        if (getWritableBytes() < length && getWriteOffset() + length <= implicitGrowthLimit()) {
            ensureWritable(length, 1, false);
        }
        advanceWriteOffset(length);
        for (int i = 0; i < length; i++) {
            setByte(writeOffset + i, source[offset + i]);
        }

        return this;
    }

    /**
     * Writes into this buffer from the source {@link ByteBuffer}. This updates the
     * {@link #getWriteOffset()} of this buffer and also the position of the source
     * {@link ByteBuffer}. Implementations are recommended to specialize this method
     * and provide a more efficient version.
     * <p>
     * Note: the behavior is undefined if the given {@link ByteBuffer} is an alias for the memory in this buffer.
     *
     * @param source The {@link ByteBuffer} to read from.
     *
     * @return this buffer for using in call chaining.
     */
    default ProtonBuffer writeBytes(ByteBuffer source) {
        if (source.hasArray()) {
            writeBytes(source.array(), source.arrayOffset() + source.position(), source.remaining());
            source.position(source.limit());
        } else {
            int writeOffset = getWriteOffset();
            int length = source.remaining();
            if (getWritableBytes() < length && getWriteOffset() + length <= implicitGrowthLimit()) {
                ensureWritable(length, 1, false);
            }
            advanceWriteOffset(source.remaining());

            // Try to reduce bounds-checking by using larger primitives when possible.
            for (; length >= Long.BYTES; length -= Long.BYTES, writeOffset += Long.BYTES) {
                setLong(writeOffset, source.getLong());
            }
            for (; length >= Integer.BYTES; length -= Integer.BYTES, writeOffset += Integer.BYTES) {
                setInt(writeOffset, source.getInt());
            }
            for (; length > 0; length--, writeOffset++) {
                setByte(writeOffset, source.get());
            }
        }

        return this;
    }

    /**
     * Read from this buffer, into the destination {@link ByteBuffer} This updates the read offset of this
     * buffer and also the position of the destination {@link ByteBuffer}.
     * <p>
     * Note: the behavior is undefined if the given {@link ByteBuffer} is an alias for the memory in this buffer.
     *
     * @param destination
     * 		The {@link ByteBuffer} to write into.
     *
     * @return this buffer for using in call chaining.
     */
    default ProtonBuffer readBytes(ByteBuffer destination) {
        final int byteCount = destination.remaining();
        copyInto(getReadOffset(), destination, destination.position(), byteCount);
        advanceReadOffset(byteCount);
        destination.position(destination.limit());
        return this;
    }

    /**
     * Read from this buffer, into the destination array, the given number of bytes.
     * This updates the read offset of this buffer by the length argument.
     *
     * @param destination The byte array to write into.
     * @param offset Position in the {@code destination} to where bytes should be written from this buffer.
     * @param length The number of bytes to copy.
     *
     * @return This buffer.
     */
    default ProtonBuffer readBytes(byte[] destination, int offset, int length) {
        copyInto(getReadOffset(), destination, offset, length);
        advanceReadOffset(length);
        return this;
    }

    /**
     * Read from this buffer and write to the given channel.
     * <p>
     * The number of bytes actually written to the channel are returned. No more than the given {@code length}
     * of bytes, or the number of {@linkplain #getReadableBytes() readable bytes}, will be written to the channel,
     * whichever is smaller. A channel that has a position marker, will be advanced by the number of bytes written.
     * The {@linkplain #getReadOffset() read offset} of this buffer will also be advanced by the number of bytes
     * written.
     *
     * @param channel The channel to write to.
     * @param length The maximum number of bytes to write.
     *
     * @return The actual number of bytes written, possibly zero.
     *
     * @throws IOException If the write-operation on the channel failed for some reason.
     */
    int transferTo(WritableByteChannel channel, int length) throws IOException;

    /**
     * Reads a sequence of bytes from the given channel into this buffer.
     * <p>
     * The method reads a given amount of bytes from the provided channel and returns the number of
     * bytes actually read which can be zero or -1 if the channel has reached the end of stream state.
     * <p>
     * The length value given is a maximum limit however the code will adjust this if the number
     * of writable bytes in this buffer is smaller (or zero) and the result will indicate how many
     * bytes where actually read.  The write offset of this buffer will be advanced by the number
     * of bytes read from the buffer as will the channel position index if one exists.
     *
     * @param channel The readable byte channel where the bytes are read
     * @param length The maximum number of bytes to read from the channel
     *
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
     *
     * @throws IOException if the read operation fails
     */
    int transferFrom(ReadableByteChannel channel, int length) throws IOException;

    /**
     * Reads a sequence of bytes from the given channel into this buffer.
     * <p>
     * The method reads a given amount of bytes from the provided channel and returns the number of
     * bytes actually read which can be zero or -1 if the channel has reached the end of stream state.
     * <p>
     * The length value given is a maximum limit however the code will adjust this if the number
     * of writable bytes in this buffer is smaller (or zero) and the result will indicate how many
     * bytes where actually read.  The write offset of this buffer will be advanced by the number
     * of bytes read from the buffer, the channel will not have its position modified.
     *
     * @param channel The File channel where the bytes are read
     * @param position The position in the channel where the read should begin
     * @param length The maximum number of bytes to read from the channel
     *
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
     *
     * @throws IOException if the read operation fails
     */
    int transferFrom(FileChannel channel, long position, int length) throws IOException;

    /**
     * @return true if the buffer is backed by one or more {@link ProtonBuffer} instances.
     */
    boolean isComposite();

    /**
     * Returns the number of constituent buffer components that are contained in this buffer instance
     * which for a non-composite buffer will always be one (namely itself).  For a composite buffer this
     * count is the total count of all buffers mapped to the composite.
     *
     * @return the number of buffers managed by this {@link ProtonBuffer} instance.
     */
    int componentCount();

    /**
     * Returns the number of readable constituent buffer components that are contained in this buffer
     * instance which for a non-composite buffer will always be zero or one (namely itself). For a composite
     * buffer this count is the total count of all buffers mapped to the composite which are readable.
     *
     * @return the number of readable buffers managed by this {@link ProtonBuffer} instance.
     */
    int readableComponentCount();

    /**
     * Returns the number of writable constituent buffer components that are contained in this buffer
     * instance which for a non-composite buffer will always be zero or one (namely itself). For a composite
     * buffer this count is the total count of all buffers mapped to the composite which are writable.
     *
     * @return the number of writable buffer components managed by this {@link ProtonBuffer} instance.
     */
    int writableComponentCount();

    /**
     * Returns a component access object that can be used to gain access to the constituent buffer components
     * for use in IO operations or other lower level buffer operations that need to work on single compoents.
     * <p>
     * The general usage of the component access object should be within a try-with-resource
     * block as follows:
     * <pre>{@code
     *   try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
     *      for (ProtonBufferComponent component : accessor.readableComponents()) {
     *         // Access logic here....
     *      }
     *   }
     * }</pre>
     *
     * @return a component access object instance used to view the buffer internal components
     */
    ProtonBufferComponentAccessor componentAccessor();

    /**
     * Creates and returns a new {@link ProtonBufferIterator} that iterates from the current
     * read offset and continues until all readable bytes have been traversed. The source buffer
     * read and write offsets are not modified by an iterator instance.
     * <p>
     * The caller must ensure that the source buffer lifetime extends beyond the lifetime of
     * the returned {@link ProtonBufferIterator}.
     *
     * @return a new buffer iterator that iterates over the readable bytes.
     */
    default ProtonBufferIterator bufferIterator() {
        return bufferIterator(getReadOffset(), getReadableBytes());
    }

    /**
     * Creates and returns a new {@link ProtonBufferIterator} that iterates from the given
     * offset and continues until specified number of bytes has been traversed. The source buffer
     * read and write offsets are not modified by an iterator instance.
     * <p>
     * The caller must ensure that the source buffer lifetime extends beyond the lifetime of
     * the returned {@link ProtonBufferIterator}.
     *
     * @param offset
     * 		The offset into the buffer where iteration begins
     * @param length
     * 		The number of bytes to iterate over.
     *
     * @return a new buffer iterator that iterates over the readable bytes.
     */
    ProtonBufferIterator bufferIterator(int offset, int length);

    /**
     * Creates and returns a new {@link ProtonBufferIterator} that reverse iterates over the readable
     * bytes of the source buffer (write offset to read offset). The source buffer read and write offsets
     * are not modified by an iterator instance.
     * <p>
     * The caller must ensure that the source buffer lifetime extends beyond the lifetime of
     * the returned {@link ProtonBufferIterator}.
     *
     * @return a new buffer iterator that iterates over the readable bytes.
     */
    default ProtonBufferIterator bufferReverseIterator() {
        return bufferReverseIterator(Math.max(0, getWriteOffset() - 1), getReadableBytes());
    }

    /**
     * Creates and returns a new {@link ProtonBufferIterator} that reverse iterates from the given
     * offset and continues until specified number of bytes has been traversed. The source buffer
     * read and write offsets are not modified by an iterator instance.
     * <p>
     * The caller must ensure that the source buffer lifetime extends beyond the lifetime of
     * the returned {@link ProtonBufferIterator}.
     *
     * @param offset
     * 		The offset into the buffer where iteration begins
     * @param length
     * 		The number of bytes to iterate over.
     *
     * @return a new buffer iterator that iterates over the readable bytes.
     */
    ProtonBufferIterator bufferReverseIterator(int offset, int length);

    /**
     * Starting from the current read offset into this buffer, find the next offset (index) in the
     * buffer where the given value is located or <code>-1</code> if the value is not found by the
     * time the remaining readable bytes has been searched. This method does not affect the read or
     * write offset and can be called from any point in the buffer regardless of the current read or
     * write offsets.
     *
     * @param needle
     * 		The byte value to search for in the remaining buffer bytes.
     *
     * @return the location in the buffer where the value was found or <code>-1</code> if not found.
     */
    default int indexOf(byte needle) {
        return indexOf(needle, getReadOffset(), getReadableBytes());
    }

    /**
     * Starting from the given offset into this buffer, find the next offset (index) in the
     * buffer where the given value is located or <code>-1</code> if the value is not found by the
     * time the specified number of bytes has been searched. This method does not affect the read or
     * write offset and can be called from any point in the buffer regardless of the current read or
     * write offsets. The search bounds are that of the buffer's readable bytes meaning that the
     * starting office cannot be less than the read offset and the length cannot cause the search
     * to read past the readable bytes otherwise an {@link IndexOutOfBoundsException} will be thrown.
     *
     * @param needle
     * 		The byte value to search for in the remaining buffer bytes.
     * @param offset
     * 		The offset into the buffer where the search should begin from.
     * @param length
     * 		The offset into the buffer where the search should begin from.
     *
     * @return the location in the buffer where the value was found or <code>-1</code> if not found.
     */
    int indexOf(byte needle, int offset, int length);

}
