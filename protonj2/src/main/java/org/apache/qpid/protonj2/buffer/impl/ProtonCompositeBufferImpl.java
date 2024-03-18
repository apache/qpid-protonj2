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
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAccessors;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.ProtonBufferIterator;
import org.apache.qpid.protonj2.buffer.ProtonBufferReadOnlyException;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.resource.SharedResource;

/**
 * The built in composite buffer implementation.
 */
public final class ProtonCompositeBufferImpl extends SharedResource<ProtonBuffer> implements ProtonCompositeBuffer {

    private static final ProtonBuffer[] EMPTY_COMPOSITE = new ProtonBuffer[0];
    private static final int[] EMPTY_COMPOSITE_INDICES = new int[0];

    private final ProtonBufferAllocator allocator;

    private int readOffset;
    private int writeOffset;
    private int capacity;
    private int implicitGrowthLimit = ProtonByteArrayBuffer.DEFAULT_MAXIMUM_CAPACITY;
    private boolean readOnly;
    private boolean closed;

    /**
     * Used when a read or write of a primitive value crosses chunk boundaries.
     */
    private final CrossChunkAccessor chunker = new CrossChunkAccessor(this);

    /**
     * Used when a read or write of a primitive value crosses chunk boundaries.
     */
    private final OffsetBufferAccessor offseter = new OffsetBufferAccessor(this);

    /**
     * The most recently used chunk which is used as a shortcut for linear read and write operations.
     */
    private int lastAccessedChunk = -1;

    /**
     * Array containing the buffers included in this composite
     */
    private ProtonBuffer[] buffers = EMPTY_COMPOSITE;

    /**
     * Array containing the start index of the buffers included in this composite
     */
    private int[] startIndices = EMPTY_COMPOSITE_INDICES;

    /**
     * Creates a new empty composite buffer.
     *
     * @param allocator the allocator that created this composite buffer.
     */
    public ProtonCompositeBufferImpl(ProtonBufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "ProtonBufferAllocator cannot be null.");
    }

    /**
     * Specialized internal constructor for splits where the buffers are already
     * validated and we don't need to perform as many setup checks, instead we just
     * take ownership of the buffer chunks and compute state from their offsets and
     * capacity values.
     *
     * @param chain
     *      The sequence of buffers chunks that will be contained here.
     * @param allocator
     * 		The buffer allocator for use new buffer allocation
     */
    private ProtonCompositeBufferImpl(ProtonBuffer[] chain, ProtonBufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "ProtonBufferAllocator cannot be null.");
        Objects.requireNonNull(chain, "Cannot create a split buffer from a null chain");

        this.buffers = chain;

        if (buffers.length > 0) {
            readOnly = buffers[0].isReadOnly();
        }

        fullRecomputeOfChunkIndexAndOffsetValues();
    }

    /**
     * Specialized constructor for use when transferring ownership.
     *
     * @param other
     * 		The other composite buffer we are capturing here.
     */
    private ProtonCompositeBufferImpl(ProtonCompositeBufferImpl other) {
        this.buffers = other.buffers;
        this.startIndices = other.startIndices;
        this.allocator = other.allocator;
        this.readOnly = other.readOnly;
        this.capacity = other.capacity;
        this.readOffset = other.readOffset;
        this.writeOffset = other.writeOffset;
        this.lastAccessedChunk = other.lastAccessedChunk;
    }

    /**
     * @return this buffer's allocator which was assigned on create.
     */
    public ProtonBufferAllocator allocator() {
        return allocator;
    }

    @Override
    public boolean isDirect() {
        if (capacity == 0) {
            return false;
        }

        // Every component must be direct in order for the composite to indicate it is direct.
        for (int i = 0; i < buffers.length; ++i) {
            if (!buffers[i].isDirect()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public ProtonCompositeBuffer convertToReadOnly() {
        // Every component must also be read only now.
        for (int i = 0; i < buffers.length; ++i) {
            buffers[i].convertToReadOnly();
        }
        readOnly = true;

        return this;
    }

    @Override
    public int implicitGrowthLimit() {
        return implicitGrowthLimit;
    }

    @Override
    public ProtonCompositeBuffer implicitGrowthLimit(int limit) {
        ProtonBufferUtils.checkImplicitGrowthLimit(limit, capacity());
        this.implicitGrowthLimit = limit;
        return this;
    }

    @Override
    public ProtonCompositeBuffer fill(byte value) {
        ProtonBufferUtils.checkIsClosed(this);
        checkWriteBounds(value, 0);

        for (int i = 0; i < buffers.length; ++i) {
            buffers[i].fill(value);
        }

        return this;
    }

    @Override
    public boolean isComposite() {
        return true;
    }

    @Override
    public int componentCount() {
        int result = 0;
        for (ProtonBuffer buffer : buffers) {
            result += buffer.componentCount();
        }
        return result;
    }

    @Override
    public int readableComponentCount() {
        int result = 0;
        for (ProtonBuffer buffer : buffers) {
            result += buffer.readableComponentCount();
        }
        return result;
    }

    @Override
    public int writableComponentCount() {
        int result = 0;
        for (ProtonBuffer buffer : buffers) {
            result += buffer.writableComponentCount();
        }
        return result;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public ProtonCompositeBuffer unwrap() {
        return this;
    }

    @Override
    public int getReadOffset() {
        return readOffset;
    }

    @Override
    public ProtonCompositeBuffer setReadOffset(int value) {
        checkReadBounds(value, 0);
        int remaining = value;
        for (int i = 0; i < buffers.length; ++i) {
            buffers[i].setReadOffset(Math.min(remaining, buffers[i].capacity()));
            remaining = Math.max(0, remaining - buffers[i].capacity());
        }
        readOffset = value;
        return this;
    }

    @Override
    public int getWriteOffset() {
        return writeOffset;
    }

    @Override
    public ProtonCompositeBuffer setWriteOffset(int value) {
        checkWriteBounds(value, 0);
        int remaining = value;
        for (int i = 0; i < buffers.length; ++i) {
            buffers[i].setWriteOffset(Math.min(remaining, buffers[i].capacity()));
            remaining = Math.max(0, remaining - buffers[i].capacity());
        }
        writeOffset = value;
        return this;
    }

    @Override
    public ProtonCompositeBuffer compact() {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (isReadOnly()) {
            throw new ProtonBufferReadOnlyException("Buffer must be writable in order to compact, but was read-only.");
        }

        final int distance = readOffset;
        if (distance == 0) {
            return this;
        }

        int pos = 0;
        for (int i = readOffset; i < writeOffset; ++i) {
            setByte(pos++, getByte(i));
        }

        // Another option might be to move completely read buffers to the tail and then
        // split any partially read remaining buffer and append the split to the tail.

        setReadOffset(0);
        setWriteOffset(writeOffset - distance);

        return this;
    }

    @Override
    public void copyInto(int offset, byte[] destination, int destOffset, int length) {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        ProtonBufferUtils.checkLength(length);
        ProtonBufferUtils.checkIsNotNegative(destOffset, "Array offset cannot be negative");
        if (offset < 0) {
            throw generateIndexOutOfBounds(offset, false);
        }
        if (offset + length > capacity) {
            throw generateIndexOutOfBounds(offset + length, false);
        }

        if (length == 0) {
            return;
        }

        int lastAccessedChunk = findChunkWithIndex(offset);

        while (length > 0) {
            final ProtonBuffer buffer = buffers[lastAccessedChunk];
            final int readBytes = Math.min(buffer.getReadableBytes(), length);

            buffer.copyInto(offset - startIndices[lastAccessedChunk], destination, destOffset, readBytes);

            offset += readBytes;
            length -= readBytes;
            destOffset += readBytes;

            lastAccessedChunk++;
        }
    }

    @Override
    public void copyInto(int offset, ByteBuffer destination, int destOffset, int length) {
        if (destination.isReadOnly()) {
            throw new ProtonBufferReadOnlyException();
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        ProtonBufferUtils.checkLength(length);
        ProtonBufferUtils.checkIsNotNegative(destOffset, "ByteBuffer offset cannot be negative");
        if (offset < 0) {
            throw generateIndexOutOfBounds(offset, false);
        }
        if (offset + length > capacity) {
            throw generateIndexOutOfBounds(offset + length, false);
        }

        int lastAccessedChunk = length > 0 ? findChunkWithIndex(offset) : 0;

        while (length > 0) {
            final ProtonBuffer buffer = buffers[lastAccessedChunk];
            final int readBytes = Math.min(buffer.getReadableBytes(), length);

            buffer.copyInto(offset - startIndices[lastAccessedChunk], destination, destOffset, readBytes);

            offset += readBytes;
            length -= readBytes;
            destOffset += readBytes;

            lastAccessedChunk++;
        }
    }

    @Override
    public void copyInto(int offset, ProtonBuffer destination, int destOffset, int length) {
        if (destination.isReadOnly()) {
            throw new ProtonBufferReadOnlyException();
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        ProtonBufferUtils.checkLength(length);
        ProtonBufferUtils.checkIsNotNegative(destOffset, "Buffer offset cannot be negative");
        if (offset < 0) {
            throw generateIndexOutOfBounds(offset, false);
        }
        if (offset + length > capacity) {
            throw generateIndexOutOfBounds(offset + length, false);
        }

        int lastAccessedChunk = length > 0 ? findChunkWithIndex(offset) : 0;

        while (length > 0) {
            final ProtonBuffer buffer = buffers[lastAccessedChunk];
            final int readBytes = Math.min(buffer.getReadableBytes(), length);

            buffer.copyInto(offset - startIndices[lastAccessedChunk], destination, destOffset, readBytes);

            offset += readBytes;
            length -= readBytes;
            destOffset += readBytes;

            lastAccessedChunk++;
        }
    }

    @Override
    public ProtonCompositeBuffer writeBytes(byte[] source, int offset, int length) {
        if (isReadOnly()) {
            throw new ProtonBufferReadOnlyException();
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        ProtonBufferUtils.checkLength(length);
        ProtonBufferUtils.checkIsNotNegative(offset, "Buffer offset cannot be negative");
        if (offset < 0) {
            throw generateIndexOutOfBounds(offset, false);
        }
        if (offset + length > source.length) {
            throw generateIndexOutOfBounds(offset + length, false);
        }

        if (length != 0) {
            prepareForWrite(writeOffset, length);

            lastAccessedChunk = findChunkWithIndex(writeOffset) - 1;

            while (length > 0) {
                final ProtonBuffer buffer = buffers[++lastAccessedChunk];
                final int writableBytes = Math.min(buffer.getWritableBytes(), length);

                buffer.writeBytes(source, offset, writableBytes);

                offset += writableBytes;
                length -= writableBytes;
                writeOffset += writableBytes;
            }
        }

        return this;
    }

    @Override
    public ProtonCompositeBuffer writeBytes(ProtonBuffer source) {
        if (isReadOnly()) {
            throw new ProtonBufferReadOnlyException();
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (!source.isReadable()) {
            return this;
        }

        prepareForWrite(writeOffset, source.getReadableBytes());

        lastAccessedChunk = findChunkWithIndex(writeOffset) - 1;

        while (source.getReadableBytes() > 0) {
            final ProtonBuffer buffer = buffers[++lastAccessedChunk];
            final int writableBytes = Math.min(buffer.getWritableBytes(), source.getReadableBytes());

            source.copyInto(source.getReadOffset(), buffer, writeOffset - startIndices[lastAccessedChunk], writableBytes);

            buffer.advanceWriteOffset(writableBytes);
            source.advanceReadOffset(writableBytes);
            writeOffset += writableBytes;
        }

        return this;
    }

    @Override
    public ProtonCompositeBuffer append(ProtonBuffer buffer) {
        final ProtonBuffer extension = Objects.requireNonNull(buffer, "Buffer to append cannot be null.").transfer();
        if (isClosed()) {
            extension.close();
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (extension.isReadOnly() != isReadOnly() && buffers.length > 0) {
            extension.close();
            throw new IllegalArgumentException("Appended buffer read-only state must match this buffers state");
        }

        try {
            ProtonBufferUtils.checkBufferCanGrowTo(capacity, extension.capacity());
        } catch (Exception e) {
            extension.close();
            throw e;
        }

        if (extension.capacity() == 0) {
            extension.close();
            return this;
        }

        if (buffers.length == 0 && !extension.isComposite()) {
            appendBuffer(extension);
            readOnly = extension.isReadOnly();
            readOffset = extension.getReadOffset();
            writeOffset = extension.getWriteOffset();
        } else if (writeOffset == capacity() && extension.getReadOffset() == 0 && !extension.isComposite()) {
            // Either more readable bytes or additional writable space which either way doesn't
            // disrupt this buffer's current state.
            appendBuffer(extension);
            writeOffset += extension.getWriteOffset();
        } else {
            final ProtonBuffer[] appendedChain = buildChainFromBuffers(new ProtonBuffer[] { extension });
            if (appendedChain.length > 0) {
                final ProtonBuffer[] newChain = new ProtonBuffer[appendedChain.length + buffers.length];

                // Fill the new buffer chain
                System.arraycopy(buffers, 0, newChain, 0, buffers.length);
                System.arraycopy(appendedChain, 0, newChain, buffers.length, appendedChain.length);

                buffers = filterCompositeBufferChain(newChain, readOnly);
                fullRecomputeOfChunkIndexAndOffsetValues();
            }
        }

        return this;
    }

    @Override
    public Iterable<ProtonBuffer> decomposeBuffer() {
        ProtonBufferUtils.checkIsClosed(this);

        final Iterable<ProtonBuffer> decomposed;

        if (buffers.length == 0) {
            decomposed = ProtonBufferIterable.EMPTY_ITERABLE;
        } else {
            decomposed = new ProtonBufferIterable(buffers);
        }

        // Reset to zero sized buffer
        buffers = EMPTY_COMPOSITE;
        startIndices = EMPTY_COMPOSITE_INDICES;
        capacity = 0;

        try {
            close();
        } catch (Throwable error) {
            for (ProtonBuffer buffer : decomposed) {
                try {
                    buffer.close();
                } catch(Throwable t) {
                    error.addSuppressed(t);
                }
            }

            throw error;
        }

        return decomposed;
    }

    @Override
    public ProtonCompositeBuffer ensureWritable(int size, int minimumGrowth, boolean allowCompaction) throws IndexOutOfBoundsException, IllegalArgumentException {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }
        ProtonBufferUtils.checkIsNotNegative(size, "New writable size value cannot be negative");
        ProtonBufferUtils.checkIsNotNegative(minimumGrowth, "Minimum growth value cannot be negative");
        if (isReadOnly()) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }
        if (getWritableBytes() >= size) {
            return this;
        }

        if (allowCompaction && readOffset >= size) {
            if (buffers.length == 1) {
                final ProtonBuffer target = buffers[0];
                target.compact();
                readOffset = target.getReadOffset();
                writeOffset = target.getWriteOffset();
            } else {
                int compacted = 0;
                for (int i = 0; i < buffers.length; ++i) {
                    final ProtonBuffer current = buffers[i];
                    if (current.getReadableBytes() == current.capacity()) {
                        compacted++;
                        // Buffer is now fully writable so reset the offsets
                        current.clear();
                        continue;
                    } else {
                        break;
                    }
                }

                if (compacted > 0) {
                    ProtonBuffer[] compactedSet = new ProtonBuffer[compacted];

                    // Retain the front buffers that are going to the end
                    System.arraycopy(buffers, 0, compactedSet, 0, compacted);
                    // Move the rest down
                    System.arraycopy(buffers, compacted, buffers, 0, buffers.length - compacted);
                    // Placed the compacted buffers at the tail
                    System.arraycopy(compactedSet, 0, buffers, compacted, compacted);

                    recomputeChunkIndexValues();
                }
            }
        }

        if (getWritableBytes() < size) {
            final int allocate = Math.max(size - getWritableBytes(), minimumGrowth);
            ProtonBufferUtils.checkBufferCanGrowTo(capacity(), allocate);
            appendBuffer(allocator.allocate(allocate));
        }

        return this;
    }

    @Override
    public ProtonCompositeBuffer copy(int index, int length, boolean readOnly) throws IllegalArgumentException {
        ProtonBufferUtils.checkLength(length);
        checkGetBounds(index, length);
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        ProtonBuffer[] copy = EMPTY_COMPOSITE;

        if (buffers.length != 0 && length > 0) {
            final int startingPoint = findChunkWithIndex(index);

            int remaining = length - (buffers[startingPoint].capacity() - index);
            int requiredCopies = 1;

            // Compute how large the copied buffers array needs to be then do the actual copy
            // to avoid allocating more than one array.
            for (int i = startingPoint + 1; i < buffers.length && remaining > 0; ++i, ++requiredCopies) {
                remaining -= buffers[i].capacity();
            }

            copy = new ProtonBuffer[requiredCopies];

            for (int i = 0; i < requiredCopies; ++i) {
                final ProtonBuffer current = buffers[startingPoint + i];
                final int copyPointOffset = index - startIndices[startingPoint + i];
                final int available = Math.min(current.capacity() - copyPointOffset, length);

                copy[i] = current.copy(copyPointOffset, available, readOnly);

                index += available;
                length -= available;
            }
        }

        return new ProtonCompositeBufferImpl(copy, allocator);
    }

    @Override
    public ProtonCompositeBuffer split(int splitOffset) {
        ProtonBufferUtils.checkIsNotNegative(splitOffset, "The split offset value cannot be negative");
        if (capacity() < splitOffset) {
            throw new IllegalArgumentException(
                "The split offset cannot be greater than the buffer capacity, " +
                "but the split offset was " + splitOffset + ", and capacity is " + capacity() + '.');
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        if (buffers.length == 0 || splitOffset == 0) {
            return new ProtonCompositeBufferImpl(allocator);
        }

        final int splitPoint = splitOffset != capacity() ? findChunkWithIndex(splitOffset) : buffers.length;
        final int offsetSplitOffset = splitOffset != capacity ? splitOffset - startIndices[splitPoint] : 0;

        final ProtonBuffer[] front;
        final ProtonBuffer[] rear = new ProtonBuffer[buffers.length - splitPoint];

        if (offsetSplitOffset != 0) {
            front = new ProtonBuffer[splitPoint + 1];

            System.arraycopy(buffers, 0, front, 0, splitPoint);
            System.arraycopy(buffers, splitPoint, rear, 0, buffers.length - splitPoint);

            front[splitPoint] = buffers[splitPoint].split(offsetSplitOffset);
        } else {
            front = new ProtonBuffer[splitPoint];

            System.arraycopy(buffers, 0, front, 0, splitPoint);
            System.arraycopy(buffers, splitPoint, rear, 0, buffers.length - splitPoint);
        }

        this.buffers = rear;

        fullRecomputeOfChunkIndexAndOffsetValues();

        return new ProtonCompositeBufferImpl(front, allocator);
    }

    @Override
    public ProtonCompositeBuffer splitComponentsFloor(int splitOffset) {
        ProtonBufferUtils.checkIsNotNegative(splitOffset, "The split offset value cannot be negative");
        if (capacity() < splitOffset) {
            throw new IllegalArgumentException(
                "The split offset cannot be greater than the buffer capacity, " +
                "but the split offset was " + splitOffset + ", and capacity is " + capacity() + '.');
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        if (buffers.length == 0 || splitOffset < buffers[0].capacity()) {
            return new ProtonCompositeBufferImpl(allocator);
        }

        final int splitPoint = splitOffset != capacity() ?
            Math.max(1, findChunkWithIndex(splitOffset) - 1) : buffers.length;

        final ProtonBuffer[] front = new ProtonBuffer[splitPoint];
        final ProtonBuffer[] back = new ProtonBuffer[buffers.length - splitPoint];

        System.arraycopy(buffers, 0, front, 0, front.length);
        System.arraycopy(buffers, splitPoint, back, 0, back.length);

        buffers = back;

        fullRecomputeOfChunkIndexAndOffsetValues();

        return new ProtonCompositeBufferImpl(front, allocator);
    }

    @Override
    public ProtonCompositeBuffer splitComponentsCeil(int splitOffset) {
        ProtonBufferUtils.checkIsNotNegative(splitOffset, "The split offset value cannot be negative");
        if (capacity() < splitOffset) {
            throw new IllegalArgumentException(
                "The split offset cannot be greater than the buffer capacity, " +
                "but the split offset was " + splitOffset + ", and capacity is " + capacity() + '.');
        }
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        if (buffers.length == 0 || splitOffset == 0) {
            return new ProtonCompositeBufferImpl(allocator);
        }

        final int splitPoint = splitOffset != capacity() ?
            Math.min(buffers.length, findChunkWithIndex(splitOffset) + 1) : buffers.length;

        final ProtonBuffer[] front = new ProtonBuffer[splitPoint];
        final ProtonBuffer[] back = new ProtonBuffer[buffers.length - splitPoint];

        System.arraycopy(buffers, 0, front, 0, front.length);
        System.arraycopy(buffers, splitPoint, back, 0, back.length);

        buffers = back;

        fullRecomputeOfChunkIndexAndOffsetValues();

        return new ProtonCompositeBufferImpl(front, allocator);
    }

    //----- Offset based get operations

    @Override
    public byte getByte(int index) {
        checkGetBounds(index, Byte.BYTES);
        return findIndexedAccessor(index, Byte.BYTES).getByte(index);
    }

    @Override
    public char getChar(int index) {
        checkGetBounds(index, Character.BYTES);
        return findIndexedAccessor(index, Character.BYTES).getChar(index);
    }

    @Override
    public short getShort(int index) {
        checkGetBounds(index, Short.BYTES);
        return findIndexedAccessor(index, Short.BYTES).getShort(index);
    }

    @Override
    public int getInt(int index) {
        checkGetBounds(index, Integer.BYTES);
        return findIndexedAccessor(index, Integer.BYTES).getInt(index);
    }

    @Override
    public long getLong(int index) {
        checkGetBounds(index, Long.BYTES);
        return findIndexedAccessor(index, Long.BYTES).getLong(index);
    }

    //----- Offset based set operations

    @Override
    public ProtonBuffer setByte(int index, byte value) {
        checkWriteBounds(index, Byte.BYTES);
        findIndexedAccessor(index, Byte.BYTES).setByte(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, char value) {
        checkWriteBounds(index, Character.BYTES);
        findIndexedAccessor(index, Character.BYTES).setChar(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, short value) {
        checkWriteBounds(index, Short.BYTES);
        findIndexedAccessor(index, Short.BYTES).setShort(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        checkWriteBounds(index, Integer.BYTES);
        findIndexedAccessor(index, Integer.BYTES).setInt(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        checkWriteBounds(index, Long.BYTES);
        findIndexedAccessor(index, Long.BYTES).setLong(index, value);
        return this;
    }

    //----- Relative read operations

    @Override
    public byte readByte() {
        checkReadBounds(readOffset, Byte.BYTES);
        return findReadAccessor(Byte.BYTES).readByte();
    }

    @Override
    public char readChar() {
        checkReadBounds(readOffset, Character.BYTES);
        return findReadAccessor(Character.BYTES).readChar();
    }

    @Override
    public short readShort() {
        checkReadBounds(readOffset, Short.BYTES);
        return findReadAccessor(Short.BYTES).readShort();
    }

    @Override
    public int readInt() {
        checkReadBounds(readOffset, Integer.BYTES);
        return findReadAccessor(Integer.BYTES).readInt();
    }

    @Override
    public long readLong() {
        checkReadBounds(readOffset, Long.BYTES);
        return findReadAccessor(Long.BYTES).readLong();
    }

    //----- Relative write operations

    @Override
    public ProtonBuffer writeByte(byte value) {
        prepareForWrite(writeOffset, Byte.BYTES);
        findWriteAccessor(Byte.BYTES).writeByte(value);
        return this;
    }

    @Override
    public ProtonBuffer writeChar(char value) {
        prepareForWrite(writeOffset, Character.BYTES);
        findWriteAccessor(Character.BYTES).writeChar(value);
        return this;
    }

    @Override
    public ProtonBuffer writeShort(short value) {
        prepareForWrite(writeOffset, Short.BYTES);
        findWriteAccessor(Short.BYTES).writeShort(value);
        return this;
    }

    @Override
    public ProtonBuffer writeInt(int value) {
        prepareForWrite(writeOffset, Integer.BYTES);
        findWriteAccessor(Integer.BYTES).writeInt(value);
        return this;
    }

    @Override
    public ProtonBuffer writeLong(long value) {
        prepareForWrite(writeOffset, Long.BYTES);
        findWriteAccessor(Long.BYTES).writeLong(value);
        return this;
    }

    //----- JDK support method overrides

    @Override
    public boolean equals(Object o) {
        return o instanceof ProtonBuffer && ProtonBufferUtils.equals(this, (ProtonBuffer) o);
    }

    @Override
    public int hashCode() {
        return ProtonBufferUtils.hashCode(this);
    }

    @Override
    public String toString() {
        return "ProtonCompositeBuffer" +
               "{ read:" + readOffset +
               ", write: " + writeOffset +
               ", capacity: " + capacity + "}";
    }

    //----- Buffer IO interoperability handlers

    @Override
    public int transferTo(WritableByteChannel channel, int length) throws IOException {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        ProtonBufferUtils.checkIsNotNegative(length, "TransferTo length cannot be negative: " + length);

        final int writableBytes = Math.min(getReadableBytes(), length);

        checkGetBounds(readOffset, writableBytes);

        if (writableBytes == 0) {
            return 0;
        }

        if (channel instanceof GatheringByteChannel) {
            return transferToGatheringByteChannel((GatheringByteChannel) channel, writableBytes);
        } else {
            return transferToWritableByteChannel(channel, writableBytes);
        }
    }

    private int transferToGatheringByteChannel(GatheringByteChannel channel, int length) throws IOException {
        final ByteBuffer[] composingBuffers = new ByteBuffer[readableComponentCount()];

        int count = 0;
        int bytesWritten = 0;

        try (ProtonBufferComponentAccessor accessor = componentAccessor()) {
            for (ProtonBufferComponent component = accessor.firstReadable(); component != null; component = accessor.nextReadable()) {
                composingBuffers[count++] = component.getReadableBuffer();
            }

            for (count = 0; count < composingBuffers.length && length > 0; ++count) {
                if ((length -= composingBuffers[count].remaining()) < 0) {
                    final ByteBuffer buffer = composingBuffers[count];
                    buffer.limit(buffer.limit() + length);
                }
            }

            bytesWritten = Math.toIntExact(channel.write(composingBuffers, 0, count));
        } finally {
            if (bytesWritten > 0) {
                advanceReadOffset(bytesWritten);
            }
        }

        return bytesWritten;
    }

    private int transferToWritableByteChannel(WritableByteChannel channel, int length) throws IOException {
        int currentChunkIndex = findChunkWithIndex(readOffset);
        int bytesWritten = 0;

        while (bytesWritten < length && currentChunkIndex < buffers.length) {
            final ProtonBuffer buffer = buffers[currentChunkIndex];
            final int chunkedWrite = Math.min(buffer.getReadableBytes(), length);

            final int written = buffer.transferTo(channel, chunkedWrite);

            if (written == 0) {
                break;
            }

            readOffset = Math.addExact(readOffset, written);
            bytesWritten += written;
            currentChunkIndex++;
        }

        return bytesWritten;
    }

    @Override
    public int transferFrom(ReadableByteChannel channel, int length) throws IOException {
        ProtonBufferUtils.checkArgumentIsNotNegative(length, "Length given cannot be negative");
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsReadOnly(this);

        final int readableBytes = Math.min(getWritableBytes(), length);

        if (readableBytes == 0) {
            return 0;
        }

        checkWriteBounds(getWriteOffset(), readableBytes);

        int lastAccessedChunk = findChunkWithIndex(writeOffset);

        int bytesRead = 0;

        while (bytesRead < readableBytes && lastAccessedChunk < buffers.length) {
            final ProtonBuffer buffer = buffers[lastAccessedChunk];
            final int chunckedRead = Math.min(buffer.getWritableBytes(), readableBytes);

            final int read = buffer.transferFrom(channel, chunckedRead);

            if (read <= 0) {
                bytesRead = bytesRead == 0 ? -1 : bytesRead;
                break;
            }

            writeOffset = Math.addExact(writeOffset, read);
            bytesRead += read;
            lastAccessedChunk++;
        }

        return bytesRead;
    }

    @Override
    public int transferFrom(FileChannel channel, long position, int length) throws IOException {
        ProtonBufferUtils.checkArgumentIsNotNegative(position, "Position");
        ProtonBufferUtils.checkArgumentIsNotNegative(length, "Length");
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsReadOnly(this);

        final int readableBytes = Math.min(getWritableBytes(), length);

        if (readableBytes == 0) {
            return 0;
        }

        checkWriteBounds(getWriteOffset(), readableBytes);

        int lastAccessedChunk = findChunkWithIndex(writeOffset);

        int bytesRead = 0;

        while (bytesRead < readableBytes && lastAccessedChunk < buffers.length) {
            final ProtonBuffer buffer = buffers[lastAccessedChunk];
            final int chunckedRead = Math.min(buffer.getWritableBytes(), readableBytes);

            final int read = buffer.transferFrom(channel, position, chunckedRead);

            if (read <= 0) {
                bytesRead = bytesRead == 0 ? -1 : bytesRead;
                break;
            }

            writeOffset = Math.addExact(writeOffset, read);
            bytesRead += read;
            position += read;
            lastAccessedChunk++;
        }

        return bytesRead;
    }

    //----- Buffer Iteration API

    @Override
    public ProtonBufferIterator bufferIterator(int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkArgumentIsNotNegative(offset, "offset");
        ProtonBufferUtils.checkArgumentIsNotNegative(length, "length");

        if (offset + length > capacity()) {
            throw new IndexOutOfBoundsException(
                "The iterator cannot read beyond the bounds of the buffer: offset=" + offset + ", length=" + length);
        }

        return new ProtonCompositeBufferIterator(offset, length);
    }

    private final class ProtonCompositeBufferIterator implements ProtonBufferIterator {

        private final int endOffset;
        private int offset;

        private int currentBufferIndex;
        private int currentBufferOffset;

        public ProtonCompositeBufferIterator(int offset, int length) {
            this.endOffset = offset + length;
            this.offset = offset;
            this.currentBufferIndex = findChunkWithIndex(offset);
            this.currentBufferOffset = offset - startIndices[currentBufferIndex];
        }

        @Override
        public boolean hasNext() {
            return offset < endOffset;
        }

        @Override
        public byte next() {
            if (offset == endOffset) {
                throw new NoSuchElementException("Buffer iteration complete, no additional bytes available");
            }

            final byte result = buffers[currentBufferIndex].getByte(currentBufferOffset++);

            if (++offset != endOffset && currentBufferOffset == buffers[currentBufferIndex].capacity()) {
                currentBufferIndex = findChunkWithIndex(currentBufferOffset);
                currentBufferOffset = 0;
            }

            return result;
        }

        @Override
        public int remaining() {
            return endOffset - offset;
        }

        @Override
        public int offset() {
            return offset;
        }
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

        return new ProtonCompositeBufferReverseIterator(offset, length);
    }

    private final class ProtonCompositeBufferReverseIterator implements ProtonBufferIterator {

        private final int endOffset;
        private int offset;

        private int currentBufferIndex;
        private int currentBufferOffset;

        public ProtonCompositeBufferReverseIterator(int offset, int length) {
            this.endOffset = offset - length;
            this.offset = offset;
            this.currentBufferIndex = findChunkWithIndex(offset);
            this.currentBufferOffset = offset - startIndices[currentBufferIndex];
        }

        @Override
        public boolean hasNext() {
            return offset > endOffset;
        }

        @Override
        public byte next() {
            if (offset == endOffset) {
                throw new NoSuchElementException("Buffer iteration complete, no additional bytes available");
            }

            final byte result = buffers[currentBufferIndex].getByte(currentBufferOffset--);

            if (--offset != endOffset && currentBufferOffset < 0) {
                currentBufferIndex = findChunkWithIndex(offset);
                currentBufferOffset = 0;
            }

            return result;
        }

        @Override
        public int remaining() {
            return Math.abs(endOffset - offset);
        }

        @Override
        public int offset() {
            return offset;
        }
    }

    //----- Buffer search API

    @Override
    public int indexOf(byte needle, int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);

        checkIndexOfBounds(offset, length);

        int bytesRead = 0;
        int lastAccessedChunk = findChunkWithIndex(offset);
        int readOffset = offset - startIndices[lastAccessedChunk];

        while (length > 0 && lastAccessedChunk != buffers.length) {
            final ProtonBuffer buffer = buffers[lastAccessedChunk++];
            final int readableBytes = Math.min(buffer.capacity(), length);

            final int result = buffer.indexOf(needle, readOffset, readableBytes);
            if (result != -1) {
                return bytesRead + result;
            }

            bytesRead += buffer.getReadableBytes();
            length -= buffer.getReadableBytes();
            readOffset = 0;
        }

        return -1;
    }

    //----- Buffer component access API

    @Override
    public ProtonBufferComponentAccessor componentAccessor() {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        return new ProtonCompositeBufferComponentAccessor((ProtonCompositeBufferImpl) acquire());
    }

    //----- Static Composite buffer API

    /**
     * Create an empty {@link ProtonCompositeBuffer} with the given allocator which
     * will be used any time the buffer needs to allocate additional storage space
     * for implicit write or explicit ensure writable calls.
     *
     * @param allocator the allocator to use when expanding capacity
     *
     * @return a new {@link ProtonCompositeBuffer} that uses the provided allocator
     */
    public static ProtonCompositeBuffer create(ProtonBufferAllocator allocator) {
        return new ProtonCompositeBufferImpl(allocator);
    }

    /**
     * Create an empty {@link ProtonCompositeBuffer} with the given allocator which
     * will be used any time the buffer needs to allocate additional storage space
     * for implicit write or explicit ensure writable calls.
     *
     * @param allocator the allocator to use when expanding capacity.
     * @param buffers the sequence of buffers to compose.
     *
     * @return a new {@link ProtonCompositeBuffer} that uses the provided allocator
     */
    public static ProtonCompositeBuffer create(ProtonBufferAllocator allocator, ProtonBuffer[] buffers) {
        Objects.requireNonNull(buffers, "Provided buffers array cannot be null");

        final boolean readOnly = buffers[0].isReadOnly();
        final ProtonBuffer[] chain = buildChainFromBuffers(Arrays.copyOf(buffers, buffers.length));
        final ProtonCompositeBuffer result;

        try {
            if (chain.length == 0) {
                result = new ProtonCompositeBufferImpl(allocator);
            } else {
                result = new ProtonCompositeBufferImpl(
                    filterCompositeBufferChain(chain, readOnly), allocator);
            }
        } catch (RuntimeException e) {
            throw closeBuffers(buffers, chain, e);
        }

        return result;
    }

    /**
     * Create an empty {@link ProtonCompositeBuffer} with the given allocator which
     * will be used any time the buffer needs to allocate additional storage space
     * for implicit write or explicit ensure writable calls.
     *
     * @param allocator the allocator to use when expanding capacity.
     * @param buffer the buffers to compose.
     *
     * @return a new {@link ProtonCompositeBuffer} that uses the provided allocator
     */
    public static ProtonCompositeBuffer create(ProtonBufferAllocator allocator, ProtonBuffer buffer) {
        Objects.requireNonNull(buffer, "Provided buffers cannot be null");

        final ProtonBuffer[] chain = buildChainFromBuffers(new ProtonBuffer[] { buffer });
        final ProtonCompositeBuffer result;

        try {
            if (chain.length == 0) {
                result = new ProtonCompositeBufferImpl(allocator);
            } else {
                result = new ProtonCompositeBufferImpl(chain, allocator);
            }
        } catch (RuntimeException e) {
            throw closeBuffers(new ProtonBuffer[] { buffer }, chain, e);
        }

        return result;
    }

    //----- Shared resource API implementation

    @Override
    protected void releaseResourceOwnership() {
        closed = true;
        readOnly = false;
        capacity = 0;
        readOffset = 0;
        writeOffset = 0;

        try {
            for (int i = 0; i < buffers.length; ++i) {
                try {
                    buffers[i].close();
                } catch (Throwable t) {
                    // Exception on close is swallowed
                }
            }
        } finally {
            buffers = EMPTY_COMPOSITE;
            startIndices = EMPTY_COMPOSITE_INDICES;
        }
    }

    @Override
    protected ProtonBuffer transferTheResource() {
        final ProtonCompositeBufferImpl transfer = new ProtonCompositeBufferImpl(this);

        // Need to ensure any subsequent close doesn't try and close the buffers that are now moved.
        buffers = EMPTY_COMPOSITE;
        startIndices = EMPTY_COMPOSITE_INDICES;

        return transfer;
    }

    @Override
    protected RuntimeException resourceIsClosedException() {
        return ProtonBufferUtils.genericBufferIsClosed(this);
    }

    //----- Internal buffer API

    private static ProtonBuffer[] buildChainFromBuffers(ProtonBuffer[] buffers) {
        RuntimeException suppressed = null;

        ProtonBuffer[] chain = buffers; // Already copied before here so just use directly

        int totalChunks = 0;

        // Claim each buffer before proceeding to filter and trim the buffers for composition.
        // Decompose and composites here before moving on to the next phase as well as removing
        // any zero capacity buffers.
        for (int i = 0; i < chain.length; i++) {
            try {
                if (buffers[i].capacity() > 0) {
                    if (buffers[i].isComposite()) {
                        final int bufferCount = buffers[i].componentCount();

                        if (bufferCount > 1) {
                            chain = Arrays.copyOf(chain, chain.length + bufferCount - 1);
                        }

                        for (ProtonBuffer buffer : ((ProtonCompositeBuffer)buffers[i]).decomposeBuffer()) {
                            chain[totalChunks++] = buffer;
                        }

                        i += bufferCount - 1;

                        continue;
                    } else {
                        chain[i] = buffers[i].transfer();
                        totalChunks++;
                    }
                } else if (buffers[i].isClosed()) {
                    // This will capture attempt to add duplicate
                    throw new ProtonBufferClosedException("Cannot create a composite from a closed buffer");
                } else {
                    buffers[i].close();
                }
            } catch (RuntimeException e) {
                suppressed = e;
                break;
            }
        }

        if (suppressed != null) {
            throw closeBuffers(buffers, chain, suppressed);
        }

        if (chain.length > totalChunks) {
            chain = Arrays.copyOf(chain, totalChunks);
        }

        return chain;
    }

    private static RuntimeException closeBuffers(ProtonBuffer[] source, ProtonBuffer[] captured, RuntimeException exRoot) {
        for (int i = 0; i < captured.length && captured[i] != null; ++i) {
            try {
                captured[i].close();
            } catch (Throwable e) {
                exRoot.addSuppressed(e);
            }
        }
        for (ProtonBuffer buffer : source) {
            try {
                buffer.close();
            } catch (Throwable e) {
                exRoot.addSuppressed(e);
            }
        }

        return exRoot;
    }

    //----- Internal API for composite buffer

    private void checkGetBounds(int index, int size) {
        if (index < 0 || capacity < index + size) {
            throw generateIndexOutOfBounds(index, false);
        }
    }

    private void checkReadBounds(int index, int size) {
        if (index < 0 || writeOffset < index + size) {
            throw generateIndexOutOfBounds(index, false);
        }
    }

    private void checkIndexOfBounds(int index, int size) {
        if (index < readOffset || writeOffset < index + size) {
            throw generateIndexOutOfBounds(index, false);
        }
    }

    private void prepareForWrite(int index, int size) {
        if (getWritableBytes() < size && writeOffset + size <= implicitGrowthLimit && !readOnly) {
            final int minGrowth;
            if (buffers.length == 0) {
                minGrowth = Math.min(implicitGrowthLimit, 64);
            } else {
                minGrowth = Math.min(Math.max((capacity() / buffers.length) * 2, size), implicitGrowthLimit - capacity);
            }
            ensureWritable(size, minGrowth, false);
        }

        checkWriteBounds(index, size);
    }

    private void checkWriteBounds(int index, int size) {
        if (index < 0 || capacity < index + size || readOnly) {
            throw generateIndexOutOfBounds(index, true);
        }
    }

    private RuntimeException generateIndexOutOfBounds(int index, boolean write) {
        if (closed) {
            return ProtonBufferUtils.genericBufferIsClosed(this);
        }
        if (write && readOnly) {
            return ProtonBufferUtils.genericBufferIsReadOnly(this);
        }

        return new IndexOutOfBoundsException(
            "Index " + index + " is out of bounds: [read 0 to " + writeOffset + ", write 0 to " + capacity + "].");
    }

    private ProtonBufferAccessors findIndexedAccessor(int index, int size) {
        final int chunkForIndex = findChunkWithIndex(index);
        final ProtonBufferAccessors accessor;

        if (size > buffers[chunkForIndex].capacity() - (index - startIndices[chunkForIndex])) {
            accessor = chunker.prepare(chunkForIndex);
        } else {
            accessor = offseter.prepare(chunkForIndex);
        }

        return accessor;
    }

    private ProtonBufferAccessors findReadAccessor(int size) {
        final int chunkForIndex = findChunkWithIndex(readOffset);
        final ProtonBufferAccessors accessor;

        if (buffers[chunkForIndex].getReadableBytes() < size) {
            accessor = chunker.prepare(chunkForIndex);
        } else {
            accessor = buffers[chunkForIndex];
        }

        readOffset += size;

        return accessor;
    }

    private ProtonBufferAccessors findWriteAccessor(int size) {
        final int chunkForIndex = findChunkWithIndex(writeOffset);
        final ProtonBufferAccessors accessor;

        if (buffers[chunkForIndex].getWritableBytes() < size) {
            accessor = chunker.prepare(chunkForIndex);
        } else {
            accessor = buffers[chunkForIndex];
        }

        writeOffset += size;

        return accessor;
    }

    private int findChunkWithIndex(int index) {
        if (index < startIndices[lastAccessedChunk]) {
            do {
                --lastAccessedChunk;
            } while (index < startIndices[lastAccessedChunk]);
        } else if (index >= startIndices[lastAccessedChunk] + buffers[lastAccessedChunk].capacity()) {
            do {
                ++lastAccessedChunk;
            } while (index >= startIndices[lastAccessedChunk] + buffers[lastAccessedChunk].capacity());
        }

        return lastAccessedChunk;
    }

    private void recomputeChunkIndexValues() {
        for (int i = 0, capacity = 0; i < buffers.length; capacity += buffers[i++].capacity()) {
            startIndices[i] = capacity;
        }
    }

    private void fullRecomputeOfChunkIndexAndOffsetValues() {
        int totalCapcity = 0;
        int writeOffset = 0;
        int readOffset = 0;

        // Buffer might have grown or been split which means the index tracker
        // would need to be updated.
        if (startIndices.length != buffers.length) {
            startIndices = new int[buffers.length];
        }

        boolean compositeWriteOffsetFound = false;
        boolean compositeReadOffsetFound = false;

        // We will already have filtered for buffers that have unread or unwritten gaps so
        // we know that the buffers are a linear progression of buffer segments.
        for (int i = 0; i < buffers.length; ++i) {
            if (!compositeWriteOffsetFound) {
                writeOffset += buffers[i].getWriteOffset();
                if (buffers[i].getWritableBytes() > 0) {
                    compositeWriteOffsetFound = true;
                }
            } else if (buffers[i].getWriteOffset() != 0) {
                throw new IllegalArgumentException(
                    "The given buffers cannot be composed because they leave an unwritten gap: ");
            }
            if (!compositeReadOffsetFound) {
                readOffset += buffers[i].getReadOffset();
                if (buffers[i].getReadableBytes() > 0 || compositeWriteOffsetFound) {
                    compositeReadOffsetFound = true;
                }
            } else if (buffers[i].getReadOffset() != 0) {
                throw new IllegalArgumentException(
                    "The given buffers cannot be composed because they leave an unread gap: ");
            }

            startIndices[i] = totalCapcity;
            totalCapcity += buffers[i].capacity();
        }

        ProtonBufferUtils.checkBufferCanGrowTo(totalCapcity, 0);

        this.readOffset = readOffset;
        this.writeOffset = writeOffset;
        this.capacity = totalCapcity;
        this.lastAccessedChunk = 0;
    }

    private ProtonCompositeBuffer appendBuffer(ProtonBuffer buffer) {
        final ProtonBuffer[] newBuffers = Arrays.copyOf(buffers, buffers.length + 1);
        final int[] newIndices = Arrays.copyOf(startIndices, newBuffers.length);

        newBuffers[buffers.length] = buffer;
        newIndices[buffers.length] = capacity; // Start index of new buffer is the current capacity
                                               // because we don't allow empty buffers.

        capacity += buffer.capacity();

        if (lastAccessedChunk == -1) {
            lastAccessedChunk = 0;
        }

        this.buffers = newBuffers;
        this.startIndices = newIndices;

        return this;
    }

    private static ProtonBuffer[] filterCompositeBufferChain(ProtonBuffer[] chain, boolean readOnlyExpectation) {
        int firstReadable = -1;
        int lastReadable = -1;
        int totalChunks = chain.length;
        int totalCapacity = 0;
        int readableBytes = 0;

        for (int i = 0; i < chain.length; ++i) {
            if (readOnlyExpectation != chain[i].isReadOnly()) {
                throw new IllegalArgumentException("The buffers must all have the same read-only state");
            }

            if (chain[i].getReadableBytes() != 0) {
                if (firstReadable == -1) {
                    firstReadable = i;
                }
                lastReadable = i;
            }

            totalCapacity += chain[i].capacity();
            readableBytes += chain[i].getReadableBytes();
        }

        // Only check for gaps if all bytes are not readable bytes
        if (firstReadable != -1 && totalCapacity != readableBytes) {
            for (int i = 0; i < totalChunks; ++i) {
                final ProtonBuffer buffer = chain[i];

                // buffers between the first readable and last readable that have no
                // readable bytes can be dropped to avoid a gap.
                if (i > firstReadable && i < lastReadable && buffer.getReadableBytes() == 0) {
                    buffer.close();
                    System.arraycopy(chain, i + 1, chain, i, chain.length - (i + 1));
                    lastReadable--;
                    totalChunks--;
                    i--;
                    continue;
                }
                // Buffers after the first readable that have already been read must
                // be dropped to avoid gaps between the constituent tail buffers
                if (i > firstReadable && buffer.getReadOffset() > 0) {
                    buffer.readSplit(0).close();
                }
                // Buffers with writable bytes that precede the last readable buffer
                // need to have the writable chunks trimmed to avoid gaps.
                if (i < lastReadable && buffer.getWriteOffset() != buffer.capacity()) {
                    if (buffer.getWriteOffset() == 0) {
                        System.arraycopy(chain, i + 1, chain, i, chain.length - (i + 1));
                        lastReadable--;
                        totalChunks--;
                        i--;
                    } else {
                        chain[i] = buffer.split();
                    }

                    buffer.close();
                }
            }

            if (chain.length > totalChunks) {
                chain = Arrays.copyOf(chain, totalChunks);
            }
        }

        return chain;
    }

    private static class OffsetBufferAccessor implements ProtonBufferAccessors {

        private int chunkIndex;
        private int startIndex;

        private final ProtonCompositeBufferImpl parent;

        public OffsetBufferAccessor(ProtonCompositeBufferImpl parent) {
            this.parent = parent;
        }

        public ProtonBufferAccessors prepare(int chunkIndex) {
            this.chunkIndex = chunkIndex;
            this.startIndex = parent.startIndices[chunkIndex];
            return this;
        }

        private int offset(int index) {
            return index - startIndex;
        }

        // These must be called after validating that the target index is
        // within the chunk in question, otherwise an IndexOutOfBoundsException
        // will be thrown from the buffer. Also the size of the operations must
        // fall within the available portion of the buffer in this chunk or
        // an IOOBE will again be thrown.

        @Override
        public byte getByte(int index) {
            return parent.buffers[chunkIndex].getByte(offset(index));
        }

        @Override
        public ProtonBuffer setByte(int index, byte value) {
            return parent.buffers[chunkIndex].setByte(offset(index), value);
        }

        @Override
        public byte readByte() {
            return parent.buffers[chunkIndex].readByte();
        }

        @Override
        public ProtonBuffer writeByte(byte value) {
            return parent.buffers[chunkIndex].writeByte(value);
        }

        @Override
        public char getChar(int index) {
            return parent.buffers[chunkIndex].getChar(offset(index));
        }

        @Override
        public ProtonBuffer setChar(int index, char value) {
            return parent.buffers[chunkIndex].setChar(offset(index), value);
        }

        @Override
        public char readChar() {
            return parent.buffers[chunkIndex].readChar();
        }

        @Override
        public ProtonBuffer writeChar(char value) {
            return parent.buffers[chunkIndex].writeChar(value);
        }

        @Override
        public short getShort(int index) {
            return parent.buffers[chunkIndex].getShort(offset(index));
        }

        @Override
        public ProtonBuffer setShort(int index, short value) {
            return parent.buffers[chunkIndex].setShort(offset(index), value);
        }

        @Override
        public short readShort() {
            return parent.buffers[chunkIndex].readShort();
        }

        @Override
        public ProtonBuffer writeShort(short value) {
            return parent.buffers[chunkIndex].writeShort(value);
        }

        @Override
        public int getInt(int index) {
            return parent.buffers[chunkIndex].getInt(offset(index));
        }

        @Override
        public ProtonBuffer setInt(int index, int value) {
            return parent.buffers[chunkIndex].setInt(offset(index), value);
        }

        @Override
        public int readInt() {
            return parent.buffers[chunkIndex].readInt();
        }

        @Override
        public ProtonBuffer writeInt(int value) {
            return parent.buffers[chunkIndex].writeInt(value);
        }

        @Override
        public long getLong(int index) {
            return parent.buffers[chunkIndex].getLong(offset(index));
        }

        @Override
        public ProtonBuffer setLong(int index, long value) {
            return parent.buffers[chunkIndex].setLong(offset(index), value);
        }

        @Override
        public long readLong() {
            return parent.buffers[chunkIndex].readLong();
        }

        @Override
        public ProtonBuffer writeLong(long value) {
            return parent.buffers[chunkIndex].writeLong(value);
        }
    }

    private static class CrossChunkAccessor implements ProtonBufferAccessors {

        private final ProtonCompositeBufferImpl parent;

        public CrossChunkAccessor(ProtonCompositeBufferImpl parent) {
            this.parent = parent;
        }

        private int chunk;

        public ProtonBufferAccessors prepare(int chunk) {
            this.chunk = chunk;
            return this;
        }

        @Override
        public byte getByte(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProtonBuffer setByte(int index, byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte readByte() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProtonBuffer writeByte(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public char getChar(int index) {
            char result = 0;

            result |= (parent.buffers[chunk].getByte(index - parent.startIndices[chunk]) & 0xFF) << Byte.SIZE;
            result |= (parent.buffers[++chunk].getByte(0) & 0xFF);

            return result;
        }

        @Override
        public ProtonBuffer setChar(int index, char value) {
            parent.buffers[chunk].setByte(index - parent.startIndices[chunk], (byte) (value >>> 8));
            parent.buffers[++chunk].setByte(0, (byte) (value & 0xFF));

            return parent;
        }

        @Override
        public char readChar() {
            char result = 0;

            result |= (parent.buffers[chunk].readByte() & 0xFF) << Byte.SIZE;
            result |= (parent.buffers[++chunk].readByte() & 0xFF);

            return result;
        }

        @Override
        public ProtonBuffer writeChar(char value) {
            parent.buffers[chunk].writeByte((byte) (value >>> 8));
            parent.buffers[++chunk].writeByte((byte) (value & 0xFF));

            return parent;
        }

        @Override
        public short getShort(int index) {
            short result = 0;

            result |= (parent.buffers[chunk].getByte(index - parent.startIndices[chunk]) & 0xFF) << Byte.SIZE;
            result |= (parent.buffers[++chunk].getByte(0) & 0xFF);

            return result;
        }

        @Override
        public ProtonBuffer setShort(int index, short value) {
            parent.buffers[chunk].setByte(index - parent.startIndices[chunk], (byte) (value >>> 8));
            parent.buffers[++chunk].setByte(0, (byte) (value & 0xFF));

            return parent;
        }

        @Override
        public short readShort() {
            short result = 0;

            result |= (parent.buffers[chunk].readByte() & 0xFF) << Byte.SIZE;
            result |= (parent.buffers[++chunk].readByte() & 0xFF);

            return result;
        }

        @Override
        public ProtonBuffer writeShort(short value) {
            parent.buffers[chunk].writeByte((byte) (value >>> 8));
            parent.buffers[++chunk].writeByte((byte) (value & 0xFF));

            return parent;
        }

        @Override
        public int getInt(int index) {
            int result = 0;

            index -= parent.startIndices[chunk];

            for (int i = Integer.BYTES - 1; i >= 0; --i) {
                result |= (parent.buffers[chunk].getByte(index++) & 0xFF) << (i * Byte.SIZE);
                if (parent.buffers[chunk].capacity() <= index) {
                    ++chunk;
                    index = 0;
                }
            }

            return result;
        }

        @Override
        public ProtonBuffer setInt(int index, int value) {
            index -= parent.startIndices[chunk];

            for (int i = Integer.BYTES - 1; i >= 0; --i) {
                parent.buffers[chunk].setByte(index++, (byte) (value >>> (i * Byte.SIZE)));
                if (parent.buffers[chunk].capacity() <= index) {
                    ++chunk;
                    index = 0;
                }
            }

            return parent;
        }

        @Override
        public int readInt() {
            int result = 0;

            for (int i = Integer.BYTES - 1; i >= 0; --i) {
                result |= (parent.buffers[chunk].readByte() & 0xFF) << (i * Byte.SIZE);
                if (parent.buffers[chunk].getReadableBytes() <= 0) {
                    ++chunk;
                }
            }

            return result;
        }

        @Override
        public ProtonBuffer writeInt(int value) {
            for (int i = Integer.BYTES - 1; i >= 0; --i) {
                parent.buffers[chunk].writeByte((byte) (value >>> (i * Byte.SIZE)));
                if (parent.buffers[chunk].getWritableBytes() <= 0) {
                    ++chunk;
                }
            }

            return parent;
        }

        @Override
        public long getLong(int index) {
            long result = 0;

            index -= parent.startIndices[chunk];

            for (int i = Long.BYTES - 1; i >= 0; --i) {
                result |= (long) (parent.buffers[chunk].getByte(index++) & 0xFF) << (i * Byte.SIZE);
                if (parent.buffers[chunk].capacity() <= index) {
                    ++chunk;
                    index = 0;
                }
            }

            return result;
        }

        @Override
        public ProtonBuffer setLong(int index, long value) {
            index -= parent.startIndices[chunk];

            for (int i = Long.BYTES - 1; i >= 0; --i) {
                parent.buffers[chunk].setByte(index++, (byte) (value >>> (i * Byte.SIZE)));
                if (parent.buffers[chunk].capacity() <= index) {
                    ++chunk;
                    index = 0;
                }
            }

            return parent;
        }

        @Override
        public long readLong() {
            long result = 0;

            for (int i = Long.BYTES - 1; i >= 0; --i) {
                result |= (long) (parent.buffers[chunk].readByte() & 0xFF) << (i * Byte.SIZE);
                if (parent.buffers[chunk].getReadableBytes() <= 0) {
                    ++chunk;
                }
            }

            return result;
        }

        @Override
        public ProtonBuffer writeLong(long value) {
            for (int i = Long.BYTES - 1; i >= 0; --i) {
                parent.buffers[chunk].writeByte((byte) (value >>> (i * Byte.SIZE)));
                if (parent.buffers[chunk].getWritableBytes() <= 0) {
                    ++chunk;
                }
            }

            return parent;
        }
    }

    /**
     * Iterable that expects an untethered chain of chunks for use in decomposing
     * the buffer by unlinking the chunks from the buffer head and tail nodes.
     */
    private static class ProtonBufferIterable implements Iterable<ProtonBuffer> {

        public static final Iterable<ProtonBuffer> EMPTY_ITERABLE = Collections.emptyList();

        private final ProtonBuffer[] chain;

        public ProtonBufferIterable(ProtonBuffer[] chain) {
            this.chain = chain;
        }

        @Override
        public Iterator<ProtonBuffer> iterator() {
            return new ProtonBufferIterator();
        }

        private final class ProtonBufferIterator implements Iterator<ProtonBuffer> {

            private int next;

            public ProtonBufferIterator() {
                this.next = 0;
            }

            @Override
            public boolean hasNext() {
                return next < chain.length;
            }

            @Override
            public ProtonBuffer next() {
                if (next == chain.length) {
                    throw new NoSuchElementException();
                }

                final ProtonBuffer result = chain[next++];

                return result;
            }
        }
    }

    private static final class ProtonCompositeBufferComponentAccessor implements ProtonBufferComponentAccessor, ProtonBufferComponent {

        private final ProtonCompositeBufferImpl composite;

        private ProtonBufferComponentAccessor currentAccessor;
        private ProtonBufferComponent currentComponent;

        private int bufferIndex;
        private int cumulativeReadSkip; // Track total read skip to update parent in-place
        private int cumulativeWriteSkip; // Track total read skip to update parent in-place

        public ProtonCompositeBufferComponentAccessor(ProtonCompositeBufferImpl composite) {
            this.composite =  composite;
        }

        @Override
        public void close() {
            if (currentAccessor != null) {
                currentAccessor.close();
                currentAccessor = null;
            }

            composite.close();
        }

        @Override
        public ProtonBufferComponent first() {
            if (currentAccessor != null) {
                currentAccessor.close();
                currentAccessor = null;
            }

            if (composite.buffers.length != 0) {
                currentAccessor = composite.buffers[0].componentAccessor();
                currentComponent = currentAccessor.first();
                bufferIndex = 0;
            }

            return currentComponent == null ? null : this;
        }

        @Override
        public ProtonBufferComponent next() {
            ProtonBufferComponent next = currentAccessor != null ? currentAccessor.next() : null;

            if (next == null) {
                if (currentAccessor != null) {
                    // We exhausted the current buffers components so clear them
                    currentAccessor.close();
                    currentAccessor = null;
                    currentComponent = null;

                    if (composite.buffers.length > ++bufferIndex) {
                        currentAccessor = composite.buffers[bufferIndex].componentAccessor();
                        currentComponent = currentAccessor.first();
                    }
                }
            }

            return currentComponent == null ? null : this;
        }

        @Override
        public int getReadableBytes() {
            return currentComponent.getReadableBytes();
        }

        @Override
        public boolean hasReadbleArray() {
            return currentComponent.hasReadbleArray();
        }

        @Override
        public ProtonBufferComponent advanceReadOffset(int amount) {
            currentComponent.advanceReadOffset(amount);
            composite.setReadOffset(cumulativeReadSkip + amount);
            cumulativeReadSkip += amount;

            return currentComponent;
        }

        @Override
        public byte[] getReadableArray() {
            return currentComponent.getReadableArray();
        }

        @Override
        public int getReadableArrayOffset() {
            return currentComponent.getReadableArrayOffset();
        }

        @Override
        public int getReadableArrayLength() {
            return currentComponent.getReadableArrayLength();
        }

        @Override
        public ByteBuffer getReadableBuffer() {
            return currentComponent.getReadableBuffer();
        }

        @Override
        public int getWritableBytes() {
            return currentComponent.getWritableBytes();
        }

        @Override
        public ProtonBufferComponent advanceWriteOffset(int amount) {
            currentComponent.advanceWriteOffset(amount);
            composite.setWriteOffset(cumulativeWriteSkip + amount);
            cumulativeWriteSkip += amount;

            return currentComponent;
        }

        @Override
        public boolean hasWritableArray() {
            return currentComponent.hasWritableArray();
        }

        @Override
        public byte[] getWritableArray() {
            return currentComponent.getWritableArray();
        }

        @Override
        public int getWritableArrayOffset() {
            return currentComponent.getWritableArrayOffset();
        }

        @Override
        public int getWritableArrayLength() {
            return currentComponent.getWritableArrayLength();
        }

        @Override
        public ByteBuffer getWritableBuffer() {
            return currentComponent.getWritableBuffer();
        }

        @Override
        public long getNativeAddress() {
            return currentComponent.getNativeAddress();
        }

        @Override
        public long getNativeReadAddress() {
            return currentComponent.getNativeReadAddress();
        }

        @Override
        public long getNativeWriteAddress() {
            return currentComponent.getNativeWriteAddress();
        }

        @Override
        public ProtonBufferIterator bufferIterator() {
            return currentComponent.bufferIterator();
        }

        @Override
        public Object unwrap() {
            return currentComponent.unwrap();
        }
    }
}
