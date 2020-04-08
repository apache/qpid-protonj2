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
import java.util.function.Consumer;

/**
 * A composite of 1 or more ProtonBuffer instances used when aggregating buffer views.
 */
public final class ProtonCompositeBuffer extends ProtonAbstractBuffer {

    public static final int DEFAULT_MAXIMUM_CAPACITY = Integer.MAX_VALUE;

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);

    /**
     * Aggregated count of all readable bytes in all buffers in the composite.
     */
    private int capacity;

    /**
     * Current number of ProtonBuffer chunks that are contained in this composite.
     */
    private int totalChunks;

    /**
     * The most recently used chunk which is used as a shortcut for linear read and write operations.
     */
    private Chunk lastAccessedChunk;

    /**
     * The fixed head pointer for the chain of buffer chunks
     */
    private final Chunk head;

    /**
     * The fixed tail pointer for the chain of buffer chunks
     */
    private final Chunk tail;

    /**
     * Creates a Composite Buffer instance with max capacity of {@link Integer#MAX_VALUE}.
     */
    public ProtonCompositeBuffer() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Creates a Composite Buffer instance with the maximum capacity provided.
     *
     * @param maximumCapacity
     *      The maximum capacity that this buffer can grow to.
     */
    public ProtonCompositeBuffer(int maximumCapacity) {
        super(maximumCapacity);

        this.head = new Chunk(null, 0, 0, -1, -1);
        this.tail = new Chunk(null, 0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);

        this.head.next = tail;
        this.tail.prev = head;

        // We never allow this to be null, it is either at the bounds or on a valid chunk.
        this.lastAccessedChunk = head;
    }

    /**
     * Appends the given byte array to the end of the buffer segments that comprise this composite
     * {@link ProtonBuffer} instance.
     *
     * @param array
     *      The array to append.
     *
     * @return this {@link ProtonCompositeBuffer} instance.
     *
     * @throws IndexOutOfBoundsException if the appended buffer would result in max capacity being exceeded.
     */
    public ProtonCompositeBuffer append(byte[] array) {
        Objects.requireNonNull(array, "Cannot append null array to composite buffer.");
        return append(ProtonByteBufferAllocator.DEFAULT.wrap(array));
    }

    /**
     * Appends the given byte array to the end of the buffer segments that comprise this composite
     * {@link ProtonBuffer} instance.
     *
     * @param array
     *      The array to append.
     * @param offset
     *      The offset into the given array to index read and write operations.
     * @param length
     *      The usable portion of the given array.
     *
     * @return this {@link ProtonCompositeBuffer} instance.
     *
     * @throws IndexOutOfBoundsException if the appended buffer would result in max capacity being exceeded.
     */
    public ProtonCompositeBuffer append(byte[] array, int offset, int length) {
        Objects.requireNonNull(array, "Cannot append null array to composite buffer.");
        return append(ProtonByteBufferAllocator.DEFAULT.wrap(array, offset, length));
    }

    /**
     * Appends the given {@link ProtonBuffer} to the end of the buffer segments that comprise this composite
     * {@link ProtonBuffer} instance.
     *
     * @param buffer
     *      The {@link ProtonBuffer} instance to append.
     *
     * @return this {@link ProtonCompositeBuffer} instance.
     *
     * @throws IndexOutOfBoundsException if the appended buffer would result in max capacity being exceeded.
     */
    public ProtonCompositeBuffer append(ProtonBuffer buffer) {
        if (!buffer.isReadable()) {
            return this;
        }

        if (buffer.getReadableBytes() + capacity() > maxCapacity()) {
            throw new IndexOutOfBoundsException(String.format(
                "capacity(%d) + readableBytes(%d) exceeds maxCapacity(%d): %s",
                capacity(), buffer.getReadableBytes(), maxCapacity(), this));
        }

        // If already at end we extend the write index to the new end of the composite
        int newWriteIndex = writeIndex == capacity ? writeIndex + buffer.getReadableBytes() : writeIndex;
        appendBuffer(buffer).setWriteIndex(newWriteIndex);

        return this;
    }

    /**
     * @return the total number of {@link ProtonBuffer} segments in this composite buffer isntance.
     */
    public int numberOfBuffers() {
        return totalChunks;
    }

    /**
     * For each of the buffers contained in this {@link ProtonCompositeBuffer} instance the
     * given consumer will be invoked with a duplicate of the buffer that can be independently
     * modified and not affect the contents of this buffer.
     *
     * @param consumer
     *      The {@link Consumer} that will be called with each buffer instance.
     *
     * @return this {@link ProtonCompositeBuffer} instance.
     */
    public ProtonCompositeBuffer foreachBuffer(Consumer<ProtonBuffer> consumer) {
        Chunk current = head.next;
        while (current != tail) {
            consumer.accept(current.buffer.duplicate());
            current = current.next;
        }

        return this;
    }

    /**
     * For each of the buffers contained in this {@link ProtonCompositeBuffer} instance the
     * given consumer will be invoked with the {@link ProtonBuffer} that backs this composite
     * instance.  Modifying the {@link ProtonBuffer} passed to the consumer modified the buffer
     * backing this composite and as such leaves this composite in an unknown and invalid state.
     *
     * @param consumer
     *      The {@link Consumer} that will be called with each buffer instance.
     *
     * @return this {@link ProtonCompositeBuffer} instance.
     */
    public ProtonCompositeBuffer foreachInternalBuffer(Consumer<ProtonBuffer> consumer) {
        Chunk current = head.next;
        while (current != tail) {
            consumer.accept(current.buffer);
            current = current.next;
        }

        return this;
    }

    //----- ProtonAbstractBuffer API implementation

    @Override
    public boolean hasArray() {
        switch (totalChunks) {
            case 0:
                return true;
            case 1:
                return head.next.buffer.hasArray();
            default:
                return false;
        }
    }

    @Override
    public byte[] getArray() {
        switch (totalChunks) {
            case 0:
                return EMPTY_BYTE_ARRAY;
            case 1:
                return head.next.buffer.getArray();
            default:
                throw new UnsupportedOperationException("Buffer does not have a backing array.");
        }
    }

    @Override
    public int getArrayOffset() {
        switch (totalChunks) {
            case 0:
                return 0;
            case 1:
                return head.next.buffer.getArrayOffset();
            default:
                throw new UnsupportedOperationException("Buffer does not have a backing array.");
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        checkNewCapacity(newCapacity);

        if (newCapacity > capacity) {
            final int amountNeeded = newCapacity - capacity;
            appendBuffer(ProtonByteBufferAllocator.DEFAULT.allocate(amountNeeded, amountNeeded).setWriteIndex(amountNeeded));
        } else if (newCapacity < capacity) {
            int reductionTarget = capacity - newCapacity;
            Chunk current = tail.prev;
            while (current != head) {
                if (current.length > reductionTarget) {
                    ProtonBuffer sliced = current.buffer.slice(current.buffer.getReadIndex(), reductionTarget);
                    Chunk replacement = new Chunk(
                        sliced, 0, reductionTarget, current.startIndex, current.startIndex + reductionTarget);
                    current.next.prev = replacement;
                    current.prev.next = replacement;
                    replacement.next = current.next;
                    replacement.prev = current.prev;
                    break;
                } else {
                    reductionTarget -= current.length;
                    current.next.prev = current.prev;
                    current.prev.next = current.next;
                    totalChunks--;
                }

                current = current.prev;
            }

            capacity = newCapacity;
            if (writeIndex > capacity) {
                writeIndex = capacity;
            }
            if (readIndex > capacity) {
                readIndex = capacity;
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer duplicate() {
        return new ProtonDuplicatedBuffer(this);
    }

    @Override
    public byte getByte(int index) {
        checkIndex(index, Byte.BYTES);
        Chunk targetChunk = findChunkWithIndex(index);
        return targetChunk.readByte(index);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index, Short.BYTES);

        short result = 0;

        lastAccessedChunk = findChunkWithIndex(index);

        for (int i = Short.BYTES - 1; i >= 0; --i) {
            result |= (lastAccessedChunk.readByte(index++) & 0xFF) << (i * Byte.SIZE);
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return result;
    }

    @Override
    public int getInt(int index) {
        checkIndex(index, Integer.BYTES);
        lastAccessedChunk = findChunkWithIndex(index);

        int result = 0;

        for (int i = Integer.BYTES - 1; i >= 0; --i) {
            result |= (lastAccessedChunk.readByte(index++) & 0xFF) << (i * Byte.SIZE);
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return result;
    }

    @Override
    public long getLong(int index) {
        checkIndex(index, Long.BYTES);
        lastAccessedChunk = findChunkWithIndex(index);

        long result = 0;

        for (int i = Long.BYTES - 1; i >= 0; --i) {
            result |= (long) (lastAccessedChunk.readByte(index++) & 0xFF) << (i * Byte.SIZE);
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return result;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int destinationIndex, int length) {
        checkDestinationIndex(index, length, destinationIndex, destination.capacity());

        while (length > 0) {
            lastAccessedChunk = findChunkWithIndex(index);
            final int readBytes = lastAccessedChunk.getBytes(index, destination, destinationIndex, length);
            index += readBytes;
            length -=readBytes;
            destinationIndex += readBytes;
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        checkDestinationIndex(index, length, offset, destination.length);

        while (length > 0) {
            lastAccessedChunk = findChunkWithIndex(index);
            final int readBytes = lastAccessedChunk.getBytes(index, destination, offset, length);
            index += readBytes;
            length -=readBytes;
            offset += readBytes;
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        checkIndex(index, destination.remaining());

        while (destination.hasRemaining()) {
            lastAccessedChunk = findChunkWithIndex(index);
            final int readBytes = lastAccessedChunk.getBytes(index, destination);
            index += readBytes;
        }

        return this;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        checkIndex(index, Byte.BYTES);
        lastAccessedChunk = findChunkWithIndex(index);
        lastAccessedChunk.writeByte(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        checkIndex(index, Short.BYTES);
        lastAccessedChunk = findChunkWithIndex(index);

        lastAccessedChunk.writeByte(index++, (byte) (value >>> 8));
        if (lastAccessedChunk.endIndex < index) {
            lastAccessedChunk = lastAccessedChunk.next;
        }
        lastAccessedChunk.writeByte(index++, (byte) (value & 0xFF));

        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        checkIndex(index, Integer.BYTES);
        lastAccessedChunk = findChunkWithIndex(index);

        for (int i = Integer.BYTES - 1; i >= 0; --i) {
            lastAccessedChunk.writeByte(index++, (byte) (value >>> (i * Byte.SIZE)));
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        checkIndex(index, Long.BYTES);
        lastAccessedChunk = findChunkWithIndex(index);

        for (int i = Long.BYTES - 1; i >= 0; --i) {
            lastAccessedChunk.writeByte(index++, (byte) (value >>> (i * Byte.SIZE)));
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.capacity());

        // TODO - Initial exceedingly slow implementation for test construction
        for (int i = 0; i < length; ++i) {
            setByte(index++, source.getByte(sourceIndex++));
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.length);

        // TODO - Initial exceedingly slow implementation for test construction
        for (int i = 0; i < length; ++i) {
            setByte(index++, source[sourceIndex + i]);
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        checkSourceIndex(index, source.remaining() - source.position(), source.position(), source.remaining());

        // TODO - Initial exceedingly slow implementation for test construction
        while (source.hasRemaining()) {
            setByte(index++, source.get());
        }

        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        checkIndex(index, length);

        final ProtonBuffer copy = ProtonByteBufferAllocator.DEFAULT.allocate(length);

        // TODO - Exceedingly slow initial implementation test get tests going.
        lastAccessedChunk = findChunkWithIndex(index);
        for (int i = 0; i < length; ++i) {
            copy.setByte(i, lastAccessedChunk.readByte(index++));
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        copy.setWriteIndex(length);

        return copy;
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        switch (totalChunks) {
            case 0:
                return EMPTY_BYTE_BUFFER;
            case 1:
                return head.next.toByteBuffer(index, length);
            default:
                return internalToByteBuffer(index, length);
        }
    }

    //----- Internal Support Framework API

    private ByteBuffer internalToByteBuffer(int index, int length) {
        checkIndex(index, length);

        Chunk targetChunk = findChunkWithIndex(index);
        if (targetChunk.isInRange(index, length)) {
            return targetChunk.toByteBuffer(index, length);
        } else {
            byte[] copy = new byte[length];
            int offset = 0;

            while (length > 0) {
                final int readBytes = targetChunk.getBytes(index, copy, offset, length);
                index += readBytes;
                length -=readBytes;
                offset += readBytes;
                targetChunk = findChunkWithIndex(index);
            }

            return ByteBuffer.wrap(copy);
        }
    }

    private Chunk findChunkWithIndex(int index) {
        if (index < lastAccessedChunk.startIndex) {
            while (lastAccessedChunk.prev != head) {
                lastAccessedChunk = lastAccessedChunk.prev;
                if (lastAccessedChunk.isInRange(index)) {
                    break;
                }
            }
        } else if (index > lastAccessedChunk.endIndex) {
            while (lastAccessedChunk.next != tail) {
                lastAccessedChunk = lastAccessedChunk.next;
                if (lastAccessedChunk.isInRange(index)) {
                    break;
                }
            }
        }

        return lastAccessedChunk;
    }

    /*
     * Appends the buffer to the end of the current set of chunks but does not alter the
     * read or write index values, this is just a way to add capacity.
     */
    private ProtonCompositeBuffer appendBuffer(ProtonBuffer buffer) {
        int window = buffer.getReadableBytes();
        // We only read and write within the readable portion of the contained chunk so
        // our capacity follows the total readable bytes from all chunks.
        capacity += window;
        totalChunks++;

        final Chunk newChunk = new Chunk(buffer, buffer.getReadIndex(), window, tail.prev.endIndex + 1, tail.prev.endIndex + window);

        // Link the new chunk onto the end updating any previous chunk as well.
        newChunk.prev = tail.prev;
        newChunk.next = tail;
        tail.prev.next = newChunk;
        tail.prev = newChunk;

        if (lastAccessedChunk == head || lastAccessedChunk == tail) {
            lastAccessedChunk = newChunk;
        }

        return this;
    }

    private void checkBufferIndex(int index) {
        if (index < 0 || index > totalChunks) {
            throw new IndexOutOfBoundsException(String.format(
                    "The buffer index: %d (expected: >= 0 && <= numberOfBuffers(%d))",
                    index, totalChunks));
        }
    }

    /*
     * A chunk of the composite buffer which holds the back buffer for that chunk and any
     * additional data needed to represent this chunk in the chain.  Chucks are chained in
     * order by link the first Chunk to the next using the next entry value.
     */
    private static class Chunk {

        private final ProtonBuffer buffer;
        private final int offset;
        private final int length;

        // We can more quickly traverse the chunks to locate an index read / write
        // by tracking in this chunk where it lives in the buffer scope.
        private final int startIndex;
        private final int endIndex;

        private Chunk next;
        private Chunk prev;

        public Chunk(ProtonBuffer buffer, int offset, int length, int startIndex, int endIndex) {
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public int getBytes(int index, ByteBuffer destination) {
            final int readable = Math.min(length - (index - startIndex), destination.remaining());

            int oldLimit = destination.limit();
            destination.limit(destination.position() + readable);
            try {
                buffer.getBytes(offset(index), destination);
            } finally {
                destination.limit(oldLimit);
            }

            return readable;
        }

        public int getBytes(int index, byte[] destination, int offset, int desiredLength) {
            final int readable = Math.min(length - (index - startIndex), desiredLength);

            buffer.getBytes(offset(index), destination, offset, readable);

            return readable;
        }

        public int getBytes(int index, ProtonBuffer destination, int destinationIndex, int desiredLength) {
            final int readable = Math.min(length - (index - startIndex), desiredLength);

            buffer.getBytes(offset(index), destination, destinationIndex, readable);

            return readable;
        }

        public byte readByte(int index) {
            return buffer.getByte(offset(index));
        }

        public void writeByte(int index, int value) {
            buffer.setByte(offset(index), value);
        }

        public boolean isInRange(int index) {
            if (index >= startIndex && index <= endIndex) {
                return true;
            } else {
                return false;
            }
        }

        public boolean isInRange(int index, int length) {
            if (index >= startIndex && (index + (length - 1) <= endIndex)) {
                return true;
            } else {
                return false;
            }
        }

        public ByteBuffer toByteBuffer(int index, int length) {
            return buffer.toByteBuffer(offset(index), length);
        }

        @Override
        public String toString() {
            return String.format("Chunk: { len=%d, sidx=%d, eidx=%d }", length, startIndex, endIndex);
        }

        private int offset(int index) {
            return (index - startIndex) + offset;
        }
    }
}
