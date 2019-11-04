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
     * @param maximumCapacity
     */
    ProtonCompositeBuffer(int maximumCapacity) {
        super(maximumCapacity);

        this.head = new Chunk(null, 0, -1, -1);
        this.tail = new Chunk(null, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);

        this.head.next = tail;
        this.tail.prev = head;

        // We never allow this to be null, it is either at the bounds or on a valid chunk.
        this.lastAccessedChunk = head;
    }

    public ProtonCompositeBuffer addBuffer(ProtonBuffer buffer) {
        if (!buffer.isReadable()) {
            return this;
        }

        // If already at end we extend the write index to the new end of the composite
        int newWriteIndex = writeIndex == capacity ? writeIndex + buffer.getReadableBytes() : writeIndex;
        appendBuffer(buffer).setWriteIndex(newWriteIndex);

        return this;
    }

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
        if (hasArray()) {
            return head.next.buffer.getArray();
        }

        throw new UnsupportedOperationException("Buffer does not have a backing array.");
    }

    @Override
    public int getArrayOffset() {
        if (hasArray()) {
            return head.next.buffer.getArrayOffset();
        }

        throw new UnsupportedOperationException("Buffer does not have a backing array.");
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
                if (current.chunkSize > reductionTarget) {
                    // We could copy here which might preserve the array backing if we held only one buffer
                    // and the original was array backed.
                    Chunk replacement = new Chunk(
                        current.buffer.slice(current.startIndex, reductionTarget), reductionTarget, 0, reductionTarget - 1);
                    current.next.prev = replacement;
                    current.prev.next = replacement;
                    break;
                } else {
                    reductionTarget -= current.chunkSize;
                    current.next.prev = current.prev;
                    current.prev.next = current.next;
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
        lastAccessedChunk = findChunkWithIndex(index);

        // TODO - Initial exceedingly slow implementation for test construction
        for (int i = 0; i < length; ++i) {
            destination.setByte(destinationIndex++, lastAccessedChunk.readByte(index++));
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        checkDestinationIndex(index, length, offset, destination.length);
        lastAccessedChunk = findChunkWithIndex(index);

        // TODO - Initial exceedingly slow implementation for test construction
        for (int i = 0; i < length; ++i) {
            destination[offset++] = lastAccessedChunk.readByte(index++);
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        checkIndex(index, destination.remaining());
        lastAccessedChunk = findChunkWithIndex(index);

        // TODO - Initial exceedingly slow implementation for test construction
        while (destination.hasRemaining()) {
            destination.put(lastAccessedChunk.readByte(index++));
            if (lastAccessedChunk.endIndex < index) {
                lastAccessedChunk = lastAccessedChunk.next;
            }
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
        //Chunk targetChunk = findChunkWithIndex(index);

        // TODO - Initial exceedingly slow implementation for test construction
        for (int i = 0; i < length; ++i) {
            setByte(index++, source.getByte(sourceIndex++));
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.length);
        //Chunk targetChunk = findChunkWithIndex(index);

        // TODO - Initial exceedingly slow implementation for test construction
        for (int i = 0; i < length; ++i) {
            setByte(index++, source[sourceIndex + i]);
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        checkSourceIndex(index, source.remaining() - source.position(), source.position(), source.remaining());
        //Chunk targetChunk = findChunkWithIndex(index);

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
                throw new RuntimeException("Not yet implemented.");  // TODO - Copy range into allocated ByteBuffer
        }
    }

    //----- Internal Support Framework API

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

        final Chunk newChunk = new Chunk(buffer, window, tail.prev.endIndex + 1, tail.prev.endIndex + window);

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

    /*
     * A chunk of the composite buffer which holds the back buffer for that chunk and any
     * additional data needed to represent this chunk in the chain.  Chucks are chained in
     * order by link the first Chunk to the next using the next entry value.
     */
    private static class Chunk {

        private final ProtonBuffer buffer;
        private final int chunkSize;

        // We can more quickly traverse the chunks to locate an index read / write
        // by tracking in this chunk where it lives in the buffer scope.
        private final int startIndex;
        private final int endIndex;

        private Chunk next;
        private Chunk prev;

        public Chunk(ProtonBuffer buffer, int chunkSize, int startIndex, int endIndex) {
            this.buffer = buffer;
            this.chunkSize = chunkSize;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public byte readByte(int index) {
            return buffer.getByte(index - startIndex);
        }

        public void writeByte(int index, int value) {
            buffer.setByte(index - startIndex, value);
        }

        public boolean isInRange(int index) {
            if (index >= startIndex && index <= endIndex) {
                return true;
            } else {
                return false;
            }
        }

        public ByteBuffer toByteBuffer(int index, int length) {
            return buffer.toByteBuffer(index, length);
        }

        @Override
        public String toString() {
            return String.format("Chunk: { len=%d, sidx=%d, eidx=%d }", chunkSize, startIndex, endIndex);
        }
    }
}
