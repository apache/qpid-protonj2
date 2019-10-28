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
public final class ProtonCompositeBuffer extends ProtonAbstractByteBuffer {

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
     * The current chunk that a read should start from which is advanced as reads consume chunks.
     */
    private Chunk currentChunk;

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
        this.tail = new Chunk(null, 0, -1, -1);

        this.head.next = tail;
        this.tail.prev = head;

        // We never allow current to be null, it is either at the bounds or on a valid chunk.
        this.currentChunk = head;
    }

    public ProtonCompositeBuffer addBuffer(ProtonBuffer newAddition) {
        final int readableBytes = newAddition.getReadableBytes();
        if (readableBytes == 0) {
            return this;
        }

        // We only read and write within the readable portion of the contained chunk so
        // our capacity follows the total readable bytes from all chunks.
        capacity += readableBytes;
        totalChunks++;

        final Chunk newChunk = new Chunk(newAddition, readableBytes, tail.prev.endIndex + 1, readableBytes - 1);

        // Link the new chunk onto the end updating any previous chunk as well.
        newChunk.prev = tail.prev;
        newChunk.next = tail;
        tail.prev.next = newChunk;
        tail.prev = newChunk;

        if (currentChunk == head) {
            currentChunk = newChunk;
        }

        return this;
    }

    @Override
    public boolean hasArray() {
        switch (totalChunks) {
            case 0:
                return true;
            case 1:
                return currentChunk.buffer.hasArray();
            default:
                return false;
        }
    }

    @Override
    public byte[] getArray() {
        if (hasArray()) {
            return currentChunk.buffer.getArray();
        }

        throw new UnsupportedOperationException("Buffer does not have a backing array.");
    }

    @Override
    public int getArrayOffset() {
        if (hasArray()) {
            return currentChunk.buffer.getArrayOffset();
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
            int amountNeeded = newCapacity - capacity;
            addBuffer(ProtonByteBufferAllocator.DEFAULT.allocate(amountNeeded, amountNeeded).setWriteIndex(amountNeeded));
        } else if (newCapacity < capacity) {
            // TODO - Reduce the buffers in the arrays until we get to one that
            //        can be copied into a smaller buffer that would meet the
            //        new capacity requirements.  The write index needs to be moved
            //        back to the new capacity value.
        }

        return this;
    }

    @Override
    public ProtonBuffer duplicate() {
        return new ProtonDuplicatedByteBuffer(this);
    }

    @Override
    public byte getByte(int index) {
        checkIndex(index, Byte.BYTES);
        currentChunk = findChunkWithIndex(index);
        return currentChunk.readByte(index);
    }

    @Override
    public short getShort(int index) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getInt(int index) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLong(int index) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int destinationIndex, int length) {
        checkDestinationIndex(index, length, destinationIndex, destination.capacity());
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        checkDestinationIndex(index, length, offset, length);
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        checkIndex(index, destination.remaining());
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        checkIndex(index, Byte.BYTES);
        currentChunk = findChunkWithIndex(index);
        currentChunk.writeByte(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, source.capacity());
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        checkSourceIndex(index, length, sourceIndex, length);
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        checkIndex(index, length);
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        checkIndex(index, length);

        final ProtonBuffer copy = ProtonByteBufferAllocator.DEFAULT.allocate(length);

        Chunk chunk = findChunkWithIndex(index);
        while (length > 0) {

        }

        // TODO Auto-generated method stub

        return copy;
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        switch (totalChunks) {
            case 0:
                return EMPTY_BYTE_BUFFER;
            case 1:
                return ByteBuffer.wrap(EMPTY_BYTE_ARRAY);  // TODO
            default:
                return ByteBuffer.wrap(EMPTY_BYTE_ARRAY);  // TODO
        }
    }

    private Chunk findChunkWithIndex(int index) {
        if (currentChunk == null) {
            return null;
        }

        Chunk result = currentChunk;

        if (index < result.startIndex) {
            while (result.prev != head) {
                result = result.prev;
                if (result.isInRange(index)) {
                    break;
                }
            }
        } else if (index > result.endIndex) {
            while (result.next != tail) {
                result = result.next;
                if (result.isInRange(index)) {
                    break;
                }
            }
        }

        return result;
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

        @Override
        public String toString() {
            return String.format("Chunk: { len=%d, sidx=%d, eidx=%d }", chunkSize, startIndex, endIndex);
        }
    }
}
