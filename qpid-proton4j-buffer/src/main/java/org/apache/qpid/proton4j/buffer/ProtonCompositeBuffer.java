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
    protected ProtonCompositeBuffer(int maximumCapacity) {
        super(maximumCapacity);

        this.head = new Chunk(null);
        this.tail = new Chunk(null);

        this.head.next(tail);
        this.tail.prev(head);
    }

    public ProtonCompositeBuffer addBuffer(ProtonBuffer newAddition) {
        if (!newAddition.isReadable()) {
            return this;
        }

        capacity += newAddition.capacity();
        totalChunks++;

        Chunk newChunk = new Chunk(newAddition);

        // New node linked into end of chain
        newChunk.prev(tail.prev());
        newChunk.next(tail);

        // Preview end of chain now linked to new end.
        tail.prev().next(newChunk);
        tail.prev(newChunk);

        // First chunk now so reset the group data.
        if (currentChunk == null) {
            currentChunk = newChunk;
        }

        return this;
    }

    @Override
    public boolean hasArray() {
        if (totalChunks == 1 && currentChunk.buffer.hasArray()) {
            return true;
        } else {
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
            addBuffer(ProtonByteBufferAllocator.DEFAULT.allocate(amountNeeded, amountNeeded));
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte getByte(int index) {
        // TODO Auto-generated method stub
        return 0;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] source, int sourceIndex, int length) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer source) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        // TODO Auto-generated method stub
        return null;
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

        public Chunk(ProtonBuffer buffer) {
            this.buffer = buffer;
            this.chunkSize = buffer != null ? buffer.getReadableBytes() : 0;

            // TODO - Future track what indices this chunk contains
            this.startIndex = 0;
            this.endIndex = 0;
        }

        public int size() {
            return chunkSize;
        }

        public Chunk next() {
            return next;
        }

        public Chunk next(Chunk next) {
            this.next = next;
            return this;
        }

        public Chunk prev() {
            return prev;
        }

        public Chunk prev(Chunk prev) {
            this.prev = prev;
            return this;
        }

        public boolean isInRange(int index) {
            if (index >= startIndex && index <= endIndex) {
                return true;
            } else {
                return false;
            }
        }
    }
}
