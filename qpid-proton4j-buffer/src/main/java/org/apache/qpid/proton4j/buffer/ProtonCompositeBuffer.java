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
import java.util.ArrayList;

/**
 * A composite of 1 or more ProtonBuffer instances used when aggregating buffer views.
 */
public final class ProtonCompositeBuffer extends ProtonAbstractByteBuffer {

    // Tracking of the buffers contained within this buffer instance.
    protected int capacity;
    protected int currentIndex;
    protected final ArrayList<ProtonBuffer> buffers = new ArrayList<>();

    /**
     * @param maximumCapacity
     */
    protected ProtonCompositeBuffer(int maximumCapacity) {
        super(maximumCapacity);
    }

    public void addBuffer(ProtonBuffer newAddition) {
        capacity += newAddition.capacity();
        buffers.add(newAddition);
    }

    @Override
    public boolean hasArray() {
        if (buffers.size() == 1 && buffers.get(0).hasArray()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public byte[] getArray() {
        return buffers.get(0).getArray();
    }

    @Override
    public int getArrayOffset() {
        return buffers.get(0).getArrayOffset();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        checkNewCapacity(newCapacity);

        if (newCapacity > capacity) {
            // TODO - Add a new buffer that is the size of the needed capacity
            //        the write index remains where it is so it can advance now
            //        to the new capacity mark.
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
}
