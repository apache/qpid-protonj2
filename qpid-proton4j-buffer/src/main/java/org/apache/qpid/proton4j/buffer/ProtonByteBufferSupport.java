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
 * Support for ProtonBuffer
 */
public class ProtonByteBufferSupport {

    /**
     * Wrapper class for ByteBuffer
     */
    public static class ProtonNIOByteBufferWrapper extends ProtonAbstractByteBuffer {

        final ByteBuffer buffer;

        public ProtonNIOByteBufferWrapper(ByteBuffer buffer) {
            super(buffer.capacity());
            this.buffer = buffer;
        }

        @Override
        public boolean hasArray() {
            return buffer.hasArray();
        }

        @Override
        public byte[] getArray() {
            return buffer.array();
        }

        @Override
        public int getArrayOffset() {
            return buffer.arrayOffset();
        }

        @Override
        public int capacity() {
            return buffer.capacity();
        }

        @Override
        public ProtonNIOByteBufferWrapper capacity(int newCapacity) {
            throw new UnsupportedOperationException("Cannot change capacity of ByteBuffer wrappers.");
        }

        @Override
        public ProtonNIOByteBufferWrapper duplicate() {
            return new ProtonByteBufferSupport.ProtonNIOByteBufferWrapper(buffer.duplicate());
        }

        @Override
        public ProtonNIOByteBufferWrapper copy() {
            return null;
        }

        @Override
        public ByteBuffer toByteBuffer() {
            return buffer;
        }

        @Override
        public byte getByte(int index) {
            return buffer.get(index);
        }

        @Override
        public short getShort(int index) {
            return buffer.getShort(index);
        }

        @Override
        public int getInt(int index) {
            return buffer.getInt(index);
        }

        @Override
        public long getLong(int index) {
            return buffer.getLong(index);
        }

        @Override
        public ProtonNIOByteBufferWrapper getBytes(int index, ProtonBuffer dst, int dstIndex, int length) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ProtonNIOByteBufferWrapper getBytes(int index, byte[] dst, int dstIndex, int length) {
            return null;
        }

        @Override
        public ProtonNIOByteBufferWrapper getBytes(int index, ByteBuffer dst) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ProtonNIOByteBufferWrapper setByte(int index, int value) {
            buffer.put(index, (byte) (value & 0xff));
            return this;
        }

        @Override
        public ProtonNIOByteBufferWrapper setShort(int index, int value) {
            buffer.putShort(index, (short) value);
            return this;
        }

        @Override
        public ProtonNIOByteBufferWrapper setInt(int index, int value) {
            buffer.putInt(index, value);
            return this;
        }

        @Override
        public ProtonNIOByteBufferWrapper setLong(int index, long value) {
            buffer.putLong(index, value);
            return this;
        }

        @Override
        public ProtonNIOByteBufferWrapper setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ProtonNIOByteBufferWrapper setBytes(int index, byte[] src, int srcIndex, int length) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ProtonNIOByteBufferWrapper setBytes(int index, ByteBuffer src) {
            // TODO Auto-generated method stub
            return null;
        }
    }
}
