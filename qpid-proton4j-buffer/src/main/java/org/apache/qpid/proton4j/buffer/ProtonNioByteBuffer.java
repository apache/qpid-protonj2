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
 * ProtonBuffer wrapper around a NIO ByteBuffer instance.
 *
 * TODO - Decide if we want to implement this, many limitations and issues
 *        around this as buffer can be read-only etc.
 */
public class ProtonNioByteBuffer extends ProtonAbstractByteBuffer {

    private final ByteBuffer buffer;

    protected ProtonNioByteBuffer(ByteBuffer buffer) {
        super(buffer.remaining());

        this.buffer = buffer.slice();
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
        return buffer.remaining();
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        throw new UnsupportedOperationException("NIO Buffer wrapper cannot adjust capacity");
    }

    @Override
    public ProtonBuffer duplicate() {
        return null;
    }

    @Override
    public byte getByte(int index) {
        return 0;
    }

    @Override
    public short getShort(int index) {
        return 0;
    }

    @Override
    public int getInt(int index) {
        return 0;
    }

    @Override
    public long getLong(int index) {
        return 0;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer dst, int dstIndex, int length) {
        return null;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] target, int offset, int length) {
        return null;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        return null;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        return null;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        return null;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        return null;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        return null;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int sourceIndex, int length) {
        return null;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] src, int srcIndex, int length) {
        return null;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer src) {
        return null;
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        return null;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        return null;
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        return null;
    }
}
