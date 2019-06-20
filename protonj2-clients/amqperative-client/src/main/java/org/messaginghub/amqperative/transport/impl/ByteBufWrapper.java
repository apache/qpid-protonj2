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
package org.messaginghub.amqperative.transport.impl;

import java.nio.ByteBuffer;

import org.apache.qpid.proton4j.buffer.ProtonAbstractByteBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Wrapper class for Netty ByteBuf instances
 */
public class ByteBufWrapper extends ProtonAbstractByteBuffer {

    private final ByteBuf wrapped;

    public ByteBufWrapper(ByteBuf toWrap) {
        super(toWrap.maxCapacity());
        this.wrapped = toWrap;
    }

    public ByteBufWrapper(int maximumCapacity) {
        super(maximumCapacity);

        wrapped = Unpooled.buffer(1024, maximumCapacity);
    }

    @Override
    public int capacity() {
        return wrapped.capacity();
    }

    @Override
    public ProtonBuffer capacity(int newCapacity) {
        wrapped.capacity(newCapacity);
        return this;
    }

    @Override
    public ProtonBuffer duplicate() {
        return new ByteBufWrapper(wrapped.duplicate());
    }

    @Override
    public byte[] getArray() {
        return wrapped.array();
    }

    @Override
    public int getArrayOffset() {
        return wrapped.arrayOffset();
    }

    @Override
    public byte getByte(int index) {
        return wrapped.getByte(index);
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        wrapped.getBytes(index, destination);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int offset, int length) {
        // TODO - Optimize get as much as possible before single byte copy.
        return null;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        wrapped.getBytes(index, destination, offset, length);
        return this;
    }

    @Override
    public int getInt(int index) {
        return wrapped.getInt(index);
    }

    @Override
    public long getLong(int index) {
        return wrapped.getLong(index);
    }

    @Override
    public short getShort(int index) {
        return wrapped.getShort(index);
    }

    @Override
    public boolean hasArray() {
        return wrapped.hasArray();
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        wrapped.setByte(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer value) {
        wrapped.setBytes(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer value, int offset, int length) {
        // TODO - Optimize put as much as possible before single byte copy.
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] value, int offset, int length) {
        wrapped.setBytes(index, value, offset, length);
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        wrapped.setInt(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        wrapped.setLong(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        wrapped.setShort(index, value);
        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        return new ByteBufWrapper(wrapped.copy(index, length));
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        return new ByteBufWrapper(wrapped.slice(index, length));
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        return wrapped.nioBuffer(index, length);
    }
}
