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
import java.nio.charset.Charset;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Wrapper class for Netty ByteBuf instances
 */
public class ProtonNettyByteBuffer implements ProtonBuffer {

    private final ByteBuf wrapped;

    public ProtonNettyByteBuffer(ByteBuf toWrap) {
        this.wrapped = toWrap;
    }

    public ProtonNettyByteBuffer(int maximumCapacity) {
        wrapped = Unpooled.buffer(1024, maximumCapacity);
    }

    @Override
    public Object unwrap() {
        return wrapped;
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
    public ProtonBuffer clear() {
        wrapped.clear();
        return this;
    }

    @Override
    public int compareTo(ProtonBuffer other) {
        int length = getReadIndex() + Math.min(getReadableBytes(), other.getReadableBytes());

        for (int i = this.getReadIndex(), j = getReadIndex(); i < length; i++, j++) {
            int cmp = Integer.compare(getByte(i) & 0xFF, other.getByte(j) & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }

        return getReadableBytes() - other.getReadableBytes();
    }

    @Override
    public ProtonBuffer copy() {
        return new ProtonNettyByteBuffer(wrapped.copy());
    }

    @Override
    public ProtonBuffer copy(int index, int length) {
        return new ProtonNettyByteBuffer(wrapped.copy(index, length));
    }

    @Override
    public ProtonBuffer duplicate() {
        return new ProtonNettyByteBuffer(wrapped.duplicate());
    }

    @Override
    public ProtonBuffer ensureWritable(int minWritableBytes) throws IndexOutOfBoundsException, IllegalArgumentException {
        wrapped.ensureWritable(minWritableBytes);
        return this;
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
    public boolean getBoolean(int index) {
        return wrapped.getBoolean(index);
    }

    @Override
    public byte getByte(int index) {
        return wrapped.getByte(index);
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination) {
        wrapped.getBytes(index, destination);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ByteBuffer destination) {
        wrapped.getBytes(index, destination);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination) {
        int length = destination.getWritableBytes();
        getBytes(index, destination, 0, length);
        destination.setWriteIndex(destination.getWriteIndex() + length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int length) {
        return getBytes(index, destination, 0, length);
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer destination, int offset, int length) {
        if (destination.hasArray()) {
            wrapped.getBytes(index, destination.getArray(), destination.getArrayOffset() + offset, length);
        } else if (hasArray()) {
            destination.setBytes(offset, getArray(), getArrayOffset() + index, length);
        } else if (destination instanceof ProtonNettyByteBuffer) {
            ProtonNettyByteBuffer wrapper = (ProtonNettyByteBuffer) destination;
            wrapped.getBytes(index, (ByteBuf) wrapper.unwrap(), offset, length);
        } else {
            checkDestinationIndex(index, length, offset, destination.capacity());
            for (int i = 0; i < length; ++i) {
                destination.setByte(index + i, wrapped.getByte(offset + i));
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] destination, int offset, int length) {
        wrapped.getBytes(index, destination, offset, length);
        return this;
    }

    @Override
    public char getChar(int index) {
        return wrapped.getChar(index);
    }

    @Override
    public double getDouble(int index) {
        return wrapped.getDouble(index);
    }

    @Override
    public float getFloat(int index) {
        return wrapped.getFloat(index);
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
    public int getReadIndex() {
        return wrapped.readerIndex();
    }

    @Override
    public int getReadableBytes() {
        return wrapped.readableBytes();
    }

    @Override
    public short getShort(int index) {
        return wrapped.getShort(index);
    }

    @Override
    public short getUnsignedByte(int index) {
        return wrapped.getUnsignedByte(index);
    }

    @Override
    public long getUnsignedInt(int index) {
        return wrapped.getUnsignedInt(index);
    }

    @Override
    public int getUnsignedShort(int index) {
        return wrapped.getUnsignedShort(index);
    }

    @Override
    public int getWritableBytes() {
        return wrapped.writableBytes();
    }

    @Override
    public int getMaxWritableBytes() {
        return wrapped.maxWritableBytes();
    }

    @Override
    public int getWriteIndex() {
        return wrapped.writerIndex();
    }

    @Override
    public boolean hasArray() {
        return wrapped.hasArray();
    }

    @Override
    public boolean isReadable() {
        return wrapped.isReadable();
    }

    @Override
    public boolean isReadable(int minReadableBytes) {
        return wrapped.isReadable(minReadableBytes);
    }

    @Override
    public boolean isWritable() {
        return wrapped.isWritable();
    }

    @Override
    public boolean isWritable(int minWritableBytes) {
        return wrapped.isWritable(minWritableBytes);
    }

    @Override
    public ProtonBuffer markReadIndex() {
        wrapped.markReaderIndex();
        return this;
    }

    @Override
    public ProtonBuffer markWriteIndex() {
        wrapped.markWriterIndex();
        return this;
    }

    @Override
    public int maxCapacity() {
        return wrapped.maxCapacity();
    }

    @Override
    public boolean readBoolean() {
        return wrapped.readBoolean();
    }

    @Override
    public byte readByte() {
        return wrapped.readByte();
    }

    @Override
    public ProtonBuffer readBytes(byte[] destination) {
        wrapped.readBytes(destination);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ByteBuffer destination) {
        wrapped.readBytes(destination);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(byte[] destination, int length) {
        wrapped.readBytes(destination, 0, length);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(byte[] destination, int offset, int length) {
        wrapped.readBytes(destination, offset, length);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ProtonBuffer destination) {
        readBytes(destination, destination.getWritableBytes());
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ProtonBuffer destination, int length) {
        if (length > destination.getWritableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                "length(%d) exceeds target Writable Bytes:(%d), target is: %s", length, destination.getWritableBytes(), destination));
        }
        readBytes(destination, destination.getWriteIndex(), length);
        destination.setWriteIndex(destination.getWriteIndex() + length);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ProtonBuffer destination, int offset, int length) {
        checkReadableBytes(length);
        getBytes(wrapped.readerIndex(), destination, offset, length);
        wrapped.skipBytes(length);
        return this;
    }

    @Override
    public double readDouble() {
        return wrapped.readDouble();
    }

    @Override
    public float readFloat() {
        return wrapped.readFloat();
    }

    @Override
    public int readInt() {
        return wrapped.readInt();
    }

    @Override
    public long readLong() {
        return wrapped.readLong();
    }

    @Override
    public short readShort() {
        return wrapped.readShort();
    }

    @Override
    public ProtonBuffer resetReadIndex() {
        wrapped.resetReaderIndex();
        return this;
    }

    @Override
    public ProtonBuffer resetWriteIndex() {
        wrapped.resetWriterIndex();
        return this;
    }

    @Override
    public ProtonBuffer setBoolean(int index, boolean value) {
        wrapped.setBoolean(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setByte(int index, int value) {
        wrapped.setByte(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] value) {
        wrapped.setBytes(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ByteBuffer value) {
        wrapped.setBytes(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer value) {
        return setBytes(index, value, value.getReadableBytes());
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer value, int length) {
        checkIndex(index, length);
        if (value == null) {
            throw new NullPointerException("src");
        }
        if (length > value.getReadableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                "length(%d) exceeds source buffer Readable Bytes(%d), source is: %s", length, value.getReadableBytes(), value));
        }

        setBytes(index, value, value.getReadIndex(), length);
        value.setReadIndex(value.getReadIndex() + length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer value, int offset, int length) {
        if (value instanceof ProtonNettyByteBuffer) {
            wrapped.setBytes(index, (ByteBuf) value.unwrap(), offset, length);
        } else if (value.hasArray()) {
            wrapped.setBytes(index, value.getArray(), value.getArrayOffset() + offset, length);
        } else if (hasArray()) {
            value.getBytes(offset, getArray(), getArrayOffset() + index, length);
        } else {
            checkSourceIndex(index, length, offset, value.capacity());
            for (int i = 0; i < length; ++i) {
                wrapped.setByte(index + i, value.getByte(offset + i));
            }
        }

        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] value, int offset, int length) {
        wrapped.setBytes(index, value, offset, length);
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, int value) {
        wrapped.setChar(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setDouble(int index, double value) {
        wrapped.setDouble(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setFloat(int index, float value) {
        wrapped.setFloat(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setIndex(int readIndex, int writeIndex) {
        wrapped.setIndex(readIndex, writeIndex);
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
    public ProtonBuffer setReadIndex(int index) {
        wrapped.readerIndex(index);
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, int value) {
        wrapped.setShort(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setWriteIndex(int index) {
        wrapped.writerIndex(index);
        return this;
    }

    @Override
    public ProtonBuffer skipBytes(int skippedBytes) {
        wrapped.skipBytes(skippedBytes);
        return this;
    }

    @Override
    public ProtonBuffer slice() {
        return new ProtonNettyByteBuffer(wrapped.slice());
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        return new ProtonNettyByteBuffer(wrapped.slice(index, length));
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return wrapped.nioBuffer();
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        return wrapped.nioBuffer(index, length);
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }

    @Override
    public String toString(Charset charset) {
        return wrapped.toString(charset);
    }

    @Override
    public ProtonBuffer writeBoolean(boolean value) {
        wrapped.writeBoolean(value);
        return this;
    }

    @Override
    public ProtonBuffer writeByte(int value) {
        wrapped.writeByte(value);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ByteBuffer value) {
        wrapped.writeBytes(value);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(byte[] value) {
        wrapped.writeBytes(value);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(byte[] value, int length) {
        wrapped.writeBytes(value, 0, length);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(byte[] array, int offset, int length) {
        wrapped.writeBytes(array, offset, length);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer value) {
        return writeBytes(value, value.getReadableBytes());
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer value, int length) {
        if (length > value.getReadableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                "length(%d) exceeds source Readable Bytes(%d), source is: %s", length, value.getReadableBytes(), value));
        }
        writeBytes(value, value.getReadIndex(), length);
        value.skipBytes(length);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer value, int offset, int length) {
        ensureWritable(length);
        setBytes(wrapped.writerIndex(), value, offset, length);
        wrapped.writerIndex(wrapped.writerIndex() + length);
        return this;
    }

    @Override
    public ProtonBuffer writeDouble(double value) {
        wrapped.writeDouble(value);
        return this;
    }

    @Override
    public ProtonBuffer writeFloat(float value) {
        wrapped.writeFloat(value);
        return this;
    }

    @Override
    public ProtonBuffer writeInt(int value) {
        wrapped.writeInt(value);
        return this;
    }

    @Override
    public ProtonBuffer writeLong(long value) {
        wrapped.writeLong(value);
        return this;
    }

    @Override
    public ProtonBuffer writeShort(short value) {
        wrapped.writeShort(value);
        return this;
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProtonBuffer)) {
            return false;
        }

        ProtonBuffer that = (ProtonBuffer) other;
        if (this.getReadableBytes() != that.getReadableBytes()) {
            return false;
        }

        int index = getReadIndex();
        for (int i = getReadableBytes() - 1, j = that.getReadableBytes() - 1; i >= index; i--, j--) {
            if (!(getByte(i) == that.getByte(j))) {
                return false;
            }
        }

        return true;
    }

    //----- Internal Bounds Checking Utilities

    protected final void checkReadableBytes(int minimumReadableBytes) {
        if (minimumReadableBytes < 0) {
            throw new IllegalArgumentException("minimumReadableBytes: " + minimumReadableBytes + " (expected: >= 0)");
        }

        internalCheckReadableBytes(minimumReadableBytes);
    }

    private void internalCheckReadableBytes(int minimumReadableBytes) {
        // Called when we know that we don't need to validate if the minimum readable
        // value is negative.
        if (wrapped.readerIndex() > wrapped.writerIndex() - minimumReadableBytes) {
            throw new IndexOutOfBoundsException(String.format(
                "readIndex(%d) + length(%d) exceeds writeIndex(%d): %s",
                wrapped.readerIndex(), minimumReadableBytes, wrapped.writerIndex(), this));
        }
    }

    protected static boolean isOutOfBounds(int index, int length, int capacity) {
        return (index | length | (index + length) | (capacity - (index + length))) < 0;
    }

    protected final void checkIndex(int index, int fieldLength) {
        if (isOutOfBounds(index, fieldLength, capacity())) {
            throw new IndexOutOfBoundsException(String.format(
                "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
        }
    }

    protected final void checkSourceIndex(int index, int length, int srcIndex, int srcCapacity) {
        checkIndex(index, length);
        if (isOutOfBounds(srcIndex, length, srcCapacity)) {
            throw new IndexOutOfBoundsException(String.format(
                "srcIndex: %d, length: %d (expected: range(0, %d))", srcIndex, length, srcCapacity));
        }
    }

    protected final void checkDestinationIndex(int index, int length, int dstIndex, int dstCapacity) {
        checkIndex(index, length);
        if (isOutOfBounds(dstIndex, length, dstCapacity)) {
            throw new IndexOutOfBoundsException(String.format(
                "dstIndex: %d, length: %d (expected: range(0, %d))", dstIndex, length, dstCapacity));
        }
    }
}
