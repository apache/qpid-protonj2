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
package org.apache.qpid.protonj2.buffer.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.ProtonBufferIterator;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.resource.SharedResource;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.buffer.BufferClosedException;
import io.netty5.buffer.BufferComponent;
import io.netty5.buffer.BufferReadOnlyException;
import io.netty5.buffer.ByteCursor;
import io.netty5.buffer.ComponentIterator;
import io.netty5.buffer.ComponentIterator.Next;

/**
 * Wrapper class for Netty 5 Buffer instances which provides a generic way
 * for proton to interact with Netty 4 buffers.
 */
public final class Netty5ToProtonBufferAdapter extends SharedResource<ProtonBuffer>
    implements ProtonBuffer, ProtonBufferComponentAccessor, ProtonBufferComponent {

    private final Netty5ProtonBufferAllocator allocator;

    private static final Buffer CLOSED_BUFFER;

    static {
        CLOSED_BUFFER = BufferAllocator.onHeapUnpooled().allocate(0);
        CLOSED_BUFFER.close();
    }

    private Buffer resource;
    private BufferComponent resourceComponent;

    /**
     * Creates a new {@link Netty5ToProtonBufferAdapter} which wraps the given Netty {@link Buffer}.
     *
     * @param allocator
     *      The allocator that created this buffer wrapper
     * @param resource
     * 		The {@link Buffer} resource to wrap.
     */
    public Netty5ToProtonBufferAdapter(Netty5ProtonBufferAllocator allocator, Buffer resource) {
        this.resource = resource;
        this.allocator = allocator;

        // To avoid allocation of component iterators we can check that the buffer
        // is a single component buffer and it implements the component interface as
        // Netty optimizes this case to also avoid allocations.
        if (resource.countComponents() == 1 && resource instanceof BufferComponent) {
            resourceComponent = (BufferComponent) resource;
        }
    }

    public Netty5ProtonBufferAllocator allocator() {
        return allocator;
    }

    /**
     * Unwraps the managed Netty {@link Buffer} and releases it from ownership by this
     * {@link ProtonBuffer} instance.  This effectively closes the {@link ProtonBuffer}
     * while also safely handing off the managed Netty resource.
     *
     * @return the managed Netty {@link Buffer}
     */
    public Buffer unwrapAndRelease() {
        if (resource != CLOSED_BUFFER) {
            try {
                return resource;
            } finally {
                resource = CLOSED_BUFFER;
            }
        } else {
            throw new ProtonBufferClosedException("The buffer has already been closed or transferred");
        }
    }

    @Override
    public Buffer unwrap() {
        ProtonBufferUtils.checkIsClosed(this);
        return resource;
    }

    @Override
    public ProtonBuffer convertToReadOnly() {
        resource.makeReadOnly();
        return this;
    }

    @Override
    public boolean isReadOnly() {
        return resource.readOnly();
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    @Override
    public int componentCount() {
        return resource.countComponents();
    }

    @Override
    public int readableComponentCount() {
        return resource.countReadableComponents();
    }

    @Override
    public int writableComponentCount() {
        return resource.countWritableComponents();
    }

    @Override
    public boolean isDirect() {
        return false; // Buffer components APIs need to allow access to native addresses
    }

    @Override
    public int implicitGrowthLimit() {
        return resource.implicitCapacityLimit();
    }

    @Override
    public ProtonBuffer implicitGrowthLimit(int limit) {
        resource.implicitCapacityLimit(limit);
        return this;
    }

    @Override
    public ProtonBuffer fill(byte value) {
        try {
            resource.fill(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public int capacity() {
        return resource.capacity();
    }

    @Override
    public int getReadOffset() {
        return resource.readerOffset();
    }

    @Override
    public ProtonBuffer setReadOffset(int value) {
        resource.readerOffset(value);
        return this;
    }

    @Override
    public int getWriteOffset() {
        return resource.writerOffset();
    }

    @Override
    public ProtonBuffer setWriteOffset(int value) {
        try {
            resource.writerOffset(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer compact() {
        try {
            resource.compact();
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public void copyInto(int offset, byte[] destination, int destOffset, int length) {
        try {
            resource.copyInto(offset, destination, destOffset, length);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public void copyInto(int offset, ByteBuffer destination, int destOffset, int length) {
        try {
            resource.copyInto(offset, destination, destOffset, length);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public void copyInto(int offset, ProtonBuffer destination, int destOffset, int length) {
        try {
            if (destination.unwrap() instanceof Buffer) {
                resource.copyInto(offset, (Buffer) destination.unwrap(), destOffset, length);
            } else {
                ProtonBufferUtils.checkIsReadOnly(destination);
                ProtonBufferUtils.checkIsClosed(this);

                // Try to reduce bounds-checking by using larger primitives when possible.
                for (; length >= Long.BYTES; length -= Long.BYTES, offset += Long.BYTES, destOffset += Long.BYTES) {
                    destination.setLong(destOffset, getLong(offset));
                }
                for (; length >= Integer.BYTES; length -= Integer.BYTES, offset += Integer.BYTES, destOffset += Integer.BYTES) {
                    destination.setInt(destOffset, getInt(offset));
                }
                for (; length > 0; length--, offset++, destOffset++) {
                    destination.setByte(destOffset, getByte(offset));
                }
            }
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public ProtonBuffer writeBytes(byte[] source, int offset, int length) {
        try {
            resource.writeBytes(source, offset, length);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }

        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ByteBuffer source) {
        try {
            resource.writeBytes(source);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }

        return this;
    }

    @Override
    public ProtonBuffer ensureWritable(int size, int minimumGrowth, boolean allowCompaction) throws IndexOutOfBoundsException, IllegalArgumentException {
        try {
            resource.ensureWritable(size, minimumGrowth, allowCompaction);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }

        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length, boolean readOnly) throws IllegalArgumentException {
        try {
            return allocator.wrap(resource.copy(index, length, readOnly));
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public ProtonBuffer split(int splitOffset) {
        try {
            return allocator.wrap(resource.split(splitOffset));
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    //----- JDK Overrides

    @Override
    public String toString(Charset charset) {
        return resource.toString(charset);
    }

    @Override
    public String toString() {
        return "Netty5ToProtonBufferAdapter" +
               "{ read:" + (resource != null ? resource.readerOffset() : null) +
               ", write: " + (resource != null ? resource.writerOffset() : 0) +
               ", capacity: " + (resource != null ? resource.capacity() : 0) + "}";
    }

    @Override
    public int compareTo(ProtonBuffer buffer) {
        return ProtonBufferUtils.compare(this, buffer);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ProtonBuffer) {
            return ProtonBufferUtils.equals(this, (ProtonBuffer) other);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return ProtonBufferUtils.hashCode(this);
    }

    //----- Primitive Get API

    @Override
    public byte getByte(int index) {
        try {
            return resource.getByte(index);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public char getChar(int index) {
        try {
            return resource.getChar(index);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public short getShort(int index) {
        try {
            return resource.getShort(index);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public int getInt(int index) {
        try {
            return resource.getInt(index);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public long getLong(int index) {
        try {
            return resource.getLong(index);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    //----- Primitive Set API

    @Override
    public ProtonBuffer setByte(int index, byte value) {
        try {
            resource.setByte(index, value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, char value) {
        try {
            resource.setChar(index, value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, short value) {
        try {
            resource.setShort(index, value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        try {
            resource.setInt(index, value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        try {
            resource.setLong(index, value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    //----- Primitive Read API

    @Override
    public byte readByte() {
        try {
            return resource.readByte();
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public char readChar() {
        try {
            return resource.readChar();
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return resource.readShort();
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return resource.readInt();
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return resource.readLong();
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    //----- Primitive Write API

    @Override
    public ProtonBuffer writeByte(byte value) {
        try {
            resource.writeByte(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeChar(char value) {
        try {
            resource.writeChar(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeShort(short value) {
        try {
            resource.writeShort(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeInt(int value) {
        try {
            resource.writeInt(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeLong(long value) {
        try {
            resource.writeLong(value);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
        return this;
    }

    //----- IO Handlers

    @Override
    public int transferTo(WritableByteChannel channel, int length) throws IOException {
        try {
            return resource.transferTo(channel, length);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public int transferFrom(ReadableByteChannel channel, int length) throws IOException {
        try {
            return resource.transferFrom(channel, length);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    @Override
    public int transferFrom(FileChannel channel, long position, int length) throws IOException {
        try {
            return resource.transferFrom(channel, position, length);
        } catch (RuntimeException e) {
            throw translateToProtonException(e);
        }
    }

    //----- Buff component access

    @Override
    public ProtonBufferComponentAccessor componentAccessor() {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        if (resourceComponent != null) {
            return (ProtonBufferComponentAccessor) acquire();
        } else {
            return new ProtonNetty5BufferComponentAccessor((Netty5ToProtonBufferAdapter) acquire(), resource.forEachComponent());
        }
    }

    @Override
    public ProtonBufferComponent first() {
        return this;
    }

    @Override
    public ProtonBufferComponent next() {
        return null;
    }

    //----- Buffer Component API that works only when there is one component in the netty buffer

    @Override
    public int getReadableBytes() {
        return resourceComponent.readableBytes();
    }

    @Override
    public boolean hasReadbleArray() {
        return resourceComponent.hasReadableArray();
    }

    @Override
    public Netty5ToProtonBufferAdapter advanceReadOffset(int amount) {
        return (Netty5ToProtonBufferAdapter) ProtonBuffer.super.advanceReadOffset(amount);
    }

    @Override
    public byte[] getReadableArray() {
        return resourceComponent.readableArray();
    }

    @Override
    public int getReadableArrayOffset() {
        return resourceComponent.readableArrayOffset();
    }

    @Override
    public int getReadableArrayLength() {
        return resourceComponent.readableArrayLength();
    }

    @Override
    public ByteBuffer getReadableBuffer() {
        return resourceComponent.readableBuffer();
    }

    @Override
    public int getWritableBytes() {
        return resourceComponent.writableBytes();
    }

    @Override
    public Netty5ToProtonBufferAdapter advanceWriteOffset(int amount) {
        return (Netty5ToProtonBufferAdapter) ProtonBuffer.super.advanceWriteOffset(amount);
    }

    @Override
    public boolean hasWritableArray() {
        return resourceComponent.hasWritableArray();
    }

    @Override
    public byte[] getWritableArray() {
        return resourceComponent.writableArray();
    }

    @Override
    public int getWritableArrayOffset() {
        return resourceComponent.writableArrayOffset();
    }

    @Override
    public int getWritableArrayLength() {
        return resourceComponent.writableArrayLength();
    }

    @Override
    public ByteBuffer getWritableBuffer() {
        return resourceComponent.writableBuffer();
    }

    @Override
    public long getNativeAddress() {
        return resourceComponent.baseNativeAddress();
    }

    @Override
    public long getNativeReadAddress() {
        return resourceComponent.readableNativeAddress();
    }

    @Override
    public long getNativeWriteAddress() {
        return resourceComponent.writableNativeAddress();
    }

    //----- Buffer Iteration API

    @Override
    public ProtonBufferIterator bufferIterator() {
        return bufferIterator(getReadOffset(), getReadableBytes());
    }

    @Override
    public ProtonBufferIterator bufferIterator(int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "length");
        ProtonBufferUtils.checkIsNotNegative(offset, "length");

        if (offset + length > resource.capacity()) {
            throw new IndexOutOfBoundsException(
                "The iterator cannot read beyond the bounds of the buffer: offset=" + offset + ", length=" + length);
        }

        return new Netty5ToProtonBufferReverseIterator(resource.openCursor(offset, length));
    }

    @Override
    public ProtonBufferIterator bufferReverseIterator(int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "length");
        ProtonBufferUtils.checkIsNotNegative(offset, "length");

        if (offset >= capacity()) {
            throw new IndexOutOfBoundsException(
                "Read offset must be within the bounds of the buffer: offset = " + offset + ", capacity = " + capacity());
        }

        if (offset - length < -1) {
            throw new IndexOutOfBoundsException(
                "Cannot read past start of buffer: offset = " + offset + ", length = " + length);
        }

        return new Netty5ToProtonBufferReverseIterator(resource.openReverseCursor(offset, length));
    }

    private static final class Netty5ToProtonBufferReverseIterator implements ProtonBufferIterator {

        private final ByteCursor cursor;

        public Netty5ToProtonBufferReverseIterator(ByteCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.bytesLeft() > 0;
        }

        @Override
        public byte next() {
            if (!cursor.readByte()) {
                throw new NoSuchElementException("Cannot read outside the iterator bounds");
            }

            return cursor.getByte();
        }

        @Override
        public int remaining() {
            return cursor.bytesLeft();
        }

        @Override
        public int offset() {
            return cursor.currentOffset();
        }
    }

    //----- Buffer search API

    @Override
    public int indexOf(byte needle, int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);

        if (offset < getReadOffset() || getWriteOffset() < offset + length) {
            throw new IndexOutOfBoundsException("Cannot search past the readable bounds of this buffer");
        }

        final int count = resource.bytesBefore(needle);
        if (count < 0) {
            return count;
        }

        final int indexOf = offset + count;

        if (offset > indexOf || offset + length < indexOf) {
            return -1;
        }

        return indexOf;
    }

    //----- Shared resource API

    @Override
    protected void releaseResourceOwnership() {
        if (resource != null && resource.isAccessible()) {
            resource.close();
            resource = CLOSED_BUFFER;
        }
    }

    @Override
    protected ProtonBuffer transferTheResource() {
        final Buffer transferred = resource;
        resource = CLOSED_BUFFER;

        return new Netty5ToProtonBufferAdapter(allocator, transferred);
    }

    @Override
    protected RuntimeException resourceIsClosedException() {
        return ProtonBufferUtils.genericBufferIsClosed(this);
    }

    //----- Support API for the buffer wrapper

    private RuntimeException translateToProtonException(RuntimeException e) {
        RuntimeException result = e;

        if (e instanceof BufferReadOnlyException) {
            result = ProtonBufferUtils.genericBufferIsReadOnly(this);
            result.addSuppressed(e);
        } else if (e instanceof BufferClosedException) {
            result = ProtonBufferUtils.genericBufferIsClosed(this);
            result.addSuppressed(e);
        }

        return result;
    }

    @SuppressWarnings("rawtypes")
    private static final class ProtonNetty5BufferComponentAccessor implements ProtonBufferComponentAccessor, ProtonBufferComponent {

        private final Netty5ToProtonBufferAdapter adapter;

        private final ComponentIterator resourceIterator;

        private Next current;
        private BufferComponent currentComponent;

        public ProtonNetty5BufferComponentAccessor(Netty5ToProtonBufferAdapter adapter, ComponentIterator iterator) {
            this.adapter = adapter;
            this.resourceIterator = iterator;
        }

        @Override
        public void close() {
            current = null;
            currentComponent = null;

            resourceIterator.close();
            adapter.close();
        }

        @Override
        public ProtonBufferComponent first() {
            current = resourceIterator.first();
            currentComponent = (BufferComponent) current;

            return this;
        }

        @Override
        public ProtonBufferComponent next() {
            if (current != null) {
                current = current.next();
                currentComponent = (BufferComponent) current;
            }

            return current != null ? this : null;
        }

        @Override
        public Object unwrap() {
            return adapter.resource;
        }

        @Override
        public int getReadableBytes() {
            return currentComponent.readableBytes();
        }

        @Override
        public boolean hasReadbleArray() {
            return currentComponent.hasReadableArray();
        }

        @Override
        public ProtonBufferComponent advanceReadOffset(int amount) {
            currentComponent.skipReadableBytes(amount);
            return this;
        }

        @Override
        public byte[] getReadableArray() {
            return currentComponent.readableArray();
        }

        @Override
        public int getReadableArrayOffset() {
            return currentComponent.readableArrayOffset();
        }

        @Override
        public int getReadableArrayLength() {
            return currentComponent.readableArrayLength();
        }

        @Override
        public ByteBuffer getReadableBuffer() {
            return currentComponent.readableBuffer();
        }

        @Override
        public int getWritableBytes() {
            return currentComponent.writableBytes();
        }

        @Override
        public ProtonBufferComponent advanceWriteOffset(int amount) {
            currentComponent.skipWritableBytes(amount);
            return this;
        }

        @Override
        public boolean hasWritableArray() {
            return currentComponent.hasWritableArray();
        }

        @Override
        public byte[] getWritableArray() {
            return currentComponent.writableArray();
        }

        @Override
        public int getWritableArrayOffset() {
            return currentComponent.writableArrayOffset();
        }

        @Override
        public int getWritableArrayLength() {
            return currentComponent.writableArrayLength();
        }

        @Override
        public ByteBuffer getWritableBuffer() {
            return currentComponent.writableBuffer();
        }

        @Override
        public long getNativeAddress() {
            return currentComponent.baseNativeAddress();
        }

        @Override
        public long getNativeReadAddress() {
            return currentComponent.readableNativeAddress();
        }

        @Override
        public long getNativeWriteAddress() {
            return currentComponent.writableNativeAddress();
        }

        @Override
        public ProtonBufferIterator bufferIterator() {
            return new Netty5ToProtonBufferIterator(currentComponent.openCursor());
        }

        private final class Netty5ToProtonBufferIterator implements ProtonBufferIterator {

            private final ByteCursor cursor;

            public Netty5ToProtonBufferIterator(ByteCursor cursor) {
                this.cursor = cursor;
            }

            @Override
            public boolean hasNext() {
                return cursor.bytesLeft() > 0;
            }

            @Override
            public byte next() {
                if (cursor.readByte()) {
                    throw new NoSuchElementException("Cannot read outside the iterator bounds");
                }

                return cursor.getByte();
            }

            @Override
            public int remaining() {
                return cursor.bytesLeft();
            }

            @Override
            public int offset() {
                return cursor.currentOffset();
            }
        }
    }
}
