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
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.ProtonBufferIterator;
import org.apache.qpid.protonj2.buffer.ProtonBufferReadOnlyException;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.resource.SharedResource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;

/**
 * Wrapper class for Netty 4 ByteBuf instances which provides a generic way
 * for proton to interact with Netty 4 buffers.
 */
public final class Netty4ToProtonBufferAdapter extends SharedResource<ProtonBuffer>
    implements ProtonBuffer, ProtonBufferComponentAccessor, ProtonBufferComponent {

    private final Netty4ProtonBufferAllocator allocator;

    private static final int CLOSED_MARKER = -1;

    /**
     * The netty ByteBuf that this adapter manages
     */
    private ByteBuf resource;

    /**
     * Track if closed directly
     */
    private boolean closed;

    /**
     * Track state of wrapped buffer
     */
    private boolean readOnly;

    /**
     * Tracks the readable capacity of this buffer.  This value is calculated based on
     * the given target array and the offset value at create time plus any create time
     * restriction that limits the usable sub-region of the array to less than the total
     * length minus the offset
     */
    private int readCapacity;

    /**
     * Tracks the read capacity for buffers that are both readable and writable and is
     * set to the closed state for a buffer than has been made read only.
     */
    private int writeCapacity;

    /**
     * The read offset for this buffer
     */
    private int readOffset;

    /**
     * The write offset for this buffer
     */
    private int writeOffset;

    /**
     * Configured implicit growth limit for this netty 4 buffer wrapper
     */
    private int implicitGrowthLimit = ProtonBufferUtils.MAX_BUFFER_CAPACITY;

    /**
     * Creates a new {@link Netty4ToProtonBufferAdapter} which wraps the given Netty {@link ByteBuf}.
     *
     * @param allocator
     *      The {@link ProtonBufferAllocator} that created this instance
     * @param resource
     * 		The {@link ByteBuf} resource to wrap.
     */
    public Netty4ToProtonBufferAdapter(Netty4ProtonBufferAllocator allocator, ByteBuf resource) {
        this(allocator, resource, Objects.requireNonNull(resource, "The provided ByteBuf instance cannot be null").isReadOnly());
    }

    private Netty4ToProtonBufferAdapter(Netty4ProtonBufferAllocator allocator, ByteBuf resource, boolean readOnly) {
        this.allocator = allocator;
        this.resource = resource;
        this.readOnly = readOnly;
        this.readCapacity = resource.capacity();
        this.writeCapacity = readOnly ? CLOSED_MARKER : readCapacity;
        this.readOffset = resource.readerIndex();
        this.writeOffset = resource.writerIndex();
    }

    public Netty4ProtonBufferAllocator allocator() {
        return allocator;
    }

    /**
     * Unwraps the managed Netty {@link ByteBuf} and releases it from ownership by this
     * {@link ProtonBuffer} instance.  This effectively closes the {@link ProtonBuffer}
     * while also safely handing off the managed Netty resource.
     *
     * @return the managed Netty {@link ByteBuf}
     */
    public ByteBuf unwrapAndRelease() {
        if (!closed) {
            try {
                return resource.setIndex(readOffset, writeOffset);
            } finally {
                resource = Unpooled.EMPTY_BUFFER;
            }
        } else {
            throw new ProtonBufferClosedException("The buffer has already been closed or transferred");
        }
    }

    @Override
    public ByteBuf unwrap() {
        ProtonBufferUtils.checkIsClosed(this);
        return resource.setIndex(readOffset, writeOffset);
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    @Override
    public int componentCount() {
        return 1;
    }

    @Override
    public int readableComponentCount() {
        return isReadable() ? 1 : 0;
    }

    @Override
    public int writableComponentCount() {
        return isWritable() ? 1 : 0;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean isDirect() {
        return false; // We don't have access to all ByteBuf internals to get at native addresses
    }

    @Override
    public ProtonBuffer convertToReadOnly() {
        readOnly = true;
        writeCapacity = CLOSED_MARKER;

        return this;
    }

    @Override
    public int getReadOffset() {
        return readOffset;
    }

    @Override
    public ProtonBuffer setReadOffset(int value) {
        checkRead(value, 0);
        readOffset = value;
        return this;
    }

    @Override
    public int getReadableBytes() {
        return writeOffset - readOffset;
    }

    @Override
    public int getWriteOffset() {
        return writeOffset;
    }

    @Override
    public ProtonBuffer setWriteOffset(int value) {
        checkWrite(value, 0, false);
        writeOffset = value;
        return this;
    }

    @Override
    public int getWritableBytes() {
        return Math.max(0, writeCapacity - writeOffset);
    }

    @Override
    public int capacity() {
        return Math.max(0, readCapacity);
    }

    @Override
    public int implicitGrowthLimit() {
        return implicitGrowthLimit;
    }

    @Override
    public ProtonBuffer implicitGrowthLimit(int limit) {
        ProtonBufferUtils.checkImplicitGrowthLimit(limit, capacity());
        this.implicitGrowthLimit = limit;
        return this;
    }

    @Override
    public ProtonBuffer fill(byte value) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsReadOnly(this);

        try {
            if (resource.hasArray()) {
                Arrays.fill(resource.array(), resource.arrayOffset(), resource.arrayOffset() + resource.capacity(), value);
            } else {
                for (int i = 0; i < resource.capacity(); ++i) {
                    resource.setByte(i, value);
                }
            }
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer compact() {
        ProtonBufferUtils.checkIsClosed(this);
        if (isReadOnly()) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }

        resource.setIndex(readOffset, writeOffset).discardReadBytes();
        readOffset = resource.readerIndex();
        writeOffset = resource.writerIndex();

        return this;
    }

    @Override
    public void copyInto(int offset, byte[] destination, int destOffset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        resource.getBytes(offset, destination, destOffset, length);
    }

    @Override
    public void copyInto(int offset, ByteBuffer destination, int destOffset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        if (destination.isReadOnly()) {
            throw new ProtonBufferReadOnlyException();
        }

        final int oldPos = destination.position();
        final int oldLimit = destination.limit();

        destination.position(destOffset).limit(destOffset + length);

        try {
            resource.getBytes(offset, destination);
        } finally {
            destination.position(oldPos).limit(oldLimit);
        }
    }

    @Override
    public void copyInto(int offset, ProtonBuffer destination, int destOffset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        if (destination.isReadOnly()) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(destination);
        }

        final int oldRPos = destination.getReadOffset();
        final int oldWPos = destination.getWriteOffset();

        destination.setReadOffset(0);
        destination.setWriteOffset(destOffset);

        try (ProtonBufferComponentAccessor accessor = destination.componentAccessor()) {
            for (ProtonBufferComponent componenet = accessor.firstWritable(); componenet != null && length > 0; componenet = accessor.nextWritable()) {
                final int toWrite = Math.min(componenet.getWritableBytes(), length);

                if (componenet.hasWritableArray()) {
                    resource.getBytes(offset, componenet.getWritableArray(), componenet.getWritableArrayOffset(), toWrite);
                } else {
                    resource.getBytes(offset, componenet.getWritableBuffer().limit(toWrite));
                }

                length -= toWrite;
                offset += toWrite;
            }
        } finally {
            destination.setWriteOffset(oldWPos);
            destination.setReadOffset(oldRPos);
        }
    }

    @Override
    public ProtonBuffer writeBytes(byte[] source, int offset, int length) {
        checkWrite(writeOffset, length, true);
        resource.setBytes(writeOffset, source, offset, length);
        writeOffset += length;

        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ByteBuffer source) {
        final int remaining = source.remaining();
        checkWrite(writeOffset, source.remaining(), true);
        resource.setBytes(writeOffset, source);
        writeOffset += remaining;

        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer source) {
        final int size = source.getReadableBytes();
        checkWrite(writeOffset, size, true);
        source.copyInto(source.getReadOffset(), this, writeOffset, size);
        source.advanceReadOffset(size);
        writeOffset += size;
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ByteBuffer destination) {
        final int remaining = destination.remaining();
        checkRead(readOffset, remaining);
        resource.getBytes(readOffset, destination);
        readOffset += remaining;

        return this;
    }

    @Override
    public ProtonBuffer readBytes(byte[] destination, int offset, int length) {
        checkRead(readOffset, length);
        resource.getBytes(readOffset, destination, offset, length);
        readOffset += length;
        return this;
    }

    @Override
    public ProtonBuffer ensureWritable(int size, int minimumGrowth, boolean allowCompaction) throws IndexOutOfBoundsException, IllegalArgumentException {
        ProtonBufferUtils.checkIsClosed(this);
        if (isReadOnly()) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }
        ProtonBufferUtils.checkBufferCanGrowTo(writeCapacity, size);

        if (getWritableBytes() >= size) {
            return this;
        }

        if (allowCompaction && readOffset > 0) {
            compact();
            if (getWritableBytes() >= size) {
                return this;
            }
        }

        final int growBy = Math.max(minimumGrowth, size - getWritableBytes());
        resource.setIndex(readOffset, writeOffset).ensureWritable(growBy, true);

        if (resource.writableBytes() < size) {
            final int target = resource.capacity() + growBy;
            ProtonBufferUtils.checkBufferCanGrowTo(resource.capacity(), target);

            // Netty buffer could not be resized internally so we need to do it
            // explicitly by creating a new buffer.  Limit max capacity on the
            // new buffer to the target to prevent overshoot of implicit growth
            // limits.
            final ByteBuf copy = allocator.allocator().buffer(target, target);
            final ByteBuf dropped = this.resource;

            resource.getBytes(0, copy, 0, resource.capacity());

            // Release the original ByteBuf but first capture the index
            // values and then restore them on the new buffer instance.
            final int roff = getReadOffset();
            final int woff = getWriteOffset();

            // Free the old resource now since we are replacing it.
            ReferenceCountUtil.safeRelease(dropped);

            resource = copy;
            resource.writerIndex(woff);
            resource.readerIndex(roff);
        }

        // Update our state to keep in touch with the ByteBuf state
        writeCapacity = readCapacity = resource.capacity();

        return this;
    }

    @Override
    public ProtonBuffer copy(int index, int length, boolean readOnly) throws IllegalArgumentException {
        ProtonBufferUtils.checkIsClosed(this);

        final ProtonBuffer copy;

        if (this.readOnly && readOnly) {
            copy = allocator.wrap(resource.retainedSlice(index, length));
            copy.setReadOffset(0);
            ((ByteBuf) copy.unwrap()).writerIndex(length);
            copy.convertToReadOnly();
        } else {
            copy = allocator.wrap(resource.copy(index, length));
            if (readOnly) {
                copy.convertToReadOnly();
            }
        }

        return copy;
    }

    @Override
    public ProtonBuffer split(int splitOffset) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(splitOffset, "Split offset cannot be negative");
        if (splitOffset > capacity()) {
            throw new IllegalArgumentException("Split offset cannot exceed buffer capacity");
        }

        final int woff = getWriteOffset();
        final int roff = getReadOffset();

        Netty4ToProtonBufferAdapter splitBuffer = new Netty4ToProtonBufferAdapter(
            allocator, resource.retainedSlice(0, splitOffset));

        splitBuffer.setWriteOffset(Math.min(woff, splitOffset));
        splitBuffer.setReadOffset(Math.min(roff, splitOffset));
        if (readOnly) {
            splitBuffer.convertToReadOnly();
        }

        resource = resource.slice(splitOffset, capacity() - splitOffset);
        resource.writerIndex(Math.max(woff, splitOffset) - splitOffset);
        resource.readerIndex(Math.max(roff, splitOffset) - splitOffset);
        readCapacity = resource.capacity();
        writeCapacity = isReadOnly() ? CLOSED_MARKER : readCapacity;
        readOffset = resource.readerIndex();
        writeOffset = resource.writerIndex();

        return splitBuffer;
    }

    //----- JDK Overrides

    @Override
    public String toString(Charset charset) {
        return resource.toString(charset);
    }

    @Override
    public String toString() {
        return "Netty4ToProtonBufferAdapter" +
               "{ read:" + readOffset +
               ", write: " + writeOffset +
               ", capacity: " + readCapacity + "}";
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

    //----- Primitive Get Methods

    @Override
    public byte getByte(int index) {
        checkGet(index, Byte.BYTES);
        return resource.getByte(index);
    }

    @Override
    public int getUnsignedByte(int index) {
        checkGet(index, Byte.BYTES);
        return resource.getUnsignedByte(index);
    }

    @Override
    public char getChar(int index) {
        checkGet(index, Character.BYTES);
        return resource.getChar(index);
    }

    @Override
    public short getShort(int index) {
        checkGet(index, Short.BYTES);
        return resource.getShort(index);
    }

    @Override
    public int getInt(int index) {
        checkGet(index, Integer.BYTES);
        return resource.getInt(index);
    }

    @Override
    public long getLong(int index) {
        checkGet(index, Long.BYTES);
        return resource.getLong(index);
    }

    //----- Primitive Set Methods

    @Override
    public ProtonBuffer setByte(int index, byte value) {
        try {
            checkSet(index, Byte.BYTES);
            resource.setByte(index, value);
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, char value) {
        try {
            checkSet(index, Character.BYTES);
            resource.setChar(index, value);
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer setShort(int index, short value) {
        try {
            checkSet(index, Short.BYTES);
            resource.setShort(index, value);
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer setInt(int index, int value) {
        try {
            checkSet(index, Integer.BYTES);
            resource.setInt(index, value);
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer setLong(int index, long value) {
        try {
            checkSet(index, Long.BYTES);
            resource.setLong(index, value);
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    //----- Primitive Read Methods

    @Override
    public byte readByte() {
        checkRead(readOffset, Byte.BYTES);
        return resource.getByte(readOffset++);
    }

    @Override
    public char readChar() {
        checkRead(readOffset, Character.BYTES);
        final char result = resource.getChar(readOffset);
        readOffset += Character.BYTES;
        return result;
    }

    @Override
    public short readShort() {
        checkRead(readOffset, Short.BYTES);
        final short result = resource.getShort(readOffset);
        readOffset += Short.BYTES;
        return result;
    }

    @Override
    public int readInt() {
        checkRead(readOffset, Integer.BYTES);
        final int result = resource.getInt(readOffset);
        readOffset += Integer.BYTES;
        return result;
    }

    @Override
    public long readLong() {
        checkRead(readOffset, Long.BYTES);
        final long result = resource.getLong(readOffset);
        readOffset += Long.BYTES;
        return result;
    }

    //----- Primitive Write Methods

    @Override
    public ProtonBuffer writeByte(byte value) {
        checkWrite(writeOffset, Byte.BYTES, true);
        try {
            resource.setByte(writeOffset++, value);
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeChar(char value) {
        checkWrite(writeOffset, Character.BYTES, true);
        try {
            resource.setChar(writeOffset, value);
            writeOffset += Character.BYTES;
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeShort(short value) {
        checkWrite(writeOffset, Short.BYTES, true);
        try {
            resource.setShort(writeOffset, value);
            writeOffset += Short.BYTES;
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeInt(int value) {
        checkWrite(writeOffset, Integer.BYTES, true);
        try {
            resource.setInt(writeOffset, value);
            writeOffset += Integer.BYTES;
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    @Override
    public ProtonBuffer writeLong(long value) {
        checkWrite(writeOffset, Long.BYTES, true);
        try {
            resource.setLong(writeOffset, value);
            writeOffset += Long.BYTES;
        } catch (RuntimeException e) {
            throw translateNettyToProtonException(e, true);
        }
        return this;
    }

    //----- IO Handlers

    @Override
    public int transferTo(WritableByteChannel channel, int length) throws IOException {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "transferTo length value cannot be negative: " + length);

        final int writableBytes = Math.min(length, getReadableBytes());

        if (writableBytes == 0) {
            return 0;
        }

        final ByteBuffer nioBuffer = resource.nioBuffer(readOffset, writableBytes);
        final int written = channel.write(nioBuffer);

        readOffset += written;

        return written;
    }

    @Override
    public int transferFrom(ReadableByteChannel channel, int length) throws IOException {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "transferTo length value cannot be negative: " + length);
        ProtonBufferUtils.checkIsReadOnly(this);

        final int readableBytes = Math.min(length, getWritableBytes());

        if (readableBytes == 0) {
            return 0;
        }

        final ByteBuffer nioBuffer = resource.nioBuffer(writeOffset, readableBytes);
        final int bytesRead = channel.read(nioBuffer);

        writeOffset += bytesRead > 0 ? bytesRead : 0;

        return bytesRead;
    }

    @Override
    public int transferFrom(FileChannel channel, long position, int length) throws IOException {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "transferTo length value cannot be negative: " + length);
        ProtonBufferUtils.checkIsReadOnly(this);

        final int readableBytes = Math.min(length, getWritableBytes());

        if (readableBytes == 0) {
            return 0;
        }

        final ByteBuffer nioBuffer = resource.nioBuffer(writeOffset, readableBytes);
        final int bytesRead = channel.read(nioBuffer, position);

        writeOffset += bytesRead > 0 ? bytesRead : 0;

        return bytesRead;
    }

    //----- Buff component access

    @Override
    public ProtonBufferComponentAccessor componentAccessor() {
        if (isClosed()) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        return (ProtonBufferComponentAccessor) acquire();
    }

    @Override
    public ProtonBufferComponent first() {
        return this;
    }

    @Override
    public ProtonBufferComponent next() {
        return null; // For now we don't try and expose Netty composites
    }

    //----- ProtonBufferComponent API Implementation

    @Override
    public boolean hasReadbleArray() {
        return resource.hasArray();
    }

    @Override
    public Netty4ToProtonBufferAdapter advanceReadOffset(int amount) {
        return (Netty4ToProtonBufferAdapter) ProtonBuffer.super.advanceReadOffset(amount);
    }

    @Override
    public byte[] getReadableArray() {
        return resource.array();
    }

    @Override
    public int getReadableArrayOffset() {
        return resource.arrayOffset() + getReadOffset();
    }

    @Override
    public int getReadableArrayLength() {
        return resource.capacity();
    }

    @Override
    public ByteBuffer getReadableBuffer() {
        return resource.nioBuffer(readOffset, getReadableBytes()).asReadOnlyBuffer();
    }

    @Override
    public Netty4ToProtonBufferAdapter advanceWriteOffset(int amount) {
        return (Netty4ToProtonBufferAdapter) ProtonBuffer.super.advanceWriteOffset(amount);
    }

    @Override
    public boolean hasWritableArray() {
        return resource.hasArray() && !isReadOnly();
    }

    @Override
    public byte[] getWritableArray() {
        return resource.array();
    }

    @Override
    public int getWritableArrayOffset() {
        return resource.arrayOffset() + getWriteOffset();
    }

    @Override
    public int getWritableArrayLength() {
        return getWritableBytes();
    }

    @Override
    public ByteBuffer getWritableBuffer() {
        if (hasWritableArray()) {
            return ByteBuffer.wrap(resource.array(), resource.arrayOffset() + writeOffset, getWritableBytes());
        } else {
            return resource.nioBuffer(writeOffset, getWritableBytes());
        }
    }

    @Override
    public long getNativeAddress() {
        return 0;
    }

    @Override
    public long getNativeReadAddress() {
        return 0;
    }

    @Override
    public long getNativeWriteAddress() {
        return 0;
    }

    //----- Buffer Iteration API

    @Override
    public ProtonBufferIterator bufferIterator(int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);
        ProtonBufferUtils.checkIsNotNegative(length, "length");
        ProtonBufferUtils.checkIsNotNegative(offset, "length");

        if (offset + length > resource.capacity()) {
            throw new IndexOutOfBoundsException(
                "The iterator cannot read beyond the bounds of the buffer: offset=" + offset + ", length=" + length);
        }

        return new Netty4ToProtonBufferIterator(resource, offset, length);
    }

    private final class Netty4ToProtonBufferIterator implements ProtonBufferIterator {

        private final ByteBuf resource;
        private final int endIndex;

        private int current;

        public Netty4ToProtonBufferIterator(ByteBuf resource, int offset, int length) {
            this.resource = resource;
            this.current = offset;
            this.endIndex = offset + length; // Exclusive
        }

        @Override
        public boolean hasNext() {
            return current != endIndex;
        }

        @Override
        public byte next() {
            if (current == endIndex) {
                throw new NoSuchElementException("Cannot read outside the iterator bounds");
            }

            return resource.getByte(current++);
        }

        @Override
        public int remaining() {
            return endIndex - current;
        }

        @Override
        public int offset() {
            return current;
        }
    }

    @Override
    public ProtonBufferIterator bufferIterator() {
        return bufferIterator(getReadOffset(), getReadableBytes());
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

        return new Netty4ToProtonBufferReverseIterator(resource, offset, length);
    }

    private final class Netty4ToProtonBufferReverseIterator implements ProtonBufferIterator {

        private final ByteBuf resource;
        private final int endIndex;

        private int current;

        public Netty4ToProtonBufferReverseIterator(ByteBuf resource, int offset, int length) {
            this.resource = resource;
            this.current = offset;
            this.endIndex = offset - length; // Exclusive
        }

        @Override
        public boolean hasNext() {
            return current != endIndex;
        }

        @Override
        public byte next() {
            if (current == endIndex) {
                throw new NoSuchElementException("Cannot read outside the iterator bounds");
            }

            return resource.getByte(current--);
        }

        @Override
        public int remaining() {
            return Math.abs(endIndex - current);
        }

        @Override
        public int offset() {
            return current;
        }
    }

    //----- Buffer search API

    @Override
    public int indexOf(byte needle, int offset, int length) {
        ProtonBufferUtils.checkIsClosed(this);

        final int count = resource.bytesBefore(offset, length, needle);

        return count >= 0 ? offset + count : -1;
    }

    //----- Shared resource API

    @Override
    protected void releaseResourceOwnership() {
        closed = true;
        try {
            if (resource != Unpooled.EMPTY_BUFFER) {
                ReferenceCountUtil.safeRelease(resource);
            }
        } finally {
            resource = Unpooled.EMPTY_BUFFER;
            readOnly = false;
            writeCapacity = CLOSED_MARKER;
            readCapacity = CLOSED_MARKER;
            readOffset = 0;
            writeOffset = 0;
        }
    }

    @Override
    protected ProtonBuffer transferTheResource() {
        try {
            return new Netty4ToProtonBufferAdapter(allocator, resource.setIndex(readOffset, writeOffset), readOnly);
        } finally {
             resource = Unpooled.EMPTY_BUFFER;
        }
    }

    @Override
    protected RuntimeException resourceIsClosedException() {
        return ProtonBufferUtils.genericBufferIsClosed(this);
    }

    //----- Internal utilities for mapping to netty

    private void checkRead(int index, int size) {
        if (index < 0 || writeOffset < index + size || closed) {
            if (closed) {
                throw ProtonBufferUtils.genericBufferIsClosed(this);
            } else {
                throw ProtonBufferUtils.genericOutOfBounds(this, index);
            }
        }
    }

    private void checkGet(int index, int size) {
        if (index < 0 || readCapacity < index + size) {
            if (closed) {
                throw ProtonBufferUtils.genericBufferIsClosed(this);
            } else {
                throw ProtonBufferUtils.genericOutOfBounds(this, index);
            }
        }
    }

    private void checkSet(int index, int size) {
        if (index < 0 || writeCapacity < index + size) {
            expandOrThrowError(index, size, false);
        }
    }

    private void checkWrite(int index, int size, boolean allowExpansion) {
        if (index < readOffset || writeCapacity < (index + size)) {
            expandOrThrowError(index, size, allowExpansion);
        }
    }

    private void expandOrThrowError(int index, int size, boolean mayExpand) {
        if (readCapacity == CLOSED_MARKER) {
            throw ProtonBufferUtils.genericBufferIsClosed(this);
        }

        if (readOnly) {
            throw ProtonBufferUtils.genericBufferIsReadOnly(this);
        }

        int capacity = capacity();
        if (mayExpand && index >= 0 && index <= capacity && writeOffset + size <= implicitGrowthLimit) {
            int minimumGrowth = Math.min(Math.max(capacity * 2, size), implicitGrowthLimit) - capacity;
            ensureWritable(size, minimumGrowth, false);
            checkSet(index, size); // Verify writing is now possible, without recursing.
            return;
        }

        throw ProtonBufferUtils.genericOutOfBounds(this, index);
    }

    private RuntimeException translateNettyToProtonException(RuntimeException caught, boolean write) {
        RuntimeException error = caught;

        if (caught instanceof IllegalReferenceCountException) {
            error = ProtonBufferUtils.genericBufferIsClosed(this);
            error.addSuppressed(caught);
        } else if (caught instanceof ReadOnlyBufferException) {
            error = ProtonBufferUtils.genericBufferIsReadOnly(this);
            error.addSuppressed(caught);
        } else if (resource.isReadOnly() && write && caught instanceof IndexOutOfBoundsException) {
            error = ProtonBufferUtils.genericBufferIsReadOnly(this);
            error.addSuppressed(caught);
        }

        return error;
    }
}
