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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.ProtonBufferIterator;
import org.apache.qpid.protonj2.buffer.ProtonBufferReadOnlyException;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferClosedException;
import io.netty5.buffer.BufferComponent;
import io.netty5.buffer.BufferReadOnlyException;
import io.netty5.buffer.ByteCursor;
import io.netty5.buffer.ComponentIterator;
import io.netty5.buffer.ComponentIterator.Next;
import io.netty5.buffer.internal.InternalBufferUtils;
import io.netty5.util.Send;

/**
 * Adapts a {@link ProtonBuffer} instance to a Netty 5 {@link Buffer}
 */
public class ProtonBufferToNetty5Adapter implements Buffer {

    private ProtonBuffer resource;

    public ProtonBufferToNetty5Adapter(ProtonBuffer resource) {
        this.resource = resource;
    }

    @Override
    public void close() {
        resource.close();
    }

    @Override
    public Send<Buffer> send() {
        try {
            final ProtonBuffer transferred = resource.transfer();

            return new Send<Buffer>() {

                @Override
                public Buffer receive() {
                    return new ProtonBufferToNetty5Adapter(transferred);
                }

                @Override
                public void close() {
                    transferred.close();
                }

                @Override
                public boolean referentIsInstanceOf(Class<?> cls) {
                    return cls.isAssignableFrom(ProtonBufferToNetty5Adapter.class);
                }
            };
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public boolean isAccessible() {
        return !resource.isClosed();
    }

    @Override
    public Buffer compact() {
        try {
            resource.compact();
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public Buffer makeReadOnly() {
        try {
            resource.convertToReadOnly();
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public boolean readOnly() {
        return resource.isReadOnly();
    }

    @Override
    public Buffer fill(byte value) {
        try {
            resource.fill(value);
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public boolean isDirect() {
        return false; // NOTE: resource.isDirect(); is only reasonable if we also expose the native address
    }

    @Override
    public int capacity() {
        return resource.capacity();
    }

    @Override
    public Buffer implicitCapacityLimit(int limit) {
        try {
            resource.implicitGrowthLimit(limit);
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public int implicitCapacityLimit() {
        return resource.implicitGrowthLimit();
    }

    @Override
    public int readerOffset() {
        return resource.getReadOffset();
    }

    @Override
    public Buffer readerOffset(int offset) {
        try {
            resource.setReadOffset(offset);
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public int writerOffset() {
        return resource.getWriteOffset();
    }

    @Override
    public Buffer writerOffset(int offset) {
        try {
            resource.setWriteOffset(offset);
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public Buffer ensureWritable(int size, int minimumGrowth, boolean allowCompaction) {
        try {
            resource.ensureWritable(size, minimumGrowth, allowCompaction);
            return this;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public void copyInto(int srcPos, byte[] dest, int destPos, int length) {
        try {
            resource.copyInto(srcPos, dest, destPos, length);
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public void copyInto(int srcPos, ByteBuffer dest, int destPos, int length) {
        try {
            resource.copyInto(srcPos, dest, destPos, length);
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public void copyInto(int srcPos, Buffer destination, int destPos, int length) {
        if (!destination.isAccessible()) {
            throw new BufferClosedException("Destination buffer is closed");
        }
        if (destination.readOnly()) {
            throw new BufferReadOnlyException("Destination buffer is read only");
        }

        checkCopyIntoArgs(srcPos, length, destPos, destination.capacity());

        try {
            // Try to reduce bounds-checking by using larger primitives when possible.
            for (; length >= Long.BYTES; length -= Long.BYTES, srcPos += Long.BYTES, destPos += Long.BYTES) {
                destination.setLong(destPos, getLong(srcPos));
            }
            for (; length >= Integer.BYTES; length -= Integer.BYTES, srcPos += Integer.BYTES, destPos += Integer.BYTES) {
                destination.setInt(destPos, getInt(srcPos));
            }
            for (; length > 0; length--, srcPos++, destPos++) {
                destination.setByte(destPos, getByte(srcPos));
            }
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public Buffer copy(int offset, int length, boolean readOnly) {
        try {
            return new ProtonBufferToNetty5Adapter(resource.copy(offset, length));
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public Buffer split(int splitOffset) {
        try {
            return new ProtonBufferToNetty5Adapter(resource.split(splitOffset));
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public int bytesBefore(byte needle) {
        try {
            final int indexOf = resource.indexOf(needle);
            if (indexOf >= 0) {
                return indexOf - resource.getReadOffset();
            }

            return -1;
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public int bytesBefore(Buffer needle) {
        return InternalBufferUtils.bytesBefore(this, null, needle, null);
    }

    //----- Buffer input and output APIs

    @Override
    public int transferTo(WritableByteChannel channel, int length) throws IOException {
        try {
            return resource.transferTo(channel, length);
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public int transferFrom(FileChannel channel, long position, int length) throws IOException {
        try {
            return resource.transferFrom(channel, position, length);
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public int transferFrom(ReadableByteChannel channel, int length) throws IOException {
        try {
            return resource.transferFrom(channel, length);
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    //----- Buffer cursors API

    @Override
    public ByteCursor openCursor() {
        return openCursor(readerOffset(), readableBytes());
    }

    @Override
    public ByteCursor openCursor(int fromOffset, int length) {
        try {
            return new ProtonBufferToNetty5ByteCursor(resource.bufferIterator(fromOffset, length));
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    @Override
    public ByteCursor openReverseCursor(int fromOffset, int length) {
        try {
            return new ProtonBufferToNetty5ByteCursor(resource.bufferReverseIterator(fromOffset, length));
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    private final class ProtonBufferToNetty5ByteCursor implements ByteCursor {

        private final ProtonBufferIterator iterator;

        private byte lastReadByte = -1;

        public ProtonBufferToNetty5ByteCursor(ProtonBufferIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean readByte() {
            if (iterator.hasNext()) {
                lastReadByte = iterator.next();
                return true;
            }

            return false;
        }

        @Override
        public byte getByte() {
            return lastReadByte;
        }

        @Override
        public int currentOffset() {
            return iterator.offset();
        }

        @Override
        public int bytesLeft() {
            return iterator.remaining();
        }
    }

    //----- Buffer components API

    @Override
    public int countComponents() {
        return resource.componentCount();
    }

    @Override
    public int countReadableComponents() {
        return resource.readableComponentCount();
    }

    @Override
    public int countWritableComponents() {
        return resource.writableComponentCount();
    }

    @Override
    public <T extends BufferComponent & Next> ComponentIterator<T> forEachComponent() {
        try {
            return new ProtonBufferComponentIterator<>(resource.componentAccessor());
        } catch (RuntimeException e) {
            throw translateToNettyException(e);
        }
    }

    //----- Primitive indexed set API

    @Override
    public Buffer setByte(int woff, byte value) {
        try {
            resource.setByte(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setUnsignedByte(int woff, int value) {
        try {
            resource.setByte(woff, (byte)(value & 0xFF));
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setChar(int woff, char value) {
        try {
            resource.setChar(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setShort(int woff, short value) {
        try {
            resource.setShort(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setUnsignedShort(int woff, int value) {
        try {
            resource.setUnsignedShort(woff, (short)(value & 0x0000FFFF));
            return null;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setMedium(int woff, int value) {
        try {
            resource.setByte(woff, (byte) (value >>> 16));
            resource.setByte(woff + 1, (byte) (value >>> 8));
            resource.setByte(woff + 2, (byte) (value >>> 0));

            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setUnsignedMedium(int woff, int value) {
        try {
            resource.setByte(woff, (byte) (value >>> 16));
            resource.setByte(woff + 1, (byte) (value >>> 8));
            resource.setByte(woff + 2, (byte) (value >>> 0));

            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setInt(int woff, int value) {
        try {
            resource.setInt(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setUnsignedInt(int woff, long value) {
        try {
            resource.setUnsignedInt(woff, (int)(value & 0x00000000FFFFFFFFl));
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setLong(int woff, long value) {
        try {
            resource.setLong(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setFloat(int woff, float value) {
        try {
            resource.setFloat(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer setDouble(int woff, double value) {
        try {
            resource.setDouble(woff, value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    //----- Primitive relative write API

    @Override
    public Buffer writeByte(byte value) {
        try {
            resource.writeByte(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeUnsignedByte(int value) {
        try {
            resource.writeByte((byte) (value & 0xFF));
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeShort(short value) {
        try {
            resource.writeShort(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeUnsignedShort(int value) {
        try {
            resource.writeShort((short) (value & 0x00FF));
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeChar(char value) {
        try {
            resource.writeChar(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeMedium(int value) {
        try {
            resource.writeByte((byte) (value >>> 16));
            resource.writeByte((byte) (value >>> 8));
            resource.writeByte((byte) (value >>> 0));

            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeUnsignedMedium(int value) {
        try {
            resource.writeByte((byte) (value >>> 16));
            resource.writeByte((byte) (value >>> 8));
            resource.writeByte((byte) (value >>> 0));

            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeInt(int value) {
        try {
            resource.writeInt(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeUnsignedInt(long value) {
        try {
            resource.writeInt((int)(value & 0x00000000FFFFFFFFl));
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeLong(long value) {
        try {
            resource.writeLong(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeFloat(float value) {
        try {
            resource.writeFloat(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeDouble(double value) {
        try {
            resource.writeDouble(value);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public Buffer writeCharSequence(CharSequence source, Charset charset) {
        try {
            resource.writeCharSequence(source, charset);
            return this;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    //----- Primitive indexed get API

    @Override
    public byte getByte(int index) {
        try {
            return resource.getByte(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public char getChar(int index) {
        try {
            return resource.getChar(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int getUnsignedByte(int index) {
        try {
            return resource.getUnsignedByte(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int getUnsignedShort(int index) {
        try {
            return resource.getUnsignedShort(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public short getShort(int index) {
        try {
            return resource.getShort(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int getMedium(int index) {
        try {
            return (getByte(index)) << 16 |
                    (getByte(index + 1) & 0xFF) << 8 |
                    (getByte(index + 2) & 0xFF) << 0;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int getUnsignedMedium(int index) {
        try {
            return ((getByte(index)) << 16 |
                    (getByte(index + 1) & 0xFF) << 8 |
                    (getByte(index + 2) & 0xFF) << 0) & 0xFFFFFF;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int getInt(int index) {
        try {
            return resource.getInt(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public long getUnsignedInt(int index) {
        try {
            return resource.getUnsignedInt(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public long getLong(int index) {
        try {
            return resource.getLong(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public float getFloat(int index) {
        try {
            return resource.getFloat(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public double getDouble(int index) {
        try {
            return resource.getDouble(index);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    //----- Primitive relative read API

    @Override
    public byte readByte() {
        try {
            return resource.readByte();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int readUnsignedByte() {
        try {
            return resource.readUnsignedByte();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public char readChar() {
        try {
            return resource.readChar();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public short readShort() {
        try {
            return resource.readShort();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int readUnsignedShort() {
        try {
            return readShort() & 0x0000FFFF;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int readMedium() {
        try {
            return readByte() << 16 |
                   (readByte() & 0xFF) << 8 |
                   (readByte() & 0xFF) << 0;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int readUnsignedMedium() {
        try {
            return ((readByte()) << 16 |
                    (readByte() & 0xFF) << 8 |
                    (readByte() & 0xFF) << 0) & 0xFFFFFF;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public int readInt() {
        try {
            return resource.readInt();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public long readUnsignedInt() {
        try {
            return readInt() & 0x00000000FFFFFFFFl;
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public long readLong() {
        try {
            return resource.readLong();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public float readFloat() {
        try {
            return resource.readFloat();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public double readDouble() {
        try {
            return resource.readDouble();
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    @Override
    public CharSequence readCharSequence(int length, Charset charset) {
        try {
            return resource.readCharSequence(length, charset);
        } catch (RuntimeException ex) {
            throw translateToNettyException(ex);
        }
    }

    //----- Object API overrides

    @Override
    public boolean equals(Object o) {
        return o instanceof Buffer && InternalBufferUtils.equals(this, (Buffer) o);
    }

    @Override
    public int hashCode() {
        return InternalBufferUtils.hashCode(this);
    }

    //----- Internal API for validation of arguments

    private void checkCopyIntoArgs(int srcPos, int length, int destPos, int destLength) {
        if (resource.isClosed()) {
            throw new BufferClosedException("The wrapped ProtonBuffer is closed");
        }
        if (srcPos < 0) {
            throw new IndexOutOfBoundsException("The srcPos cannot be negative: " + srcPos + '.');
        }
        if (length < 0) {
            throw new IndexOutOfBoundsException("The length value cannot be negative " + length + ".");
        }
        if (resource.capacity() < srcPos + length) {
            throw new IndexOutOfBoundsException("The srcPos + length is beyond the end of the buffer: " +
                    "srcPos = " + srcPos + ", length = " + length + '.');
        }
        if (destPos < 0) {
            throw new IndexOutOfBoundsException("The destPos cannot be negative: " + destPos + '.');
        }
        if (destLength < destPos + length) {
            throw new IndexOutOfBoundsException("The destPos + length is beyond the end of the destination: " +
                    "destPos = " + destPos + ", length = " + length + '.');
        }
    }

    private RuntimeException translateToNettyException(RuntimeException e) {
        RuntimeException result = e;

        if (e instanceof ProtonBufferReadOnlyException) {
            result = new BufferReadOnlyException("Buffer is read-only");
            result.addSuppressed(e);
        } else if (e instanceof ProtonBufferClosedException) {
            result = new BufferClosedException("Buffer is closed");
            result.addSuppressed(e);
        }

        return result;
    }

    //----- Wrappers needed to adapt to the Netty 5 BufferCompoent and ComponentIterator API

    private static class ProtonBufferComponentIterator<T extends BufferComponent & Next> implements Next, BufferComponent, ComponentIterator<T> {

        private ProtonBufferComponentAccessor accessor;

        private ProtonBufferComponent current;

        public ProtonBufferComponentIterator(ProtonBufferComponentAccessor accessor) {
            this.accessor = accessor;
        }

        @Override
        public void close() {
            current = null;

            accessor.close();
        }

        @SuppressWarnings("unchecked")
        @Override
        public T first() {
            return (current = accessor.first()) != null ? (T) this : null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
            if (current != null) {
                current = accessor.next();
            }

            return current != null ? (T) this : null;
        }

        @Override
        public boolean hasReadableArray() {
            return current.hasReadbleArray();
        }

        @Override
        public boolean hasWritableArray() {
            return current.hasWritableArray();
        }

        @Override
        public byte[] readableArray() {
            return current.getReadableArray();
        }

        @Override
        public byte[] writableArray() {
            return current.getWritableArray();
        }

        @Override
        public int readableArrayOffset() {
            return current.getReadableArrayOffset();
        }

        @Override
        public int writableArrayOffset() {
            return current.getWritableArrayOffset();
        }

        @Override
        public int readableArrayLength() {
            return current.getReadableArrayLength();
        }

        @Override
        public int writableArrayLength() {
            return current.getWritableArrayLength();
        }

        @Override
        public long baseNativeAddress() {
            return 0;
        }

        @Override
        public long readableNativeAddress() {
            return 0;
        }

        @Override
        public long writableNativeAddress() {
            return 0;
        }

        @Override
        public ByteBuffer readableBuffer() {
            return current.getReadableBuffer();
        }

        @Override
        public ByteBuffer writableBuffer() {
            return current.getWritableBuffer();
        }

        @Override
        public int readableBytes() {
            return current.getReadableBytes();
        }

        @Override
        public int writableBytes() {
            return current.getWritableBytes();
        }

        @Override
        public ByteCursor openCursor() {
            return new ProtonByteCursorAdapter(current.bufferIterator());
        }

        @Override
        public BufferComponent skipReadableBytes(int byteCount) {
            current.advanceReadOffset(byteCount);
            return this;
        }

        @Override
        public BufferComponent skipWritableBytes(int byteCount) {
            current.advanceWriteOffset(byteCount);
            return this;
        }
    }

    private final static class ProtonByteCursorAdapter implements ByteCursor {

        private final ProtonBufferIterator iterator;

        private byte lastByte = -1;

        public ProtonByteCursorAdapter(ProtonBufferIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean readByte() {
            if (iterator.hasNext()) {
                lastByte = iterator.next();
                return true;
            }

            return false;
        }

        @Override
        public byte getByte() {
            return lastByte;
        }

        @Override
        public int currentOffset() {
            return iterator.offset();
        }

        @Override
        public int bytesLeft() {
            return iterator.remaining();
        }
    }
}
