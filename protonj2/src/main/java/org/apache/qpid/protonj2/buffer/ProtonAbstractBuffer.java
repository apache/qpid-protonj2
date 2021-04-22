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
package org.apache.qpid.protonj2.buffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Base class used to hold the common implementation details for Proton buffer
 * implementations.
 */
public abstract class ProtonAbstractBuffer implements ProtonBuffer {

    protected int readIndex;
    protected int writeIndex;
    protected int markedReadIndex;
    protected int markedWriteIndex;

    private int maximumCapacity;

    protected ProtonAbstractBuffer(int maximumCapacity) {
        if (maximumCapacity < 0) {
            throw new IllegalArgumentException("Maximum capacity should be non-negative but was: " + maximumCapacity);
        }

        this.maximumCapacity = maximumCapacity;
    }

    @Override
    public int maxCapacity() {
        return maximumCapacity;
    }

    @Override
    public int getReadableBytes() {
        return writeIndex - readIndex;
    }

    @Override
    public int getWritableBytes() {
        return capacity() - writeIndex;
    }

    @Override
    public int getMaxWritableBytes() {
        return maxCapacity() - writeIndex;
    }

    @Override
    public int getReadIndex() {
        return readIndex;
    }

    @Override
    public ProtonBuffer setReadIndex(int value) {
        if (value < 0 || value > writeIndex) {
            throw new IndexOutOfBoundsException(String.format(
                "readIndex: %d (expected: 0 <= readIndex <= writeIndex(%d))", value, writeIndex));
        }
        readIndex = value;
        return this;
    }

    @Override
    public int getWriteIndex() {
        return writeIndex;
    }

    @Override
    public ProtonBuffer setWriteIndex(int value) {
        if (value < readIndex || value > capacity()) {
            throw new IndexOutOfBoundsException(String.format(
                "writeIndex: %d (expected: readIndex(%d) <= writeIndex <= capacity(%d))",
                value, readIndex, capacity()));
        }
        writeIndex = value;
        return this;
    }

    @Override
    public ProtonBuffer setIndex(int readIndex, int writeIndex) {
        if (readIndex < 0 || readIndex > writeIndex || writeIndex > capacity()) {
            throw new IndexOutOfBoundsException(String.format(
                "readIndex: %d, writeIndex: %d (expected: 0 <= readeIndex <= writeIndex <= capacity(%d))",
                readIndex, writeIndex, capacity()));
        }
        this.readIndex = readIndex;
        this.writeIndex = writeIndex;

        return this;
    }

    @Override
    public ProtonBuffer markReadIndex() {
        this.markedReadIndex = readIndex;
        return this;
    }

    @Override
    public ProtonBuffer resetReadIndex() {
        setReadIndex(markedReadIndex);
        return this;
    }

    @Override
    public ProtonBuffer markWriteIndex() {
        this.markedWriteIndex = writeIndex;
        return this;
    }

    @Override
    public ProtonBuffer resetWriteIndex() {
        setWriteIndex(markedWriteIndex);
        return this;
    }

    @Override
    public boolean isReadable() {
        return writeIndex > readIndex;
    }

    @Override
    public boolean isReadable(int numBytes) {
        return writeIndex - readIndex >= numBytes;
    }

    @Override
    public boolean isWritable() {
        return capacity() > writeIndex;
    }

    @Override
    public boolean isWritable(int numBytes) {
        return capacity() - writeIndex >= numBytes;
    }

    @Override
    public ProtonBuffer clear() {
        readIndex = 0;
        writeIndex = 0;

        return this;
    }

    @Override
    public ProtonBuffer skipBytes(int length) {
        checkReadableBytes(length);
        readIndex += length;
        return this;
    }

    @Override
    public ProtonBuffer slice() {
        return slice(readIndex, getReadableBytes());
    }

    @Override
    public ProtonBuffer slice(int index, int length) {
        checkIndex(index, length);
        return new ProtonSlicedBuffer(this, index, length);
    }

    @Override
    public ProtonBuffer duplicate() {
        return new ProtonDuplicatedBuffer(this);
    }

    @Override
    public ProtonBuffer copy() {
        return copy(readIndex, getReadableBytes());
    }

    @Override
    public abstract ProtonBuffer copy(int index, int length);

    @Override
    public ByteBuffer toByteBuffer() {
        return toByteBuffer(readIndex, getReadableBytes());
    }

    @Override
    public abstract ByteBuffer toByteBuffer(int index, int length);

    @Override
    public ProtonBuffer ensureWritable(int minWritableBytes) {
        if (minWritableBytes < 0) {
            throw new IllegalArgumentException(String.format(
                "minWritableBytes: %d (expected: >= 0)", minWritableBytes));
        }

        internalEnsureWritable(minWritableBytes);
        return this;
    }

    //----- Read methods -----------------------------------------------------//

    @Override
    public byte readByte() {
        internalCheckReadableBytes(1);
        return getByte(readIndex++);
    }

    @Override
    public boolean readBoolean() {
        return readByte() != 0;
    }

    @Override
    public short readShort() {
        internalCheckReadableBytes(2);
        short result = getShort(readIndex);
        readIndex += 2;
        return result;
    }

    @Override
    public int readInt() {
        internalCheckReadableBytes(4);
        int result = getInt(readIndex);
        readIndex += 4;
        return result;
    }

    @Override
    public long readLong() {
        internalCheckReadableBytes(8);
        long result = getLong(readIndex);
        readIndex += 8;
        return result;
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public ProtonBuffer readBytes(byte[] target) {
        readBytes(target, 0, target.length);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(byte[] target, int length) {
        readBytes(target, 0, length);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(byte[] target, int offset, int length) {
        checkReadableBytes(length);
        getBytes(readIndex, target, offset, length);
        readIndex += length;
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ProtonBuffer target) {
        readBytes(target, target.getWritableBytes());
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ProtonBuffer target, int length) {
        if (length > target.getWritableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                "length(%d) exceeds target Writable Bytes:(%d), target is: %s", length, target.getWritableBytes(), target));
        }
        readBytes(target, target.getWriteIndex(), length);
        target.setWriteIndex(target.getWriteIndex() + length);
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ProtonBuffer target, int offset, int length) {
        checkReadableBytes(length);
        getBytes(readIndex, target, offset, length);
        readIndex += length;
        return this;
    }

    @Override
    public ProtonBuffer readBytes(ByteBuffer dst) {
        int length = dst.remaining();
        checkReadableBytes(length);
        getBytes(readIndex, dst);
        readIndex += length;
        return this;
    }

    //----- Write methods ----------------------------------------------------//

    @Override
    public ProtonBuffer writeByte(int value) {
        internalEnsureWritable(1);
        return setByte(writeIndex++, value);
    }

    @Override
    public ProtonBuffer writeBoolean(boolean value) {
        writeByte(value ? (byte) 1 : (byte) 0);
        return this;
    }

    @Override
    public ProtonBuffer writeShort(short value) {
        internalEnsureWritable(2);
        setShort(writeIndex, value);
        writeIndex += 2;
        return this;
    }

    @Override
    public ProtonBuffer writeInt(int value) {
        internalEnsureWritable(4);
        setInt(writeIndex, value);
        writeIndex += 4;
        return this;
    }

    @Override
    public ProtonBuffer writeLong(long value) {
        internalEnsureWritable(8);
        setLong(writeIndex, value);
        writeIndex += 8;
        return this;
    }

    @Override
    public ProtonBuffer writeFloat(float value) {
        writeInt(Float.floatToRawIntBits(value));
        return this;
    }

    @Override
    public ProtonBuffer writeDouble(double value) {
        writeLong(Double.doubleToRawLongBits(value));
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(byte[] source) {
        writeBytes(source, 0, source.length);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(byte[] source, int length) {
        writeBytes(source, 0, length);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(byte[] source, int offset, int length) {
        ensureWritable(length);
        setBytes(writeIndex, source, offset, length);
        writeIndex += length;
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer source) {
        writeBytes(source, source.getReadableBytes());
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer source, int length) {
        if (length > source.getReadableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                "length(%d) exceeds source Readable Bytes(%d), source is: %s", length, source.getReadableBytes(), source));
        }
        writeBytes(source, source.getReadIndex(), length);
        source.setReadIndex(source.getReadIndex() + length);
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ProtonBuffer source, int offset, int length) {
        ensureWritable(length);
        setBytes(writeIndex, source, offset, length);
        writeIndex += length;
        return this;
    }

    @Override
    public ProtonBuffer writeBytes(ByteBuffer source) {
        int length = source.remaining();
        ensureWritable(length);
        setBytes(writeIndex, source);
        writeIndex += length;
        return this;
    }

    //----- Get methods that call into other get methods ---------------------//

    @Override
    public boolean getBoolean(int index) {
        return getByte(index) != 0;
    }

    @Override
    public short getUnsignedByte(int index) {
        return (short) (getByte(index) & 0xFF);
    }

    @Override
    public int getUnsignedShort(int index) {
        return getShort(index) & 0xFFFF;
    }

    @Override
    public long getUnsignedInt(int index) {
        return getInt(index) & 0xFFFFFFFFL;
    }

    @Override
    public char getChar(int index) {
        return (char) getShort(index);
    }

    @Override
    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    @Override
    public double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    @Override
    public ProtonBuffer getBytes(int index, byte[] target) {
        getBytes(index, target, 0, target.length);
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer target) {
        getBytes(index, target, target.getWritableBytes());
        return this;
    }

    @Override
    public ProtonBuffer getBytes(int index, ProtonBuffer target, int length) {
        getBytes(index, target, target.getWriteIndex(), length);
        target.setWriteIndex(target.getWriteIndex() + length);
        return this;
    }

    //----- Set methods that call into other set methods ---------------------//

    @Override
    public ProtonBuffer setBoolean(int index, boolean value) {
        setByte(index, value ? 1 : 0);
        return this;
    }

    @Override
    public ProtonBuffer setChar(int index, int value) {
        setShort(index, value);
        return this;
    }

    @Override
    public ProtonBuffer setFloat(int index, float value) {
        setInt(index, Float.floatToRawIntBits(value));
        return this;
    }

    @Override
    public ProtonBuffer setDouble(int index, double value) {
        setLong(index, Double.doubleToRawLongBits(value));
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, byte[] src) {
        setBytes(index, src, 0, src.length);
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source) {
        setBytes(index, source, source.getReadableBytes());
        return this;
    }

    @Override
    public ProtonBuffer setBytes(int index, ProtonBuffer source, int length) {
        checkIndex(index, length);
        if (source == null) {
            throw new NullPointerException("src");
        }
        if (length > source.getReadableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                "length(%d) exceeds source buffer Readable Bytes(%d), source is: %s", length, source.getReadableBytes(), source));
        }

        setBytes(index, source, source.getReadIndex(), length);
        source.setReadIndex(source.getReadIndex() + length);
        return this;
    }

    //----- Comparison and Equality implementations --------------------------//

    @Override
    public int hashCode() {
        final int readable = getReadableBytes();
        final int readableInts = readable >>> 2;
        final int remainingBytes = readable & 3;

        int hash = 1;
        int position = getReadIndex();

        for (int i = readableInts; i > 0; i --) {
            hash = 31 * hash + getInt(position);
            position += 4;
        }

        for (int i = remainingBytes; i > 0; i --) {
            hash = 31 * hash + getByte(position++);
        }

        if (hash == 0) {
            hash = 1;
        }

        return hash;
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

        final int readable = getReadableBytes();
        final int longCount = readable >>> 3;
        final int byteCount = readable & 7;

        int positionSelf = getReadIndex();
        int positionOther = that.getReadIndex();

        for (int i = longCount; i > 0; i --) {
            if (getLong(positionSelf) != that.getLong(positionOther)) {
                return false;
            }

            positionSelf += 8;
            positionOther += 8;
        }

        for (int i = byteCount; i > 0; i --) {
            if (getByte(positionSelf) != that.getByte(positionOther)) {
                return false;
            }

            positionSelf++;
            positionOther++;
        }

        return true;
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
    public String toString() {
        return getClass().getSimpleName() +
               "{ read:" + getReadIndex() +
               ", write: " + getWriteIndex() +
               ", capacity: " + capacity() + "}";
    }

    @Override
    public String toString(Charset charset) {
        final String result;

        if (hasArray()) {
            result = new String(getArray(), getArrayOffset() + getReadIndex(), getReadableBytes(), charset);
        } else {
            byte[] copy = new byte[getReadableBytes()];
            getBytes(getReadIndex(), copy);
            result = new String(copy, 0, copy.length, charset);
        }

        return result;
    }

    //----- Validation methods for buffer access -----------------------------//

    protected final void checkNewCapacity(int newCapacity) {
        if (newCapacity < 0 || newCapacity > maxCapacity()) {
            throw new IllegalArgumentException("newCapacity: " + newCapacity + " (expected: 0-" + maxCapacity() + ')');
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

    protected final void checkReadableBytes(int minimumReadableBytes) {
        if (minimumReadableBytes < 0) {
            throw new IllegalArgumentException("minimumReadableBytes: " + minimumReadableBytes + " (expected: >= 0)");
        }

        internalCheckReadableBytes(minimumReadableBytes);
    }

    protected final void adjustIndexMarks(int decrement) {
        final int markedReaderIndex = markedReadIndex;
        if (markedReaderIndex <= decrement) {
            markedReadIndex = 0;
            final int markedWriterIndex = markedWriteIndex;
            if (markedWriterIndex <= decrement) {
                markedWriteIndex = 0;
            } else {
                markedWriteIndex = markedWriterIndex - decrement;
            }
        } else {
            markedReadIndex = markedReaderIndex - decrement;
            markedWriteIndex -= decrement;
        }
    }

    private void internalCheckReadableBytes(int minimumReadableBytes) {
        // Called when we know that we don't need to validate if the minimum readable
        // value is negative.
        if (readIndex > writeIndex - minimumReadableBytes) {
            throw new IndexOutOfBoundsException(String.format(
                "readIndex(%d) + length(%d) exceeds writeIndex(%d): %s",
                readIndex, minimumReadableBytes, writeIndex, this));
        }
    }

    private void internalEnsureWritable(int minWritableBytes) {
        // Called when we know that we don't need to validate if the minimum writable
        // value is negative.
        if (minWritableBytes <= getWritableBytes()) {
            return;
        }

        if (minWritableBytes > maxCapacity() - writeIndex) {
            throw new IndexOutOfBoundsException(String.format(
                "writeIndex(%d) + minWritableBytes(%d) exceeds maxCapacity(%d): %s",
                writeIndex, minWritableBytes, maxCapacity(), this));
        }

        int newCapacity = calculateNewCapacity(writeIndex + minWritableBytes, maxCapacity());

        // Adjust to the new capacity.
        capacity(newCapacity);
    }

    private int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        if (minNewCapacity < 0) {
            throw new IllegalArgumentException("minNewCapacity: " + minNewCapacity + " (expectd: 0+)");
        }

        if (minNewCapacity > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                "minNewCapacity: %d (expected: not greater than maxCapacity(%d)",
                minNewCapacity, maxCapacity));
        }

        int newCapacity = 64;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        return Math.min(newCapacity, maxCapacity);
    }
}
