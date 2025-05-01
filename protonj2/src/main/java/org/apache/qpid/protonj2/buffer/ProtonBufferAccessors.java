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

/**
 * Interface for any buffer access implementation which provides consistent read
 * and write APIs for primitive values.  These APIs are useful for serialization
 * and de-serialization APIs which need to write at a primitive level.
 */
public interface ProtonBufferAccessors {

    /**
     * Look ahead an return the next byte that would be read from a call to readByte or
     * a call to getByte at the current read offset.
     *
     * @return the next readable byte without advancing the read offset.
     *
     * @throws IndexOutOfBoundsException if there is no readable bytes left in the buffer.
     */
    byte peekByte();

    /**
     * Reads a single byte at the given index and returns it without modification to the target
     * buffer read offset.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    byte getByte(int index);

    /**
     * Sets the byte value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setByte(int index, byte value);

    /**
     * Reads one byte from the buffer and advances the read index by one.
     *
     * @return a single byte from the ProtonBuffer.
     *
     * @throws IndexOutOfBoundsException if there is no readable bytes left in the buffer.
     */
    byte readByte();

    /**
     * Writes a single byte to the buffer and advances the write index by one.
     *
     * @param value
     *      The byte to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeByte(byte value);

    /**
     * Reads a single byte at the given index and returns it as a boolean value,
     * without modification to the target buffer read offset.
     *
     * @param index
     * 		The index where the value should be read from.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    default boolean getBoolean(int index) {
        return getByte(index) != 0;
    }

    /**
     * Sets the boolean value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    default ProtonBuffer setBoolean(int index, boolean value) {
        return setByte(index, (byte)(value ? 1 : 0));
    }

    /**
     * Reads a boolean value from the buffer and advances the read index by one.
     *
     * @return boolean value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    default boolean readBoolean() {
        return readByte() != 0;
    }

    /**
     * Writes a single boolean to the buffer and advances the write index by one.
     *
     * @param value
     *      The boolean to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    default ProtonBuffer writeBoolean(boolean value) {
        return writeByte((byte)(value ? 1 : 0));
    }

    /**
     * Gets a unsigned byte from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    default int getUnsignedByte(int index) {
        return getByte(index) & 0xFF;
    }

    /**
     * Sets the unsigned byte value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    default ProtonBuffer setUnsignedByte(int index, int value) {
        return setByte(index, (byte)(value & 0xFF));
    }

    /**
     * Reads one byte from the buffer and advances the read index by one.
     *
     * @return a single byte from the ProtonBuffer.
     *
     * @throws IndexOutOfBoundsException if there is no readable bytes left in the buffer.
     */
    default int readUnsignedByte() {
        return readByte() & 0xFF;
    }

    /**
     * Writes a single byte to the buffer and advances the write index by one.
     *
     * @param value
     *      The byte to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    default ProtonBuffer writeUnsignedByte(int value) {
        return writeByte((byte)(value & 0xFF));
    }

    /**
     * Gets a 2-byte char from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    char getChar(int index);

    /**
     * Sets the char value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setChar(int index, char value);

    /**
     * Reads a character value from the buffer and advances the read index by four.
     *
     * @return char value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    char readChar();

    /**
     * Writes a single character to the buffer and advances the write index by four.
     *
     * @param value
     *      The char to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeChar(char value);


    /**
     * Gets a short from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    short getShort(int index);

    /**
     * Sets the short value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setShort(int index, short value);

    /**
     * Reads a short value from the buffer and advances the read index by two.
     *
     * @return short value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    short readShort();

    /**
     * Writes a single short to the buffer and advances the write index by two.
     *
     * @param value
     *      The short to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeShort(short value);

    /**
     * Gets a unsigned short from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    default int getUnsignedShort(int index) {
        return getShort(index) & 0xFFFF;
    }

    /**
     * Sets the short value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    default ProtonBuffer setUnsignedShort(int index, int value) {
        return setShort(index, (short)(value & 0xFFFF));
    }

    /**
     * Reads an integer value from the buffer that represent the unsigned value of the short and
     * advances the read index by two.
     *
     * @return unsigned short value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    default int readUnsignedShort() {
        return readShort() & 0xFFFF;
    }

    /**
     * Writes a single short to the buffer using the input integer value and advances the write index by two.
     *
     * @param value
     *      The integer to write into the buffer as an unsigned short.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    default ProtonBuffer writeUnsignedShort(int value) {
        return writeShort((short)(value & 0xFFFF));
    }

    /**
     * Gets a int from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    int getInt(int index);

    /**
     * Sets the int value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setInt(int index, int value);

    /**
     * Reads a integer value from the buffer and advances the read index by four.
     *
     * @return integer value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    int readInt();

    /**
     * Writes a single integer to the buffer and advances the write index by four.
     *
     * @param value
     *      The integer to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeInt(int value);

    /**
     * Gets a unsigned int from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    default long getUnsignedInt(int index) {
        return getInt(index) & 0x0000_0000_FFFF_FFFFL;
    }

    /**
     * Sets the long value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    default ProtonBuffer setUnsignedInt(int index, long value) {
        return setInt(index, (int)(value & 0xFFFF_FFFFL));
    }

    /**
     * Reads a unsigned integer value from the buffer and advances the read index by four.
     *
     * @return long value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    default long readUnsignedInt() {
        return readInt() & 0x0000_0000_FFFF_FFFFL;
    }

    /**
     * Writes a single unsigned int to the buffer and advances the write index by four.
     *
     * @param value
     *      The long to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    default ProtonBuffer writeUnsignedInt(long value) {
        return writeInt((int)(value & 0xFFFF_FFFFL));
    }

    /**
     * Gets a long from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    long getLong(int index);

    /**
     * Sets the long value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    ProtonBuffer setLong(int index, long value);

    /**
     * Reads a long value from the buffer and advances the read index by eight.
     *
     * @return long value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    long readLong();

    /**
     * Writes a single long to the buffer and advances the write index by eight.
     *
     * @param value
     *      The long to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    ProtonBuffer writeLong(long value);

    /**
     * Gets a float from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    default float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    /**
     * Sets the float value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    default ProtonBuffer setFloat(int index, float value) {
        return setInt(index, Float.floatToIntBits(value));
    }

    /**
     * Reads a float value from the buffer and advances the read index by four.
     *
     * @return float value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    default float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * Writes a single float to the buffer and advances the write index by four.
     *
     * @param value
     *      The float to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    default ProtonBuffer writeFloat(float value) {
        return writeInt(Float.floatToIntBits(value));
    }

    /**
     * Gets a double from the specified index, this method will not modify the read or write
     * index.
     *
     * @param index
     *      The index into the buffer where the value should be read.
     *
     * @return the value read from the given index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or past the current buffer capacity.
     */
    default double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    /**
     * Sets the double value at the given write index in this buffer's backing data store.
     *
     * @param index
     *      The index to start the write from.
     * @param value
     *      The value to write at the given index.
     *
     * @return a reference to this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if the index is negative or the write would exceed capacity.
     */
    default ProtonBuffer setDouble(int index, double value) {
        return setLong(index, Double.doubleToLongBits(value));
    }

    /**
     * Reads a double value from the buffer and advances the read index by eight.
     *
     * @return double value read from the buffer.
     *
     * @throws IndexOutOfBoundsException if a value cannot be read from the buffer.
     */
    default double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Writes a single double to the buffer and advances the write index by eight.
     *
     * @param value
     *      The double to write into the buffer.
     *
     * @return this ProtonBuffer for chaining.
     *
     * @throws IndexOutOfBoundsException if there is no room in the buffer for this write operation.
     */
    default ProtonBuffer writeDouble(double value) {
        return writeLong(Double.doubleToLongBits(value));
    }
}
