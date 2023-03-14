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

import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * Set of Utility methods useful when dealing with byte arrays and other
 * primitive types.
 */
public abstract class ProtonBufferUtils {

    /**
     * The maximum buffer size that allows for JDK byte reservations on buffer
     * size or addresses.
     */
    public static final int MAX_BUFFER_CAPACITY = Integer.MAX_VALUE - 8;

    /*
     * Cleaner used by buffer implementations to close out wrapped or otherwise
     * managed buffer resources when the buffer is no longer reachable and can be
     * garbage collected.
     */
    private static Cleaner CLEANER;

    /**
     * Create and / or return a Cleaner instance on demand and then serve out only that
     * instance from then on.
     * <p>
     * Care should be taken when using Cleaners as the instance will be tired to a thread that
     * will run to perform the cleanups, an application is advised to assign a single global
     * value for the whole application,
     *
     * @return a {@link Cleaner} instance which is created on demand or was already assigned.
     */
    public static synchronized Cleaner getCleaner() {
        if (CLEANER == null) {
            CLEANER = Cleaner.create();
        }

        return CLEANER;
    }

    /**
     * Allows an external application {@link Cleaner} instance to be assigned to the
     * buffer utilities Cleaner instance which will then be used if a cleaner for a
     * {@link ProtonBuffer} is registered.
     *
     * @param cleaner
     * 		The cleaner to assign as the global {@link Cleaner} for buffer instances.
     */
    public static synchronized void setCleaner(Cleaner cleaner) {
        CLEANER = cleaner;
    }

    /**
     * Register a cleanup watch on the given object which is related to the {@link ProtonBuffer}
     * provided and ensure that if the object does not close the buffer by the time it becomes
     * unreachable that the buffer is closed.
     *
     * @param observed
     * 		The resource holder that if unreachable should release the given buffer
     * @param buffer
     * 		The buffer to be closed when the observed object is unreachable.
     *
     * @return a {@link Cleaner} instance the caller can invoke explicitly.
     */
    public static Cleanable registerCleanup(Object observed, ProtonBuffer buffer) {
        Objects.requireNonNull(observed, "The observed resource holder cannot be null");
        Objects.requireNonNull(buffer, "The buffer resource to be cleaned cannot be null");

        return getCleaner().register(observed, () -> {
            buffer.close();
        });
    }

    /**
     * Create a new String that is a copy of the boffer's readable bytes and
     * is defined by the {@link Charset} provided.
     *
     * @param buffer
     * 		The buffer to convert to a string
     * @param charset
     * 		The charset to use when creating the new string.
     *
     * @return a {@link String} that is a view of the given buffer's readable bytes.
     */
    public static String toString(ProtonBuffer buffer, Charset charset) {
        byte[] copy = new byte[buffer.getReadableBytes()];
        buffer.copyInto(buffer.getReadOffset(), copy, 0, copy.length);
        return new String(copy, 0, copy.length, charset);
    }

    /**
     * Given a {@link ProtonBuffer} returns an array containing a deep copy of the readable
     * bytes from the provided buffer.
     *
     * @param buffer
     * 		The buffer whose readable bytes are to be copied.
     *
     * @return a new array containing a copy of the readable bytes from the buffer.
     */
    public static byte[] toByteArray(ProtonBuffer buffer) {
        byte[] copy = new byte[buffer.getReadableBytes()];
        buffer.copyInto(buffer.getReadOffset(), copy, 0, copy.length);
        return copy;
    }

    /**
     * Given a {@link ByteBuffer} returns an array containing a copy of the readable bytes
     * from the provided buffer.
     *
     * @param buffer
     * 		The buffer whose readable bytes are to be copied.
     *
     * @return a new array containing a copy of the readable bytes from the buffer.
     */
    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy, 0, copy.length);
        return copy;
    }

    /**
     * Given a {@link ProtonBuffer} returns an {@link ByteBuffer} containing a copy of the
     * readable bytes from the provided buffer.
     *
     * @param buffer
     * 		The buffer whose readable bytes are to be copied.
     *
     * @return a new {@link ByteBuffer} containing a copy of the readable bytes from the buffer.
     */
    public static ByteBuffer toByteBuffer(ProtonBuffer buffer) {
        ByteBuffer copy = ByteBuffer.allocate(buffer.getReadableBytes());
        buffer.copyInto(buffer.getReadOffset(), copy, 0, copy.remaining());
        return copy;
    }

    /**
     * Given a byte value returns an array containing the given byte as the only entry.
     *
     * @param value
     * 		The value to wrap in an array instance.
     *
     * @return a new array containing the primitive value as the only array element.
     */
    public static byte[] toByteArray(byte value) {
        return writeByte(value, new byte[Byte.BYTES], 0);
    }

    /**
     * Given a short value returns an array containing the given byte as the only entry.
     *
     * @param value
     * 		The value to wrap in an array instance.
     *
     * @return a new array containing the primitive value as the only array element.
     */
    public static byte[] toByteArray(short value) {
        return writeShort(value, new byte[Short.BYTES], 0);
    }

    /**
     * Given an integer value returns an array containing the given byte as the only entry.
     *
     * @param value
     * 		The value to wrap in an array instance.
     *
     * @return a new array containing the primitive value as the only array element.
     */
    public static byte[] toByteArray(int value) {
        return writeInt(value, new byte[Integer.BYTES], 0);
    }

    /**
     * Given a long value returns an array containing the given byte as the only entry.
     *
     * @param value
     * 		The value to wrap in an array instance.
     *
     * @return a new array containing the primitive value as the only array element.
     */
    public static byte[] toByteArray(long value) {
        return writeLong(value, new byte[Long.BYTES], 0);
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeChar(char value, byte[] destination, int offset) {
        return writeShort((short) value, destination, offset);
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeByte(byte value, byte[] destination, int offset) {
        destination[offset] = value;

        return destination;
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeUnsignedByte(int value, byte[] destination, int offset) {
        destination[offset] = (byte) (value & 0xff);

        return destination;
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeShort(short value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeUnsignedShort(int value, byte[] destination, int offset) {
        return writeShort((short) (value & 0xffff), destination, offset);
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeInt(int value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 24);
        destination[offset++] = (byte) (value >>> 16);
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeUnsignedInt(long value, byte[] destination, int offset) {
        return writeInt((int) (value & 0x0000_0000_FFFF_FFFFL), destination, offset);
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeLong(long value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 56);
        destination[offset++] = (byte) (value >>> 48);
        destination[offset++] = (byte) (value >>> 40);
        destination[offset++] = (byte) (value >>> 32);
        destination[offset++] = (byte) (value >>> 24);
        destination[offset++] = (byte) (value >>> 16);
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeFloat(float value, byte[] destination, int offset) {
        return writeInt(Float.floatToIntBits(value), destination, offset);
    }

    /**
     * Writes the value given into the provided array at the specified offset returning to
     * destination once done.  The provided array must have enough space starting from the
     * given offset for the value to be encoded or an exception will be thrown.
     *
     * @param value
     * 		The value to be encoded into the given array
     * @param destination
     * 		The given array where the provided value should be written.
     * @param offset
     *      The offset into the array to start writing.
     *
     * @return the provided destination array.
     */
    public static byte[] writeDouble(double value, byte[] destination, int offset) {
        return writeLong(Double.doubleToLongBits(value), destination, offset);
    }

    /**
     * Reads a single byte from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static byte readByte(byte[] array, int offset) {
        return array[offset];
    }

    /**
     * Reads a single byte from the given array from the provided offset and returns it as an
     * integer that represents the unsigned byte value.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static int readUnsignedByte(byte[] array, int offset) {
        return array[offset] & 0xff;
    }

    /**
     * Reads a two byte short from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static short readShort(byte[] array, int offset) {
        return (short) ((array[offset++] & 0xFF) << 8 |
                        (array[offset++] & 0xFF) << 0);
    }

    /**
     * Reads a two byte short from the given array from the provided offset and return it
     * in an integer value that represents the unsigned short value.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static int readUnsignedShort(byte[] array, int offset) {
        return readShort(array, offset) & 0xffff;
    }

    /**
     * Reads a two byte UTF-16 character from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static char readChar(byte[] array, int offset) {
        return (char)readShort(array, offset);
    }

    /**
     * Reads a four byte integer from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static int readInt(byte[] array, int offset) {
        return (array[offset++] & 0xFF) << 24 |
               (array[offset++] & 0xFF) << 16 |
               (array[offset++] & 0xFF) << 8 |
               (array[offset++] & 0xFF) << 0;
    }

    /**
     * Reads a four byte integer from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static long readUnsignedInt(byte[] array, int offset) {
        return readInt(array, offset) & 0x0000_0000_FFFF_FFFFL;
    }

    /**
     * Reads an eight byte integer from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static long readLong(byte[] array, int offset) {
        return (long) (array[offset++] & 0xFF) << 56 |
               (long) (array[offset++] & 0xFF) << 48 |
               (long) (array[offset++] & 0xFF) << 40 |
               (long) (array[offset++] & 0xFF) << 32 |
               (long) (array[offset++] & 0xFF) << 24 |
               (long) (array[offset++] & 0xFF) << 16 |
               (long) (array[offset++] & 0xFF) << 8 |
               (long) (array[offset++] & 0xFF) << 0;
    }

    /**
     * Reads a four byte floating point value from the given array from the provided offset.
     *
     * @param array
     * 		The array to be read from
     * @param offset
     * 		The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static float readFloat(byte[] array, int offset) {
        return Float.intBitsToFloat(readInt(array, offset));
    }

    /**
     * Reads an eight byte double precision value from the given array from the provided offset.
     *
     * @param array  The array to be read from
     * @param offset The offset into the array to start reading from.
     *
     * @return the resulting value read from the array at the provided array offset.
     */
    public static double readDouble(byte[] array, int offset) {
        return Double.longBitsToDouble(readLong(array, offset));
    }

    /**
     * Checks the length value is not negative and throws an exception if it is.

     * @param length
     * 		The length value to be validated
     */
    public static void checkLength(int length) {
        if (length < 0) {
            throw new IndexOutOfBoundsException(String.format("The length value cannot be negative: %d", length));
        }
    }

    /**
     * Checks the value to determine if it less than zero and throws if it is.

     * @param value
     * 		The length value to be validated
     * @param description
     * 		A description value appended to the exception string.
     */
    public static void checkIsNotNegative(int value, String description) {
        if (value < 0) {
            throw new IllegalArgumentException(String.format("The value cannot be negative: %d : %s", value, description));
        }
    }

    /**
     * Checks the value to determine if it less than zero and throws if it is.

     * @param value
     * 		The length value to be validated
     * @param description
     * 		A description value appended to the exception string.
     */
    public static void checkIsNotNegative(long value, String description) {
        if (value < 0) {
            throw new IllegalArgumentException(String.format("The value cannot be negative: %d : %s", value, description));
        }
    }

    /**
     * Checks the index to determine if it less than zero and throws if it is.

     * @param value
     * 		The index value to be validated
     * @param description
     * 		A description value appended to the exception string.
     */
    public static void checkIndexIsNotNegative(int value, String description) {
        if (value < 0) {
            throw new IndexOutOfBoundsException(String.format("The index cannot be negative: %d : %s", value, description));
        }
    }

    /**
     * Checks the index to determine if it less than zero and throws if it is.

     * @param value
     * 		The index value to be validated
     * @param description
     * 		A description value appended to the exception string.
     */
    public static void checkIndexIsNotNegative(long value, String description) {
        if (value < 0) {
            throw new IndexOutOfBoundsException(String.format("The index cannot be negative: %d : %s", value, description));
        }
    }

    /**
     * Checks the argument to determine if it less than zero and throws if it is.

     * @param value
     * 		The argument value to be validated
     * @param description
     * 		A description value appended to the exception string.
     */
    public static void checkArgumentIsNotNegative(int value, String description) {
        if (value < 0) {
            throw new IllegalArgumentException(String.format("The argument cannot be negative: %d : %s", value, description));
        }
    }

    /**
     * Checks the argument to determine if it less than zero and throws if it is.

     * @param value
     * 		The argument value to be validated
     * @param description
     * 		A description value appended to the exception string.
     */
    public static void checkArgumentIsNotNegative(long value, String description) {
        if (value < 0) {
            throw new IllegalArgumentException(String.format("The argument cannot be negative: %d : %s", value, description));
        }
    }

    /**
     * Checks the offset value is not negative and throws an exception if it is.

     * @param offset
     * 		The offset value to be validated
     */
    public static void checkOffset(int offset) {
        if (offset < 0) {
            throw new IllegalArgumentException(String.format("The offset value cannot be negative: %d", offset));
        }
    }

    /**
     * Checks the given ProtonBuffer to see if it has already been closed.
     *
     * @param buffer
     * 		The buffer to check if closed
     */
    public static void checkIsClosed(ProtonBuffer buffer) {
        if (buffer.isClosed()) {
            throw new ProtonBufferClosedException("The buffer has already been closed: " + buffer);
        }
    }

    /**
     * Checks the given ProtonBuffer to see if it has already been closed.
     *
     * @param buffer
     * 		The buffer to check if closed
     */
    public static void checkIsReadOnly(ProtonBuffer buffer) {
        if (buffer.isReadOnly()) {
            throw new ProtonBufferReadOnlyException("The buffer is read-only and not writes are allowed: " + buffer);
        }
    }

    /**
     * Checks if a buffer can grow buffer the given amount or if that would exceed the
     * maximum allowed buffer size.
     *
     * @param currentCapacity
     * 		The buffer's current capacity.
     * @param additional
     *      The amount of new space that will be added to the buffer.
     */
    public static void checkBufferCanGrowTo(int currentCapacity, int additional) {
        checkIsNotNegative(additional, "The additional capacity value cannot be negative");
        final long newCapacity = (long) currentCapacity + (long) additional;
        if (newCapacity > MAX_BUFFER_CAPACITY) {
            throw new IllegalArgumentException(
                "Requested new buffer capacity {" + newCapacity +
                "} is greater than the max allowed size: " + MAX_BUFFER_CAPACITY);
        }
    }

    /**
     * Checks the implicit growth limit value for buffer implementations
     *
     * @param implicitCapacity
     * 		The intended implicit growth limit to assign
     * @param currentCapacity
     * 		The current buffer capacity
     */
    public static void checkImplicitGrowthLimit(int implicitCapacity, int currentCapacity) {
        if (implicitCapacity < currentCapacity) {
            throw new IndexOutOfBoundsException(
                "Implicit capacity limit (" + implicitCapacity +
                ") cannot be less than capacity (" + currentCapacity + ')');
        }
        if (implicitCapacity > MAX_BUFFER_CAPACITY) {
            throw new IndexOutOfBoundsException(
                "Implicit capacity limit (" + implicitCapacity +
                ") cannot be greater than max buffer size (" + MAX_BUFFER_CAPACITY + ')');
        }
    }

    public static ProtonBufferClosedException genericBufferIsClosed(ProtonBuffer buffer) {
        return new ProtonBufferClosedException("This buffer is closed: " + buffer);
    }

    public static ProtonBufferReadOnlyException genericBufferIsReadOnly(ProtonBuffer buffer) {
        return new ProtonBufferReadOnlyException("This buffer is read only: " + buffer);
    }

    public static IndexOutOfBoundsException genericOutOfBounds(ProtonBuffer buffer, int index) {
        return new IndexOutOfBoundsException(
            "Index " + index + " is out of bounds: [read 0 to " + buffer.getWriteOffset() +
            ", write 0 to " + buffer.capacity() + "].");
    }

    /**
     * Compares two {@link ProtonBuffer} instances for equality.
     *
     * @param left
     * 		The left hand side buffer to compare.
     * @param right
     * 		The right hand side buffer to compare.
     *
     * @return true if both buffers are equal.
     */
    public static boolean equals(ProtonBuffer left, ProtonBuffer right) {
        if (left == right) {
            return true;
        }

        final int length = left.getReadableBytes();

        if (length != right.getReadableBytes()) {
            return false;
        }

        return equalsImpl(left, left.getReadOffset(), right, right.getReadOffset(), length);
    }

    /**
     * Compares two {@link ProtonBuffer} instances for equality.
     *
     * @param left
     * 		The left hand side buffer to compare.
     * @param right
     * 		The right hand side buffer to compare.
     * @param length
     * 		The number of bytes in the two buffers to compare.
     *
     * @return true if both buffers are equal.
     */
    public static boolean equals(ProtonBuffer left, ProtonBuffer right, int length) {
        Objects.requireNonNull(left, "The left hand buffer cannot be null");
        Objects.requireNonNull(right, "The right hand buffer cannot be null");

        return equalsImpl(left, left.getReadOffset(), right, right.getReadOffset(), length);
    }

    /**
     * Compares two {@link ProtonBuffer} instances for equality.
     *
     * @param left
     * 		The left hand side buffer to compare.
     * @param leftStartIndex
     * 		The index in the readable bytes of the left buffer to start the comparison
     * @param right
     * 		The right hand side buffer to compare.
     * @param rightStartIndex
     * 		The index in the readable bytes of the right buffer to start the comparison
     * @param length
     * 		The number of bytes in the two buffers to compare.
     *
     * @return true if both buffers are equal.
     */
    public static boolean equals(ProtonBuffer left, int leftStartIndex, ProtonBuffer right, int rightStartIndex, int length) {
        Objects.requireNonNull(left, "The left hand buffer cannot be null");
        Objects.requireNonNull(right, "The right hand buffer cannot be null");

        checkArgumentIsNotNegative(leftStartIndex, "The left hand buffer start index cannot be negative");
        checkArgumentIsNotNegative(rightStartIndex, "The right hand buffer start index cannot be negative");
        checkArgumentIsNotNegative(length, "The comparison length cannot be negative");

        return equalsImpl(left, leftStartIndex, right, rightStartIndex, length);
    }

    private static boolean equalsImpl(ProtonBuffer left, int leftStartIndex, ProtonBuffer right, int rightStartIndex, int length) {
        if (left.getWriteOffset() - length < leftStartIndex || right.getWriteOffset() - length < rightStartIndex) {
            return false;
        }

        final int longCount = length >>> 3;
        final int byteCount = length & 7;

        for (int i = longCount; i > 0; i --) {
            if (left.getLong(leftStartIndex) != right.getLong(rightStartIndex)) {
                return false;
            }

            leftStartIndex += 8;
            rightStartIndex += 8;
        }

        for (int i = byteCount; i > 0; i --) {
            if (left.getByte(leftStartIndex) != right.getByte(rightStartIndex)) {
                return false;
            }

            leftStartIndex++;
            rightStartIndex++;
        }

        return true;
    }

    /**
     * Compute a hash code from the given {@link ProtonBuffer}.
     *
     * @param buffer
     * 		The buffer to compute the hash code for.
     *
     * @return the computed hash code for the given buffer
     */
    public static int hashCode(ProtonBuffer buffer) {
        final int readable = buffer.getReadableBytes();
        final int readableInts = readable >>> 2;
        final int remainingBytes = readable & 3;

        int hash = 1;
        int position = buffer.getReadOffset();

        for (int i = readableInts; i > 0; i --) {
            hash = 31 * hash + buffer.getInt(position);
            position += 4;
        }

        for (int i = remainingBytes; i > 0; i --) {
            hash = 31 * hash + buffer.getByte(position++);
        }

        if (hash == 0) {
            hash = 1;
        }

        return hash;
    }

    /**
     * Compares two {@link ProtonBuffer} instances.
     *
     * @param lhs
     * 		The left hand side buffer to compare
     * @param rhs
     *      The right hand side buffer to compare.
     *
     * @return the value 0 if {@code x == y}; a value less than 0 if {@code x < y}; and a value greater than 0 if {@code x > y}.
     */
    public static int compare(ProtonBuffer lhs, ProtonBuffer rhs) {
        int length = lhs.getReadOffset() + Math.min(lhs.getReadableBytes(), rhs.getReadableBytes());

        for (int i = lhs.getReadOffset(), j = rhs.getReadOffset(); i < length; i++, j++) {
            int cmp = Integer.compare(lhs.getByte(i) & 0xFF, rhs.getByte(j) & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }

        return lhs.getReadableBytes() - rhs.getReadableBytes();
    }

    /**
     * Writes the bytes that comprise the given {@link CharSequence} into the provided buffer
     * using the given {@link Charset} to map the characters to their bytes encoded values.
     * The write index of the destination buffer will be advanced by the resulting number
     * of bytes that represent the encoded {@link CharSequence}.
     *
     * @param source
     * 		The source {@link CharSequence} which will be read and converted into bytes.
     * @param destination
     * 		The {@link ProtonBuffer} where the converted bytes will be written.
     * @param charset
     * 		The {@link Charset} to use when mapping the characters to byte values.
     */
    static void writeCharSequence(CharSequence source, ProtonBuffer destination, Charset charset) {
        byte[] bytes = source.toString().getBytes(charset);
        destination.writeBytes(bytes);
    }

    /**
     * Reads a {@link CharSequence} from the given {@link ProtonBuffer} advancing the read offset
     * by the length value provided.
     *
     * @param source
     * 		The {@link ProtonBuffer} that will provide the bytes to create the {@link CharSequence}.
     * @param length
     * 		The number of bytes to copy starting at the given offset.
     * @param charset
     * 		The {@link Charset} to use to create the returned {@link CharSequence}.
     *
     * @return a {@link CharSequence} that is made up of the copied bytes using the provided {@link Charset}.
     */
    public static CharSequence readCharSequence(ProtonBuffer source, int length, Charset charset) {
        final CharSequence charSequence = copyToCharSequence(source, source.getReadOffset(), length, charset);
        source.advanceReadOffset(length);
        return charSequence;
    }

    /**
     * Copies the given length number of bytes from the provided buffer and returns a {@link CharSequence}
     * that is comprised of the characters of that sequence using the provided {@link Charset} to make
     * the transformation.
     *
     * @param source
     * 		The {@link ProtonBuffer} that will provide the bytes to create the {@link CharSequence}.
     * @param offset
     *		The offset into the given buffer where the copy of bytes should be started from.
     * @param length
     * 		The number of bytes to copy starting at the given offset.
     * @param charset
     * 		The {@link Charset} to use to create the returned {@link CharSequence}.
     *
     * @return a {@link CharSequence} that is made up of the copied bytes using the provided {@link Charset}.
     */
    public static CharSequence copyToCharSequence(ProtonBuffer source, int offset, int length, Charset charset) {
        byte[] data = new byte[length];
        source.copyInto(offset, data, 0, length);
        return new String(data, 0, length, charset);
    }

    /**
     * Creates a wrapper around the given allocator that prevents the close call
     * from having any effect.
     * <p>
     * Care should be taken to ensure that the allocator being wrapper is safe to leave
     * unclosed or that the code closes it purposefully in some other context as certain
     * wrapped allocators might require a close to free native resources.
     *
     * @param allocator
     * 		the {@link ProtonBufferAllocator} to wrap.
     *
     * @return a buffer allocator that cannot be closed.
     */
    public static ProtonBufferAllocator unclosable(ProtonBufferAllocator allocator) {
        return new UnclosableBufferAllocator(allocator);
    }

    // This wrapper relies on the default implementation of the close method being a no-op
    private static class UnclosableBufferAllocator implements ProtonBufferAllocator {

        private final ProtonBufferAllocator allocator;

        public UnclosableBufferAllocator(ProtonBufferAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public ProtonBuffer outputBuffer(int initialCapacity) {
            return allocator.outputBuffer(initialCapacity);
        }

        @Override
        public ProtonBuffer allocate() {
            return allocator.allocate();
        }

        @Override
        public ProtonBuffer allocate(int initialCapacity) {
            return allocator.allocate(initialCapacity);
        }

        @Override
        public ProtonBuffer allocateHeapBuffer() {
            return allocator.allocateHeapBuffer();
        }

        @Override
        public ProtonBuffer allocateHeapBuffer(int initialCapacity) {
            return allocator.allocateHeapBuffer(initialCapacity);
        }

        @Override
        public ProtonCompositeBuffer composite() {
            return allocator.composite();
        }

        @Override
        public ProtonCompositeBuffer composite(ProtonBuffer buffer) {
            return allocator.composite(buffer);
        }

        @Override
        public ProtonCompositeBuffer composite(ProtonBuffer[] buffers) {
            return allocator.composite(buffers);
        }

        @Override
        public ProtonBuffer copy(byte[] array, int offset, int length) {
            return allocator.copy(array, offset, length);
        }
    }
}
