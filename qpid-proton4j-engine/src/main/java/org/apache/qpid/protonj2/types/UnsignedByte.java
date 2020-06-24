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
package org.apache.qpid.protonj2.types;

public final class UnsignedByte extends Number implements Comparable<UnsignedByte> {

    private static final long serialVersionUID = 6271683731751283403L;
    private static final UnsignedByte[] cachedValues = new UnsignedByte[256];

    static {
        for (int i = 0; i < 256; i++) {
            cachedValues[i] = new UnsignedByte((byte) i);
        }
    }

    private final byte underlying;

    public UnsignedByte(byte underlying) {
        this.underlying = underlying;
    }

    @Override
    public byte byteValue() {
        return underlying;
    }

    @Override
    public short shortValue() {
        return (short) intValue();
    }

    @Override
    public int intValue() {
        return (underlying) & 0xFF;
    }

    @Override
    public long longValue() {
        return (underlying) & 0xFFl;
    }

    @Override
    public float floatValue() {
        return longValue();
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnsignedByte that = (UnsignedByte) o;

        if (underlying != that.underlying) {
            return false;
        }

        return true;
    }

    /**
     * Compares the give byte value to this unsigned byte numerically treating the given value as unsigned.
     *
     * @param value
     *       the byte to compare to this unsigned byte instance.
     *
     * @return the value 0 if this == value; a value less than 0 if this &lt; value as unsigned values; and a value
     *         greater than 0 if this &gt; value as unsigned values
     */
    public int compareTo(byte value) {
        return compare(underlying, value);
    }

    @Override
    public int compareTo(UnsignedByte o) {
        return compare(underlying, o.underlying);
    }

    /**
     * Compares two short values numerically treating the values as unsigned.
     *
     * @param left
     *       the left hand side short to compare
     * @param right
     *       the right hand side short to compare
     *
     * @return the value 0 if left == right; a value less than 0 if left &lt; right as unsigned values; and a value
     *         greater than 0 if left &gt; right as unsigned values
     */
    public static int compare(byte left, byte right) {
        return Integer.compareUnsigned(Byte.toUnsignedInt(left), Byte.toUnsignedInt(right));
    }

    @Override
    public int hashCode() {
        return underlying;
    }

    @Override
    public String toString() {
        return String.valueOf(intValue());
    }

    /**
     * Returns an UnsignedByte instance representing the specified byte value. This method always returns
     * a cached {@link UnsignedByte} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedByte#UnsignedByte(byte)} which will always create a new
     * instance of the {@link UnsignedByte} type.
     *
     * @param value
     *      The byte value to return as an {@link UnsignedByte} instance.
     *
     * @return an {@link UnsignedByte} instance representing the unsigned view of the given byte.
     */
    public static UnsignedByte valueOf(byte value) {
        final int index = (value) & 0xFF;
        return cachedValues[index];
    }

    /**
     * Returns an {@link UnsignedByte} instance representing the specified {@link String} value. This method always
     * returns a cached {@link UnsignedByte} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedByte#UnsignedByte(byte)} which will always create a new instance
     * of the {@link UnsignedByte} type.
     *
     * @param value
     *      The byte value to return as an {@link UnsignedByte} instance.
     *
     * @return an {@link UnsignedByte} instance representing the unsigned view of the given byte.
     *
     * @throws NumberFormatException if the given {@link String} value given cannot be converted to a numeric value.
     */
    public static UnsignedByte valueOf(final String value) throws NumberFormatException {
        int intVal = Integer.parseInt(value);
        if (intVal < 0 || intVal >= (1 << 8)) {
            throw new NumberFormatException("Value \"" + value + "\" lies outside the range [" + 0 + "-" + (1 << 8) + ").");
        }
        return valueOf((byte) intVal);
    }

    /**
     * Returns a {@code short} that represents the unsigned view of the given {@code byte} value.
     *
     * @param value
     *      The {@code short} whose unsigned value should be converted to a long.
     *
     * @return a positive {@code short} value that represents the given {@code byte} as unsigned.
     */
    public static short toUnsignedShort(byte value) {
        return (short) (value & 0xff);
    }

    /**
     * Returns a {@code int} that represents the unsigned view of the given {@code byte} value.
     *
     * @param value
     *      The {@code int} whose unsigned value should be converted to a long.
     *
     * @return a positive {@code int} value that represents the given {@code short} as unsigned.
     */
    public static int toUnsignedInt(byte value) {
        return Byte.toUnsignedInt(value);
    }

    /**
     * Returns a {@code long} that represents the unsigned view of the given {@code byte} value.
     *
     * @param value
     *      The {@code long} whose unsigned value should be converted to a long.
     *
     * @return a positive {@code long} value that represents the given {@code byte} as unsigned.
     */
    public static long toUnsignedLong(byte value) {
        return Byte.toUnsignedLong(value);
    }
}
