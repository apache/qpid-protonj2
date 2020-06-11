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
package org.apache.qpid.proton4j.types;

public final class UnsignedInteger extends Number implements Comparable<UnsignedInteger> {

    private static final long serialVersionUID = 3042749852724499995L;
    private static final UnsignedInteger[] cachedValues = new UnsignedInteger[256];

    static {
        for (int i = 0; i < 256; i++) {
            cachedValues[i] = new UnsignedInteger(i);
        }
    }

    public static final UnsignedInteger ZERO = cachedValues[0];
    public static final UnsignedInteger ONE = cachedValues[1];
    public static final UnsignedInteger MAX_VALUE = new UnsignedInteger(0xffffffff);

    private final int underlying;

    public UnsignedInteger(int underlying) {
        this.underlying = underlying;
    }

    @Override
    public int intValue() {
        return underlying;
    }

    @Override
    public long longValue() {
        return Integer.toUnsignedLong(underlying);
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

        UnsignedInteger that = (UnsignedInteger) o;

        if (underlying != that.underlying) {
            return false;
        }

        return true;
    }

    /**
     * Compares the give integer value to this unsigned integer numerically treating the given value as unsigned.
     *
     * @param value
     *       the integer to compare to this unsigned integer instance.
     *
     * @return the value 0 if this == value; a value less than 0 if this &lt; value as unsigned values; and a value
     *         greater than 0 if this &gt; value as unsigned values
     */
    public int compareTo(int value) {
        return Integer.compareUnsigned(underlying, value);
    }

    /**
     * Compares the give long value to this unsigned integer numerically treating the given value as unsigned.
     *
     * @param value
     *       the long to compare to this unsigned integer instance.
     *
     * @return the value 0 if this == value; a value less than 0 if this &lt; value as unsigned values; and a value
     *         greater than 0 if this &gt; value as unsigned values
     */
    public int compareTo(long value) {
        return Long.compareUnsigned(longValue(), value);
    }

    @Override
    public int compareTo(UnsignedInteger value) {
        return Long.compareUnsigned(longValue(), value.longValue());
    }

    /**
     * Compares two integer values numerically treating the values as unsigned.
     *
     * @param left
     *       the left hand side integer to compare
     * @param right
     *       the right hand side integer to compare
     *
     * @return the value 0 if left == right; a value less than 0 if left &lt; right as unsigned values; and a value
     *         greater than 0 if left &gt; right as unsigned values
     */
    public static int compare(int left, int right) {
        return Integer.compareUnsigned(left, right);
    }

    /**
     * Compares two long values numerically treating the values as unsigned.
     *
     * @param left
     *       the left hand side long value to compare
     * @param right
     *       the right hand side long value to compare
     *
     * @return the value 0 if left == right; a value less than 0 if left &lt; right as unsigned values; and a value
     *         greater than 0 if left &gt; right as unsigned values
     */
    public static int compare(long left, long right) {
        return Long.compareUnsigned(left, right);
    }

    @Override
    public int hashCode() {
        return underlying;
    }

    @Override
    public String toString() {
        return String.valueOf(longValue());
    }

    /**
     * Returns an UnsignedInteger instance representing the specified int value. This method always returns
     * a cached {@link UnsignedInteger} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedInteger#UnsignedInteger(int)} which will always create a new
     * instance of the {@link UnsignedInteger} type.
     *
     * @param value
     *      The int value to return as an {@link UnsignedInteger} instance.
     *
     * @return an {@link UnsignedInteger} instance representing the unsigned view of the given int.
     */
    public static UnsignedInteger valueOf(int value) {
        if ((value & 0xFFFFFF00) == 0) {
            return cachedValues[value];
        } else {
            return new UnsignedInteger(value);
        }
    }

    /**
     * Adds the value of the given {@link UnsignedInteger} to this instance and return a new {@link UnsignedInteger}
     * instance that represents the newly computed value.
     *
     * @param value
     *      The {@link UnsignedInteger} whose underlying value should be added to this instance's value.
     *
     * @return a new immutable {@link UnsignedInteger} resulting from the addition of this with the given value.
     */
    public UnsignedInteger add(final UnsignedInteger value) {
        int val = underlying + value.underlying;
        return UnsignedInteger.valueOf(val);
    }

    /**
     * Subtract the value of the given {@link UnsignedInteger} from this instance and return a new {@link UnsignedInteger}
     * instance that represents the newly computed value.
     *
     * @param value
     *      The {@link UnsignedInteger} whose underlying value should be subtracted to this instance's value.
     *
     * @return a new immutable {@link UnsignedInteger} resulting from the subtraction the given value from this.
     */
    public UnsignedInteger subtract(final UnsignedInteger value) {
        int val = underlying - value.underlying;
        return UnsignedInteger.valueOf(val);
    }

    /**
     * Returns an {@link UnsignedInteger} instance representing the specified {@link String} value. This method
     * always returns a cached {@link UnsignedInteger} instance for values in the range [0...255] which can save
     * space and time over calling the constructor {@link UnsignedInteger#UnsignedInteger(int)} which will always
     * create a new instance of the {@link UnsignedInteger} type.
     *
     * @param value
     *      The String value to return as an {@link UnsignedInteger} instance.
     *
     * @return an {@link UnsignedInteger} instance representing the unsigned view of the given String.
     *
     * @throws NumberFormatException if the given value is greater than the max {@link UnsignedInteger} value possible
     *                               or the {@link String} value given cannot be converted to a numeric value.
     */
    public static UnsignedInteger valueOf(final String value) {
        long longVal = Long.parseLong(value);
        return valueOf(longVal);
    }

    /**
     * Returns an UnsignedInteger instance representing the specified long value. This method always returns
     * a cached {@link UnsignedInteger} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedInteger#UnsignedInteger(int)} which will always create a new
     * instance of the {@link UnsignedInteger} type.
     *
     * @param value
     *      The long value to return as an {@link UnsignedInteger} instance.
     *
     * @return an {@link UnsignedInteger} instance representing the unsigned view of the given long.
     *
     * @throws NumberFormatException if the given value is greater than the max {@link UnsignedInteger} value possible.
     */
    public static UnsignedInteger valueOf(final long value) {
        if (value < 0L || value >= (1L << 32)) {
            throw new NumberFormatException("Value \"" + value + "\" lies outside the range [" + 0L + "-" + (1L << 32) + ").");
        }
        return valueOf((int) value);
    }

    /**
     * Returns a {@code long} that represents the unsigned view of the given {@code int} value.
     *
     * @param value
     *      The integer whose unsigned value should be converted to a long.
     *
     * @return a positive long value that represents the given {@code int} as unsigned.
     */
    public static long toUnsignedLong(int value) {
        return Integer.toUnsignedLong(value);
    }
}
