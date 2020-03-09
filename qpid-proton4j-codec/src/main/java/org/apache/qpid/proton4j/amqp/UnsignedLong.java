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
package org.apache.qpid.proton4j.amqp;

import java.math.BigInteger;

public final class UnsignedLong extends Number implements Comparable<UnsignedLong> {

    private static final long serialVersionUID = -5901821450224443596L;
    private static final BigInteger TWO_TO_THE_SIXTY_FOUR = new BigInteger(
        new byte[] { (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0 });
    private static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

    private static final UnsignedLong[] cachedValues = new UnsignedLong[256];

    static {
        for (int i = 0; i < 256; i++) {
            cachedValues[i] = new UnsignedLong(i);
        }
    }

    public static final UnsignedLong ZERO = cachedValues[0];

    private final long underlying;

    public UnsignedLong(long underlying) {
        this.underlying = underlying;
    }

    @Override
    public int intValue() {
        return (int) underlying;
    }

    @Override
    public long longValue() {
        return underlying;
    }

    public BigInteger bigIntegerValue() {
        if (underlying >= 0L) {
            return BigInteger.valueOf(underlying);
        } else {
            return TWO_TO_THE_SIXTY_FOUR.add(BigInteger.valueOf(underlying));
        }
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

        UnsignedLong that = (UnsignedLong) o;

        if (underlying != that.underlying) {
            return false;
        }

        return true;
    }

    /**
     * Compares the give long value to this {@link UnsignedLong} numerically treating the given value as unsigned.
     *
     * @param value
     *       the long to compare to this {@link UnsignedLong} instance.
     *
     * @return the value 0 if this == value; a value less than 0 if this &lt; value as unsigned values; and a value
     *         greater than 0 if this &gt; value as unsigned values
     */
    public int compareTo(long value) {
        return Long.compareUnsigned(underlying, value);
    }

    @Override
    public int compareTo(UnsignedLong o) {
        return bigIntegerValue().compareTo(o.bigIntegerValue());
    }

    /**
     * Compares two long values numerically treating the values as unsigned.
     *
     * @param left
     *       the left hand side long to compare
     * @param right
     *       the right hand side long to compare
     *
     * @return the value 0 if left == right; a value less than 0 if left &lt; right as unsigned values; and a value
     *         greater than 0 if left &gt; right as unsigned values
     */
    public static int compare(long left, long right) {
        return Long.compareUnsigned(left, right);
    }

    @Override
    public int hashCode() {
        return (int) (underlying ^ (underlying >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(bigIntegerValue());
    }

    /**
     * Returns an UnsignedLong instance representing the specified int value. This method always returns
     * a cached {@link UnsignedLong} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedLong#UnsignedLong(long)} which will always create a new
     * instance of the {@link UnsignedLong} type.
     *
     * @param value
     *      The long value to return as an {@link UnsignedLong} instance.
     *
     * @return an {@link UnsignedLong} instance representing the unsigned view of the given long.
     */
    public static UnsignedLong valueOf(long value) {
        if ((value & 0xFFL) == value) {
            return cachedValues[(int) value];
        } else {
            return new UnsignedLong(value);
        }
    }

    /**
     * Returns an {@link UnsignedLong} instance representing the specified {@link String} value. This method
     * always returns a cached {@link UnsignedLong} instance for values in the range [0...255] which can save
     * space and time over calling the constructor {@link UnsignedLong#UnsignedLong(long)} which will always
     * create a new instance of the {@link UnsignedLong} type.
     *
     * @param value
     *      The String value to return as an {@link UnsignedLong} instance.
     *
     * @return an {@link UnsignedLong} instance representing the unsigned view of the given String.
     *
     * @throws NumberFormatException if the given value is greater than the max {@link UnsignedLong} value possible
     *                               or the {@link String} value given cannot be converted to a numeric value.
     */
    public static UnsignedLong valueOf(final String value) {
        BigInteger bigInt = new BigInteger(value);

        return valueOf(bigInt);
    }

    /**
     * Returns an {@link UnsignedLong} instance representing the specified {@link BigInteger} value. This method
     * always returns a cached {@link UnsignedLong} instance for values in the range [0...255] which can save
     * space and time over calling the constructor {@link UnsignedLong#UnsignedLong(long)} which will always
     * create a new instance of the {@link UnsignedLong} type.
     *
     * @param value
     *      The {@link BigInteger} value to return as an {@link UnsignedLong} instance.
     *
     * @return an {@link UnsignedLong} instance representing the unsigned view of the given {@link BigInteger}.
     *
     * @throws NumberFormatException if the given value is greater than the max {@link UnsignedLong} value possible.
     */
    public static UnsignedLong valueOf(BigInteger value) {
        if (value.signum() == -1 || value.bitLength() > 64) {
            throw new NumberFormatException("Value \"" + value + "\" lies outside the range [0 - 2^64).");
        } else if (value.compareTo(LONG_MAX_VALUE) >= 0) {
            return UnsignedLong.valueOf(value.longValue());
        } else {
            return UnsignedLong.valueOf(TWO_TO_THE_SIXTY_FOUR.subtract(value).negate().longValue());
        }
    }
}
