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

public final class UnsignedShort extends Number implements Comparable<UnsignedShort> {

    private static final long serialVersionUID = 6006944990203315231L;
    private static final UnsignedShort[] cachedValues = new UnsignedShort[256];

    static {
        for (short i = 0; i < 256; i++) {
            cachedValues[i] = new UnsignedShort(i);
        }
    }

    public static final UnsignedShort MAX_VALUE = new UnsignedShort((short) -1);

    private final short underlying;

    public UnsignedShort(short underlying) {
        this.underlying = underlying;
    }

    @Override
    public short shortValue() {
        return underlying;
    }

    @Override
    public int intValue() {
        return Short.toUnsignedInt(underlying);
    }

    @Override
    public long longValue() {
        return Short.toUnsignedLong(underlying);
    }

    @Override
    public float floatValue() {
        return intValue();
    }

    @Override
    public double doubleValue() {
        return intValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnsignedShort that = (UnsignedShort) o;

        if (underlying != that.underlying) {
            return false;
        }

        return true;
    }

    /**
     * Compares the give short value to this unsigned short numerically treating the given value as unsigned.
     *
     * @param value
     *       the short to compare to this unsigned short instance.
     *
     * @return the value 0 if this == value; a value less than 0 if this &lt; value as unsigned values; and a value
     *         greater than 0 if this &gt; value as unsigned values
     */
    public int compareTo(short value) {
        return Integer.signum(intValue() - Short.toUnsignedInt(value));
    }

    @Override
    public int compareTo(UnsignedShort value) {
        return Integer.signum(intValue() - value.intValue());
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
    public static int compare(short left, short right) {
        return Integer.compareUnsigned(Short.toUnsignedInt(left), Short.toUnsignedInt(right));
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
     * Returns an UnsignedShort instance representing the specified short value. This method always returns
     * a cached {@link UnsignedShort} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedShort#UnsignedShort(short)} which will always create a new
     * instance of the {@link UnsignedShort} type.
     *
     * @param value
     *      The short value to return as an {@link UnsignedShort} instance.
     *
     * @return an {@link UnsignedShort} instance representing the unsigned view of the given short.
     */
    public static UnsignedShort valueOf(final short value) {
        if ((value & 0xFF00) == 0) {
            return cachedValues[value];
        } else {
            return new UnsignedShort(value);
        }
    }

    /**
     * Returns an UnsignedShort instance representing the specified int value. This method always returns
     * a cached {@link UnsignedShort} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedShort#UnsignedShort(short)} which will always create a new
     * instance of the {@link UnsignedShort} type.
     *
     * @param value
     *      The short value to return as an {@link UnsignedShort} instance.
     *
     * @return an {@link UnsignedShort} instance representing the unsigned view of the given short.
     *
     * @throws NumberFormatException if the given value is greater than the max {@link UnsignedShort} value possible.
     */
    public static UnsignedShort valueOf(final int value) {
        if (value < 0L || value >= (1L << 16)) {
            throw new NumberFormatException("Value \"" + value + "\" lies outside the range [" + 0L + "-" + (1L << 16) + ").");
        }
        return valueOf((short) value);
    }

    /**
     * Returns an UnsignedShort instance representing the specified {@link String} value. This method always returns
     * a cached {@link UnsignedShort} instance for values in the range [0...255] which can save space and time
     * over calling the constructor {@link UnsignedShort#UnsignedShort(short)} which will always create a new
     * instance of the {@link UnsignedShort} type.
     *
     * @param value
     *      The String value to return as an {@link UnsignedShort} instance.
     *
     * @return an {@link UnsignedShort} instance representing the unsigned view of the given String.
     *
     * @throws NumberFormatException if the given value is greater than the max {@link UnsignedShort} value possible
     *                               or the {@link String} value given cannot be converted to a numeric value.
     */
    public static UnsignedShort valueOf(final String value) {
        int intVal = Integer.parseInt(value);

        if (intVal < 0 || intVal >= (1 << 16)) {
            throw new NumberFormatException(
                "Value \"" + value + "\" lies outside the range [" + 0 + "-" + (1 << 16) + ").");
        }

        return valueOf((short) intVal);
    }
}
