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

    public int compareTo(short value) {
        return Integer.signum(intValue() - Short.toUnsignedInt(value));
    }

    @Override
    public int compareTo(UnsignedShort o) {
        return Integer.signum(intValue() - o.intValue());
    }

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

    public static UnsignedShort valueOf(final short underlying) {
        if ((underlying & 0xFF00) == 0) {
            return cachedValues[underlying];
        } else {
            return new UnsignedShort(underlying);
        }
    }

    public static UnsignedShort valueOf(final int intValue) {
        if (intValue < 0L || intValue >= (1L << 16)) {
            throw new NumberFormatException("Value \"" + intValue + "\" lies outside the range [" + 0L + "-" + (1L << 16) + ").");
        }
        return valueOf((short) intValue);
    }

    public static UnsignedShort valueOf(final String value) {
        int intVal = Integer.parseInt(value);

        if (intVal < 0 || intVal >= (1 << 16)) {
            throw new NumberFormatException(
                "Value \"" + value + "\" lies outside the range [" + 0 + "-" + (1 << 16) + ").");
        }

        return valueOf((short) intVal);
    }
}
