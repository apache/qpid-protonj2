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
        return (underlying) & 0xFFFFFFFFl;
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

    public int compreTo(int value) {
        return Integer.compareUnsigned(underlying, value);
    }

    public int compareTo(long o) {
        return Long.signum(longValue() - o);
    }

    @Override
    public int compareTo(UnsignedInteger o) {
        return Long.signum(longValue() - o.longValue());
    }

    public static int compare(int left, int right) {
        return Integer.compareUnsigned(left, right);
    }

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

    public static UnsignedInteger valueOf(int underlying) {
        if ((underlying & 0xFFFFFF00) == 0) {
            return cachedValues[underlying];
        } else {
            return new UnsignedInteger(underlying);
        }
    }

    public UnsignedInteger add(final UnsignedInteger i) {
        int val = underlying + i.underlying;
        return UnsignedInteger.valueOf(val);
    }

    public UnsignedInteger subtract(final UnsignedInteger i) {
        int val = underlying - i.underlying;
        return UnsignedInteger.valueOf(val);
    }

    public static UnsignedInteger valueOf(final String value) {
        long longVal = Long.parseLong(value);
        return valueOf(longVal);
    }

    public static UnsignedInteger valueOf(final long longVal) {
        if (longVal < 0L || longVal >= (1L << 32)) {
            throw new NumberFormatException("Value \"" + longVal + "\" lies outside the range [" + 0L + "-" + (1L << 32) + ").");
        }
        return valueOf((int) longVal);
    }
}
