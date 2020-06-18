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
package org.apache.qpid.proton4j.test.driver.codec.primitives;

import java.lang.annotation.Native;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public final class Decimal128 extends Number {

    private static final long serialVersionUID = -4863018398624288737L;

    /**
     * The number of bits used to represent an {@code Decimal128} value in two's
     * complement binary form.
     */
    @Native public static final int SIZE = 128;

    /**
     * The number of bytes used to represent a {@code Decimal128} value in two's
     * complement binary form.
     */
    public static final int BYTES = SIZE / Byte.SIZE;

    private final BigDecimal underlying;
    private final long msb;
    private final long lsb;

    public Decimal128(BigDecimal underlying) {
        this.underlying = underlying;

        this.msb = calculateMostSignificantBits(underlying);
        this.lsb = calculateLeastSignificantBits(underlying);
    }

    public Decimal128(final long msb, final long lsb) {
        this.msb = msb;
        this.lsb = lsb;

        this.underlying = calculateBigDecimal(msb, lsb);
    }

    public Decimal128(byte[] data) {
        this(ByteBuffer.wrap(data));
    }

    private Decimal128(final ByteBuffer buffer) {
        this(buffer.getLong(), buffer.getLong());
    }

    private static long calculateMostSignificantBits(final BigDecimal underlying) {
        return 0; // TODO.
    }

    private static long calculateLeastSignificantBits(final BigDecimal underlying) {
        return 0; // TODO.
    }

    private static BigDecimal calculateBigDecimal(final long msb, final long lsb) {
        return BigDecimal.ZERO; // TODO.
    }

    @Override
    public int intValue() {
        return underlying.intValue();
    }

    @Override
    public long longValue() {
        return underlying.longValue();
    }

    @Override
    public float floatValue() {
        return underlying.floatValue();
    }

    @Override
    public double doubleValue() {
        return underlying.doubleValue();
    }

    public long getMostSignificantBits() {
        return msb;
    }

    public long getLeastSignificantBits() {
        return lsb;
    }

    public byte[] asBytes() {
        byte[] bytes = new byte[16];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.putLong(getMostSignificantBits());
        buf.putLong(getLeastSignificantBits());
        return bytes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Decimal128 that = (Decimal128) o;

        if (lsb != that.lsb) {
            return false;
        }
        if (msb != that.msb) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (msb ^ (msb >>> 32));
        result = 31 * result + (int) (lsb ^ (lsb >>> 32));
        return result;
    }
}
