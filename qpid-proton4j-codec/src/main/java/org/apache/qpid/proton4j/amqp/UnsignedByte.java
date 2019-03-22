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

    public int compareTo(byte value) {
        return Byte.compare(underlying, value);
    }

    @Override
    public int compareTo(UnsignedByte o) {
        return Byte.compare(underlying, o.underlying);
    }

    @Override
    public int hashCode() {
        return underlying;
    }

    @Override
    public String toString() {
        return String.valueOf(intValue());
    }

    public static UnsignedByte valueOf(byte underlying) {
        final int index = (underlying) & 0xFF;
        return cachedValues[index];
    }

    public static UnsignedByte valueOf(final String value) throws NumberFormatException {
        int intVal = Integer.parseInt(value);
        if (intVal < 0 || intVal >= (1 << 8)) {
            throw new NumberFormatException("Value \"" + value + "\" lies outside the range [" + 0 + "-" + (1 << 8) + ").");
        }
        return valueOf((byte) intVal);
    }
}
