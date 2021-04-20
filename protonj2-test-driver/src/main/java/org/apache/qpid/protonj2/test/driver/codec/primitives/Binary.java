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
package org.apache.qpid.protonj2.test.driver.codec.primitives;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Binary {

    private final byte[] buffer;
    private int hashCode;

    public Binary() {
        this.buffer = null;
    }

    public Binary(final byte[] data) {
        this(data, 0, data.length);
    }

    public Binary(final byte[] data, final int offset, final int length) {
        this.buffer = Arrays.copyOfRange(data, offset, offset + length);
    }

    public Binary copy() {
        if (buffer == null) {
            return new Binary();
        } else {
            return new Binary(Arrays.copyOf(buffer, buffer.length));
        }
    }

    public byte[] arrayCopy() {
        byte[] dataCopy = null;

        if (buffer != null) {
            dataCopy = Arrays.copyOf(buffer, buffer.length);
        }

        return dataCopy;
    }

    public ByteBuffer asByteBuffer() {
        return buffer != null ? ByteBuffer.wrap(buffer) : null;
    }

    @Override
    public final int hashCode() {
        int hc = hashCode;
        if (hc == 0 && buffer != null) {
            hashCode = buffer.hashCode();
        }
        return hc;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Binary other = (Binary) o;
        if (getLength() != other.getLength()) {
            return false;
        }

        if (buffer == null) {
            return other.buffer == null;
        }

        return Arrays.equals(buffer, other.buffer);
    }

    public boolean hasArray() {
        return buffer != null;
    }

    public byte[] getArray() {
        return buffer;
    }

    public int getLength() {
        return buffer != null ? buffer.length : 0;
    }

    @Override
    public String toString() {
        if (buffer == null) {
            return "";
        }

        StringBuilder str = new StringBuilder();

        for (int i = 0; i < getLength(); i++) {
            byte c = buffer[i];

            if (c > 31 && c < 127 && c != '\\') {
                str.append((char) c);
            } else {
                str.append(String.format("\\x%02x", c));
            }
        }

        return str.toString();
    }
}
