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

import java.nio.ByteBuffer;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

public final class Binary {

    private final ProtonBuffer buffer;
    private int hashCode;

    public Binary() {
        this((ProtonBuffer) null);
    }

    public Binary(ProtonBuffer buffer) {
        this.buffer = buffer;
    }

    public Binary(final byte[] data) {
        this(data, 0, data.length);
    }

    public Binary(final byte[] data, final int offset, final int length) {
        this.buffer = ProtonByteBufferAllocator.DEFAULT.wrap(data, offset, length).setWriteIndex(length);
    }

    public Binary copy() {
        if (buffer == null) {
            return new Binary();
        } else {
            return new Binary(buffer.copy());
        }
    }

    public byte[] arrayCopy() {
        byte[] dataCopy = null;
        if (buffer != null) {
            dataCopy = new byte[buffer.getReadableBytes()];
            buffer.getBytes(buffer.getReadIndex(), dataCopy);
        }

        return dataCopy;
    }

    public ByteBuffer asByteBuffer() {
        return buffer != null ? buffer.toByteBuffer() : null;
    }

    public ProtonBuffer asProtonBuffer() {
        return buffer;
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

        return buffer.equals(other.buffer);
    }

    public boolean hasArray() {
        return buffer != null ? buffer.hasArray() : false;
    }

    public int getArrayOffset() {
        return buffer != null ? buffer.getArrayOffset() : 0;
    }

    public byte[] getArray() {
        return buffer != null ? buffer.getArray() : null;
    }

    public int getLength() {
        return buffer != null ? buffer.getReadableBytes() : 0;
    }

    @Override
    public String toString() {
        if (buffer == null) {
            return "";
        }

        StringBuilder str = new StringBuilder();

        for (int i = 0; i < getLength(); i++) {
            byte c = buffer.getByte(i);

            if (c > 31 && c < 127 && c != '\\') {
                str.append((char) c);
            } else {
                str.append(String.format("\\x%02x", c));
            }
        }

        return str.toString();
    }
}
