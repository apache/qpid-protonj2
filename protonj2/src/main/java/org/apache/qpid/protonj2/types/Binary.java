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

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;

/**
 * A Binary wrapper that presents an immutable view of a payload.
 */
public final class Binary {

    private final ProtonBuffer buffer;
    private int hashCode;

    /**
     * Creates an empty Binary instance.
     */
    public Binary() {
        this((ProtonBuffer) null);
    }

    /**
     * Creates an {@link Binary} that wraps the given buffer or copies it
     * if the given buffer is not read-only to preserves the immutable nature
     * of this Binary instance.
     *
     * @param buffer the {@link ProtonBuffer} to wrap or copy.
     */
    public Binary(ProtonBuffer buffer) {
        if (buffer != null && !buffer.isReadOnly()) {
            this.buffer = buffer.copy(true);
        } else {
            this.buffer = buffer;
        }

        if (buffer != null) {
            ProtonBufferUtils.registerCleanup(this, this.buffer);
        }
    }

    public Binary(final byte[] data) {
        this(data, 0, data.length);
    }

    public Binary(final byte[] data, final int offset, final int length) {
        this.buffer = ProtonBufferAllocator.defaultAllocator().copy(data, offset, length).convertToReadOnly();
    }

    public Binary copy() {
        if (buffer == null) {
            return new Binary();
        } else {
            return new Binary(buffer.copy());
        }
    }

    /**
     * Creates a <code>byte[]</code> that contains a copy of the bytes wrapped by this
     * {@link Binary} instance. If the Binary has no backing buffer than this method
     * returns <code>null</code>.
     *
     * @return a byte array based copy of the Binary instance backing bytes
     */
    public byte[] asByteArray() {
        byte[] result = null;

        if (buffer != null) {
            result = new byte[buffer.getReadableBytes()];
            buffer.copyInto(buffer.getReadOffset(), result, 0, result.length);
        }

        return result;
    }

    /**
     * Creates a read-only {@link ByteBuffer} that contains a copy of the bytes
     * wrapped by this Binary instance. If the Binary has no backing buffer than
     * this method returns <code>null</code>.
     *
     * @return a {@link ByteBuffer} copy of the Binary instance backing bytes
     */
    public ByteBuffer asByteBuffer() {
        ByteBuffer result = null;

        if (buffer != null) {
            result = ByteBuffer.allocate(buffer.getReadableBytes());
            buffer.copyInto(buffer.getReadOffset(), result, 0, result.remaining());
            result = result.asReadOnlyBuffer();
        }

        return result;
    }

    /**
     * Creates a read-only {@link ProtonBuffer} that contains a copy of the bytes
     * wrapped by this Binary instance. If the Binary has no backing buffer than
     * this method returns <code>null</code>.
     *
     * @return a {@link ProtonBuffer} copy of the Binary instance backing bytes
     */
    public ProtonBuffer asProtonBuffer() {
        return buffer == null ? null : buffer.copy(true);
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
