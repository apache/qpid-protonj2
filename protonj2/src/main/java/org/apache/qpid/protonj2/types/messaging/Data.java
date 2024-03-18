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
package org.apache.qpid.protonj2.types.messaging;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBufferAllocator;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Data implements Section<byte[]> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");

    private final ProtonBuffer buffer;

    private Binary cachedBinary;

    public Data(Binary binary) {
        this.buffer = binary != null ? binary.asProtonBuffer() : null;
        this.cachedBinary = binary;
    }

    public Data(ProtonBuffer buffer) {
        this.buffer = buffer != null ? buffer.convertToReadOnly() : null;
    }

    public Data(byte[] value) {
        // Creates heap buffers that will be cleaned on GC
        this.buffer = value != null ? ProtonByteArrayBufferAllocator.wrapped(value, 0, value.length).convertToReadOnly() : null;
    }

    public Data(byte[] value, int offset, int length) {
        // Creates heap buffers that will be cleaned on GC
        this.buffer = value != null ? ProtonByteArrayBufferAllocator.wrapped(value, offset, length).convertToReadOnly() : null;
    }

    public Data copy() {
        return new Data(buffer == null ? null : buffer.copy(true));
    }

    public boolean hasBinary() {
        return buffer != null;
    }

    public Binary getBinary() {
        if (cachedBinary != null || buffer == null) {
            return cachedBinary;
        } else {
            return cachedBinary = new Binary(buffer.copy(true));
        }
    }

    /**
     * @return the number of actual bytes carried in this Data section.
     */
    public int getDataLength() {
        return buffer == null ? 0 : buffer.getReadableBytes();
    }

    /**
     * Returns the {@link ProtonBuffer} that contains the bytes carried in the {@link Data} section.
     * If the section carries no bytes then this method returns null.  This method allows the {@link Data}
     * section to be considered a carrier of {@link ProtonBuffer} types instead of the {@link Binary}
     * value it will encode as part of its body and avoids creation of a Binary object when one is not
     * needed. If a Binary instance is required then calling the {@link #getBinary()} method will create
     * an instance that wraps the internal {@link ProtonBuffer}.
     *
     * @return the {@link ProtonBuffer} that back this Data section.
     */
    public ProtonBuffer getBuffer() {
        return buffer == null ? null : buffer.copy(true);
    }

    /**
     * Copies the binary payload of this Data section info the given target buffer.
     *
     * @param target
     * 		The buffer where the binary payload is written to.
     *
     * @return this {@link Data} section instance.
     */
    public Data copyTo(ProtonBuffer target) {
        if (buffer != null) {
            buffer.copyInto(buffer.getReadOffset(), target, target.getWriteOffset(), buffer.getReadableBytes());
            target.advanceWriteOffset(buffer.getReadableBytes());
        }

        return this;
    }

    /**
     * Returns the backing array for this Data {@link Section} copying the contents into a new array
     * instance if the backing array in the contained Binary is a subsequence of a larger referenced
     * array instance.
     *
     * @return the byte array view of this Data {@link Section} {@link Binary} payload.
     */
    @Override
    public byte[] getValue() {
        byte[] dataCopy = null;
        if (buffer != null) {
            dataCopy = new byte[buffer.getReadableBytes()];
            buffer.copyInto(buffer.getReadOffset(), dataCopy, 0, dataCopy.length);
        }

        return dataCopy;
    }

    @Override
    public String toString() {
        if (buffer == null) {
            return "";
        }

        StringBuilder str = new StringBuilder();

        str.append("Data{ ");

        for (int i = 0; i < buffer.getReadableBytes(); i++) {
            byte c = buffer.getByte(i);

            if (c > 31 && c < 127 && c != '\\') {
                str.append((char) c);
            } else {
                str.append(String.format("\\x%02x", c));
            }
        }

        str.append(" }");

        return str.toString();
    }

    @Override
    public SectionType getType() {
        return SectionType.Data;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((buffer == null) ? 0 : buffer.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        Data other = (Data) obj;
        if (buffer == null) {
            return other.buffer == null;
        }

        return buffer.equals(other.buffer);
    }
}
