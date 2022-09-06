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

import java.util.Arrays;
import java.util.Objects;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * An abstraction around Transfer frames Binary delivery tag which can be used to
 * provide additional features to code sending transfers such as tag pooling etc.
 *
 * @see ProtonBuffer
 * @see Transfer
 */
public interface DeliveryTag {

    /**
     * @return the total number of bytes needed to represent the given tag.
     */
    int tagLength();

    /**
     * Returns a view of this {@link DeliveryTag} object as a byte array.  The returned array may
     * be the actual underlying tag bytes or a synthetic view based on the value used to generate
     * the tag.  It is advised not to modify the returned value and copy if such modification are
     * necessary to the caller.
     *
     * @return the underlying tag bytes as a byte array that may or may no be a singleton instance..
     */
    byte[] tagBytes();

    /**
     * Returns a view of this {@link DeliveryTag} object as a {@link ProtonBuffer}.  The returned array
     * may be the actual underlying tag bytes or a synthetic view based on the value used to generate
     * the tag.  It is advised not to modify the returned value and copy if such modification are
     * necessary to the caller.
     *
     * @return the ProtonBuffer view of the tag bytes.
     */
    ProtonBuffer tagBuffer();

    /**
     * Optional method used by tag implementations that provide pooling of tags.
     */
    default void release() {
        // Default is no action, a subclass can add an action if needed.
    }

    /**
     * Create a copy of this delivery tag, the copy should account for any underlying pooling of tags that
     * the tag source's implementation is using.
     *
     * @return a copy of the underlying bytes that compose this delivery tag.
     */
    DeliveryTag copy();

    /**
     * Writes the tag as a sequence of bytes into the given buffer in the manner most efficient
     * for the underlying {@link DeliveryTag} implementation.
     *
     * @param buffer
     *      The target buffer where the tag bytes are to be written.
     */
    void writeTo(ProtonBuffer buffer);

    /**
     * A default DeliveryTag implementation that can be used by a codec when decoding DeliveryTag
     * instances from the wire.
     */
    public static class ProtonDeliveryTag implements DeliveryTag {

        public static final ProtonDeliveryTag EMPTY_TAG = new ProtonDeliveryTag();

        private static final byte[] EMPTY_TAG_ARRAY = new byte[0];

        private final byte[] tagBytes;
        private ProtonBuffer tagView;
        private Integer hashCode;

        public ProtonDeliveryTag() {
            this.tagBytes = EMPTY_TAG_ARRAY;
        }

        public ProtonDeliveryTag(byte[] tagBytes) {
            Objects.requireNonNull(tagBytes, "Tag bytes cannot be null");
            this.tagBytes = tagBytes;
        }

        public ProtonDeliveryTag(ProtonBuffer tagBytes) {
            Objects.requireNonNull(tagBytes, "Tag bytes cannot be null");
            if (tagBytes.hasArray() && tagBytes.getArrayOffset() == 0) {
                this.tagBytes = tagBytes.getArray();
            } else {
                this.tagBytes = new byte[tagBytes.getReadableBytes()];
                tagBytes.getBytes(tagBytes.getReadIndex(), this.tagBytes);
            }
            this.tagView = tagBytes;
        }

        @Override
        public byte[] tagBytes() {
            return tagBytes;
        }

        @Override
        public int tagLength() {
            return tagBytes.length;
        }

        @Override
        public ProtonBuffer tagBuffer() {
            if (tagView == null) {
                tagView = ProtonByteBufferAllocator.DEFAULT.wrap(tagBytes);
            }

            return tagView;
        }

        @Override
        public DeliveryTag copy() {
            return new ProtonDeliveryTag(Arrays.copyOf(tagBytes, tagBytes.length));
        }

        @Override
        public int hashCode() {
            if (hashCode == null) {
                hashCode = Arrays.hashCode(tagBytes);
            }

            return hashCode.intValue();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DeliveryTag)) {
                return false;
            }

            return Arrays.equals(tagBytes, ((DeliveryTag) other).tagBytes());
        }

        @Override
        public String toString() {
            return "DeliveryTag: {" + Arrays.toString(tagBytes) + "}";
        }

        @Override
        public void writeTo(ProtonBuffer buffer) {
            buffer.writeBytes(tagBytes);
        }
    }
}
