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

import java.util.Arrays;
import java.util.Objects;

import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

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
     * @return the underlying tag bytes (not a copy, user should not modify).
     */
    byte[] tagBytes();

    /**
     * @return the ProtonBuffer view of the tag bytes.
     */
    ProtonBuffer tagBuffer();

    /**
     * Optional method used by tag implementations that provide pooling of tags.
     */
    default void release() {}

    /**
     * Create a copy of this delivery tag, the copy should account for any underlying pooling of tags that
     * the tag source's implementation is using.
     *
     * @return a copy of the underlying bytes that compose this delivery tag.
     */
    DeliveryTag copy();

    /**
     * A default DeliveryTag implementation that can be used by a codec when decoding DeliveryTag
     * instances from the wire.
     */
    public static class ProtonDeliveryTag implements DeliveryTag {

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
    }
}
