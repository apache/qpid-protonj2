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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBuffer;
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
    default int tagLength() {
        return tagBytes().getReadableBytes();
    }

    /**
     * @return the ProtonBuffer view of the tag bytes.
     */
    ProtonBuffer tagBytes();

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

        private static final ProtonByteBuffer EMPTY_TAG = new ProtonByteBuffer(0, 0);

        private final ProtonBuffer tag;

        public ProtonDeliveryTag() {
            this.tag = EMPTY_TAG;
        }

        public ProtonDeliveryTag(byte[] tagBytes) {
            Objects.requireNonNull(tagBytes, "Tag bytes cannot be null");
            this.tag = ProtonByteBufferAllocator.DEFAULT.wrap(tagBytes);
        }

        public ProtonDeliveryTag(ProtonBuffer tagBytes) {
            Objects.requireNonNull(tagBytes, "Tag bytes cannot be null");
            this.tag = tagBytes;
        }

        @Override
        public ProtonBuffer tagBytes() {
            return tag;
        }

        @Override
        public DeliveryTag copy() {
            return new ProtonDeliveryTag(tag.copy());
        }

        @Override
        public int hashCode() {
            return tag.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DeliveryTag)) {
                return false;
            }

            return tag.equals(((DeliveryTag) other).tagBytes());
        }

        @Override
        public String toString() {
            return "DeliveryTag: {" + tag.toString(StandardCharsets.UTF_8) + "}";
        }
    }
}
