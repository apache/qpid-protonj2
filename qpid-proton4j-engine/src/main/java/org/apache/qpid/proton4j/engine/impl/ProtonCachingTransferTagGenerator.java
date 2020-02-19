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
package org.apache.qpid.proton4j.engine.impl;

import java.util.Queue;

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.util.RingQueue;

/**
 * Built in Transfer {@link DeliveryTag} generator that uses a fixed size tag
 * pool to reduce GC overhead by reusing tags that have been released from settled
 * messages.  When not using cached tags the generator creates new tags using a
 * running tag counter of type {@link Long} that assumes that when it wraps the user
 *  has already release all tags within the lower range of the tag counter.
 */
public class ProtonCachingTransferTagGenerator {

    public static final int MAX_NUM_CACHED_TAGS = 256;

    private long nextTagId = 0;
    private final Queue<DeliveryTag> tagCache = new RingQueue<>(MAX_NUM_CACHED_TAGS);

    public DeliveryTag nextTag() {
        DeliveryTag nextTag = tagCache.poll();
        if (nextTag == null) {
            nextTag = createTag();
        }

        return nextTag;
    }

    private DeliveryTag createTag() {
        DeliveryTag nextTag = null;

        if (nextTagId >= 0 && nextTagId < MAX_NUM_CACHED_TAGS) {
            // Cached tag that will return to cache on next release.
            nextTag = new PooledProtonDeliveryTag((byte) nextTagId++);
        } else {
            // Non-cached tag that will not return to the cache on next release.
            nextTag = new DeliveryTag.ProtonDeliveryTag(generateNextTagBytes(nextTagId++));
            if (nextTagId == 0) {
                nextTagId = MAX_NUM_CACHED_TAGS;
            }
        }

        return nextTag;
    }

    private static byte[] generateNextTagBytes(long tag) {
        int size = encodingSize(tag);

        byte[] tagBytes = new byte[size];

        for (int i = 0; i < size; ++i) {
            tagBytes[size - 1 - i] = (byte) (tag >>> (i * 8));
        }

        return tagBytes;
    }

    private static int encodingSize(long value) {
        if (value < 0) {
            return Long.BYTES;
        } else if (value < 0x00000000000000FFl) {
            return Byte.BYTES;
        } else if (value < 0x000000000000FFFFl) {
            return Short.BYTES;
        } else if (value < 0x00000000FFFFFFFFl) {
            return Integer.BYTES;
        } else {
            return Long.BYTES;
        }
    }

    /*
     * Test entry point to validate tag cache and tag counter overflow.
     */
    void setNextTagId(long nextIdValue) {
        this.nextTagId = nextIdValue;
    }

    //----- Specialized DeliveryTag and releases itself back to the cache

    private class PooledProtonDeliveryTag implements DeliveryTag {

        private final byte tagValue;

        public PooledProtonDeliveryTag(byte tagValue) {
            this.tagValue = tagValue;
        }

        @Override
        public void release() {
            tagCache.offer(this);
        }

        @Override
        public int tagLength() {
            return Byte.BYTES;
        }

        @Override
        public byte[] tagBytes() {
            return tagBuffer().getArray();
        }

        @Override
        public ProtonBuffer tagBuffer() {
            return ProtonByteBufferAllocator.DEFAULT.allocate(2, 2).writeByte(tagValue);
        }

        @Override
        public DeliveryTag copy() {
            return new DeliveryTag.ProtonDeliveryTag(tagBytes());
        }

        @Override
        public void writeTo(ProtonBuffer buffer) {
            buffer.writeShort(tagValue);
        }
    }
}
