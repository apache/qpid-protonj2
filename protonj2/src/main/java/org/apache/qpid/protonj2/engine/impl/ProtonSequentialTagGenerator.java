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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonByteUtils;
import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;
import org.apache.qpid.protonj2.types.DeliveryTag;

/**
 * A Built in proton {@link DeliveryTagGenerator} that creates new tags using a sequential
 * numeric value which is encoded using the most compact representation of the numeric value.
 */
public class ProtonSequentialTagGenerator extends ProtonDeliveryTagGenerator {

    protected long nextTagId = 0;

    @Override
    public DeliveryTag nextTag() {
        return new ProtonNumericDeliveryTag(nextTagId++);
    }

    /*
     * Test entry point to validate tag cache and tag counter overflow.
     */
    void setNextTagId(long nextIdValue) {
        this.nextTagId = nextIdValue;
    }

    protected static class ProtonNumericDeliveryTag implements DeliveryTag {

        protected final long tagValue;

        public ProtonNumericDeliveryTag(long tagValue) {
            this.tagValue = tagValue;
        }

        @Override
        public int tagLength() {
            if (tagValue < 0) {
                return Long.BYTES;
            } else if (tagValue <= 0x00000000000000FFl) {
                return Byte.BYTES;
            } else if (tagValue <= 0x000000000000FFFFl) {
                return Short.BYTES;
            } else if (tagValue <= 0x00000000FFFFFFFFl) {
                return Integer.BYTES;
            } else {
                return Long.BYTES;
            }        }

        @Override
        public byte[] tagBytes() {
            if (tagValue < 0) {
                return ProtonByteUtils.toByteArray(tagValue);
            } else if (tagValue <= 0x00000000000000FFl) {
                return ProtonByteUtils.toByteArray((byte) tagValue);
            } else if (tagValue <= 0x000000000000FFFFl) {
                return ProtonByteUtils.toByteArray((short) tagValue);
            } else if (tagValue <= 0x00000000FFFFFFFFl) {
                return ProtonByteUtils.toByteArray((int) tagValue);
            } else {
                return ProtonByteUtils.toByteArray(tagValue);
            }
        }

        @Override
        public ProtonBuffer tagBuffer() {
            return ProtonByteBufferAllocator.DEFAULT.wrap(tagBytes());
        }

        @Override
        public void release() {
            // Nothing to do in this implementation
        }

        @Override
        public DeliveryTag copy() {
            return new ProtonNumericDeliveryTag(tagValue);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(tagValue);
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

            ProtonNumericDeliveryTag other = (ProtonNumericDeliveryTag) obj;
            if (tagValue != other.tagValue) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return "{" + tagValue + "}";
        }

        @Override
        public void writeTo(ProtonBuffer buffer) {
            if (tagValue < 0) {
                buffer.writeLong(tagValue);
            } else if (tagValue <= 0x00000000000000FFl) {
                buffer.writeByte((int) tagValue);
            } else if (tagValue <= 0x000000000000FFFFl) {
                buffer.writeShort((short) tagValue);
            } else if (tagValue <= 0x00000000FFFFFFFFl) {
                buffer.writeInt((int) tagValue);
            } else {
                buffer.writeLong(tagValue);
            }
        }
    }
}
