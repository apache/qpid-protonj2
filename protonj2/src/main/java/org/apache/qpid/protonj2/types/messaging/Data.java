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

import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Data implements Section<byte[]> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");

    private final Binary value;

    public Data(Binary value) {
        this.value = value;
    }

    public Data(byte[] value) {
        this.value = value != null ? new Binary(value) : null;
    }

    public Data(byte[] value, int offset, int length) {
        this.value = value != null ? new Binary(value, offset, length) : null;
    }

    public Data copy() {
        return new Data(value == null ? null : value.copy());
    }

    public Binary getBinary() {
        return value;
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
        if (value != null && value.hasArray() && value.getArrayOffset() == 0 && value.getLength() == value.getArray().length) {
            return value.getArray();
        } else {
            return value != null ? value.arrayCopy() : null;
        }
    }

    @Override
    public String toString() {
        return "Data{ " + value + " }";
    }

    @Override
    public SectionType getType() {
        return SectionType.Data;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }

        return true;
    }
}
