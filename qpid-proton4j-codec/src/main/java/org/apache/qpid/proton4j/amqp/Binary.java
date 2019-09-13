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
import java.util.Collection;

// TODO - Consider whether this class makes sense or if just using ProtonBuffer and or byte[]
//        API in the codec and engine makes more sense and allow for more flexibility

public final class Binary {

    private final byte[] data;
    private final int offset;
    private final int length;
    private int hashCode;

    public Binary(final byte[] data) {
        this(data, 0, data.length);
    }

    public Binary(final byte[] data, final int offset, final int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public Binary copy() {
        byte[] dataCopy = new byte[length];
        System.arraycopy(data, offset, dataCopy, 0, length);
        return new Binary(dataCopy, 0, length);
    }

    public byte[] arrayCopy() {
        byte[] dataCopy = null;
        if (data != null) {
            dataCopy = new byte[length];
            System.arraycopy(data, offset, dataCopy, 0, length);
        }

        return dataCopy;
    }

    public ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(data, offset, length);
    }

    @Override
    public final int hashCode() {
        int hc = hashCode;
        if (hc == 0) {
            for (int i = 0; i < length; i++) {
                hc = 31 * hc + (0xFF & data[offset + i]);
            }
            hashCode = hc;
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

        Binary buf = (Binary) o;
        final int size = length;
        if (size != buf.length) {
            return false;
        }

        final byte[] myData = data;
        final byte[] theirData = buf.data;
        int myOffset = offset;
        int theirOffset = buf.offset;
        final int myLimit = myOffset + size;

        while (myOffset < myLimit) {
            if (myData[myOffset++] != theirData[theirOffset++]) {
                return false;
            }
        }

        return true;
    }

    public int getArrayOffset() {
        return offset;
    }

    public byte[] getArray() {
        return data;
    }

    public int getLength() {
        return length;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();

        for (int i = 0; i < length; i++) {
            byte c = data[offset + i];

            if (c > 31 && c < 127 && c != '\\') {
                str.append((char) c);
            } else {
                str.append(String.format("\\x%02x", c));
            }
        }

        return str.toString();
    }

    // TODO - These aren't used currently and could be removed to allow this type of operation
    //        to fall on the ProtonBuffer class.

    public static Binary combine(final Collection<Binary> binaries) {
        if (binaries.size() == 1) {
            return binaries.iterator().next();
        }

        int size = 0;
        for (Binary binary : binaries) {
            size += binary.getLength();
        }
        byte[] data = new byte[size];
        int offset = 0;
        for (Binary binary : binaries) {
            System.arraycopy(binary.data, binary.offset, data, offset, binary.length);
            offset += binary.length;
        }
        return new Binary(data);
    }

    public Binary subBinary(final int offset, final int length) {
        return new Binary(this.data, this.offset + offset, length);
    }

    public static Binary create(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        } else {
            if (buffer.isDirect() || buffer.isReadOnly()) {
                byte data[] = new byte[buffer.remaining()];
                ByteBuffer dup = buffer.duplicate();
                dup.get(data);
                return new Binary(data);
            } else {
                return new Binary(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            }
        }
    }
}
