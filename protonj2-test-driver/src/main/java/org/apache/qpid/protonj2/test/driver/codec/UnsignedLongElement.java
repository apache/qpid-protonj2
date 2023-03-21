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
package org.apache.qpid.protonj2.test.driver.codec;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;

class UnsignedLongElement extends AtomicElement<UnsignedLong> {

    private final UnsignedLong value;

    UnsignedLongElement(Element<?> parent, Element<?> prev, UnsignedLong ul) {
        super(parent, prev);
        value = ul;
    }

    @Override
    public int size() {
        if (isElementOfArray()) {
            final ArrayElement parent = (ArrayElement) parent();
            if (parent.constructorType() == ArrayElement.TINY) {
                if (value.longValue() == 0l) {
                    return 0;
                } else {
                    parent.setConstructorType(ArrayElement.SMALL);
                }
            }

            if (parent.constructorType() == ArrayElement.SMALL) {
                if (0l <= value.longValue() && value.longValue() <= 255l) {
                    return 1;
                } else {
                    parent.setConstructorType(ArrayElement.LARGE);
                }
            }

            return 8;

        } else {
            return 0l == value.longValue() ? 1 : (1l <= value.longValue() && value.longValue() <= 255l) ? 2 : 9;
        }

    }

    @Override
    public UnsignedLong getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.ULONG;
    }

    @Override
    public int encode(DataOutput output) {
        try {
            int size = size();

            switch (size) {
                case 1:
                    if (isElementOfArray()) {
                        output.writeByte((byte) value.longValue());
                    } else {
                        output.writeByte((byte) 0x44);
                    }
                    break;
                case 2:
                    output.writeByte((byte) 0x53);
                    output.writeByte((byte) value.longValue());
                    break;
                case 9:
                    output.writeByte((byte) 0x80);
                case 8:
                    output.writeLong(value.longValue());
            }

            return size;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
