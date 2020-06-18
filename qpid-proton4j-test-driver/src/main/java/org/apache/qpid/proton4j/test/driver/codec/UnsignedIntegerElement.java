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
package org.apache.qpid.proton4j.test.driver.codec;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.UnsignedInteger;

class UnsignedIntegerElement extends AtomicElement<UnsignedInteger> {

    private final UnsignedInteger value;

    UnsignedIntegerElement(Element<?> parent, Element<?> prev, UnsignedInteger i) {
        super(parent, prev);
        value = i;
    }

    @Override
    public int size() {
        if (isElementOfArray()) {
            final ArrayElement parent = (ArrayElement) parent();
            if (parent.constructorType() == ArrayElement.TINY) {
                if (value.intValue() == 0) {
                    return 0;
                } else {
                    parent.setConstructorType(ArrayElement.SMALL);
                }
            }

            if (parent.constructorType() == ArrayElement.SMALL) {
                if (0 <= value.intValue() && value.intValue() <= 255) {
                    return 1;
                } else {
                    parent.setConstructorType(ArrayElement.LARGE);
                }
            }

            return 4;

        } else {
            return 0 == value.intValue() ? 1 : (1 <= value.intValue() && value.intValue() <= 255) ? 2 : 5;
        }

    }

    @Override
    public UnsignedInteger getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.UINT;
    }

    @Override
    public int encode(ProtonBuffer buffer) {
        int size = size();
        if (size > buffer.getMaxWritableBytes()) {
            return 0;
        }
        switch (size) {
            case 1:
                if (isElementOfArray()) {
                    buffer.writeByte((byte) value.intValue());
                } else {
                    buffer.writeByte((byte) 0x43);
                }
                break;
            case 2:
                buffer.writeByte((byte) 0x52);
                buffer.writeByte((byte) value.intValue());
                break;
            case 5:
                buffer.writeByte((byte) 0x70);
            case 4:
                buffer.writeInt(value.intValue());

        }

        return size;
    }
}
