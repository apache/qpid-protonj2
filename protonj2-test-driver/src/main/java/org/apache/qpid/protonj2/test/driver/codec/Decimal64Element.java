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

import org.apache.qpid.protonj2.test.driver.codec.primitives.Decimal64;

import io.netty.buffer.ByteBuf;

class Decimal64Element extends AtomicElement<Decimal64> {

    private final Decimal64 value;

    Decimal64Element(Element<?> parent, Element<?> prev, Decimal64 d) {
        super(parent, prev);
        value = d;
    }

    @Override
    public int size() {
        return isElementOfArray() ? 8 : 9;
    }

    @Override
    public Decimal64 getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.DECIMAL64;
    }

    @Override
    public int encode(ByteBuf buffer) {
        int size = size();
        if (buffer.maxWritableBytes() >= size) {
            if (size == 9) {
                buffer.writeByte((byte) 0x84);
            }
            buffer.writeLong(value.getBits());
            return size;
        } else {
            return 0;
        }
    }
}
