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

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;

class UnsignedByteElement extends AtomicElement<UnsignedByte> {

    private final UnsignedByte value;

    UnsignedByteElement(Element<?> parent, Element<?> prev, UnsignedByte ub) {
        super(parent, prev);
        value = ub;
    }

    @Override
    public int size() {
        return isElementOfArray() ? 1 : 2;
    }

    @Override
    public UnsignedByte getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.UBYTE;
    }

    @Override
    public int encode(DataOutput output) {
        try {
            if (isElementOfArray()) {
                output.writeByte(value.byteValue());
                return 1;
            } else {
                output.writeByte((byte) 0x50);
                output.writeByte(value.byteValue());
                return 2;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
