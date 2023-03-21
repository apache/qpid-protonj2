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

class BooleanElement extends AtomicElement<Boolean> {

    private final boolean value;

    public BooleanElement(Element<?> parent, Element<?> current, boolean b) {
        super(parent, current);
        value = b;
    }

    @Override
    public int size() {
        // in non-array parent then there is a single byte encoding, in an array
        // there is a 1-byte encoding but no
        // constructor
        return 1;
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.BOOL;
    }

    @Override
    public int encode(DataOutput output) {
        try {
            if (isElementOfArray()) {
                output.writeByte(value ? (byte) 1 : (byte) 0);
            } else {
                output.writeByte(value ? (byte) 0x41 : (byte) 0x42);
            }
            return 1;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
