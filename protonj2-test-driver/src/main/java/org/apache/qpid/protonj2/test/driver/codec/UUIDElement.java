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
import java.util.UUID;

class UUIDElement extends AtomicElement<UUID> {

    private final UUID value;

    UUIDElement(Element<?> parent, Element<?> prev, UUID u) {
        super(parent, prev);
        value = u;
    }

    @Override
    public int size() {
        return isElementOfArray() ? 16 : 17;
    }

    @Override
    public UUID getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.UUID;
    }

    @Override
    public int encode(DataOutput output) {
        final int size = size();

        try {
            if (size == 17) {
                output.writeByte((byte) 0x98);
            }
            output.writeLong(value.getMostSignificantBits());
            output.writeLong(value.getLeastSignificantBits());
            return size;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
