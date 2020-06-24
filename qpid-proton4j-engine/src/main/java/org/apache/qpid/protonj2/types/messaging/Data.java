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

public final class Data implements Section {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");

    private final Binary value;

    public Data(Binary value) {
        this.value = value;
    }

    public Data copy() {
        return new Data(value == null ? null : value.copy());
    }

    public Binary getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Data{ " + value + " }";
    }

    @Override
    public SectionType getType() {
        return SectionType.Data;
    }
}
