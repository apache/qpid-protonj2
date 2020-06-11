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
package org.apache.qpid.proton4j.amqp.driver.codec.messaging;

import java.util.List;

import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;

public class Received extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:received:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000023L);

    /**
     * Enumeration which maps to fields in the Received Performative
     */
    public enum Field {
        SECTION_NUMBER,
        SECTION_OFFSET,
    }

    public Received() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Received(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Received(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Received setSectionNumber(Object o) {
        getList().set(Field.SECTION_NUMBER.ordinal(), o);
        return this;
    }

    public Object getSectionNumber() {
        return getList().get(Field.SECTION_NUMBER.ordinal());
    }

    public Received setSectionOffset(Object o) {
        getList().set(Field.SECTION_OFFSET.ordinal(), o);
        return this;
    }

    public Object getSectionOffset() {
        return getList().get(Field.SECTION_OFFSET.ordinal());
    }
}
