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
package org.apache.qpid.protonj2.test.driver.codec.transactions;

import java.util.Arrays;
import java.util.List;

import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;

public class Coordinator extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:coordinator:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000030L);

    /**
     * Enumeration which maps to fields in the Coordinator Performative
     */
    public enum Field {
        CAPABILITIES
    }

    public Coordinator() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Coordinator(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Coordinator(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Coordinator setCapabilities(Symbol... o) {
        getList().set(Field.CAPABILITIES.ordinal(), o);
        return this;
    }

    public Symbol[] getCapabilities() {
        return (Symbol[]) getList().get(Field.CAPABILITIES.ordinal());
    }

    @Override
    public String toString() {
        return "Coordinator{" + "capabilities=" + Arrays.toString(getCapabilities()) + '}';
    }
}
