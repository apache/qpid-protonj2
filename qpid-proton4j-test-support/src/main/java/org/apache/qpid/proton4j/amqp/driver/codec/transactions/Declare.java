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
package org.apache.qpid.proton4j.amqp.driver.codec.transactions;

import java.util.List;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class Declare extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:declare:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000031L);

    /**
     * Enumeration which maps to fields in the Declare Performative
     */
    public enum Field {
        GLOBAL_ID
    }

    public Declare() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Declare(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Declare(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Declare setGlobalId(Binary o) {
        getList().set(Field.GLOBAL_ID.ordinal(), o);
        return this;
    }

    public Binary getGlobalId() {
        return (Binary) getList().get(Field.GLOBAL_ID.ordinal());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof DescribedType)) {
            return false;
        }

        DescribedType d = (DescribedType) obj;
        if (!(DESCRIPTOR_CODE.equals(d.getDescriptor()) || DESCRIPTOR_SYMBOL.equals(d.getDescriptor()))) {
            return false;
        }

        Object described = getDescribed();
        Object described2 = d.getDescribed();
        if (described == null) {
            return described2 == null;
        } else {
            return described.equals(described2);
        }
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
