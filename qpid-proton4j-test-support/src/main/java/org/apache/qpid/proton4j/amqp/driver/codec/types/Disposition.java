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
package org.apache.qpid.proton4j.amqp.driver.codec.types;

import java.util.List;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class Disposition extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:disposition:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000015L);

    /**
     * Enumeration which maps to fields in the Disposition Performative
     */
    public enum Field {
        ROLE,
        FIRST,
        LAST,
        SETTLED,
        STATE,
        BATCHABLE
    }

    public Disposition() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Disposition(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Disposition(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Disposition setRole(Object o) {
        getList().set(Field.ROLE.ordinal(), o);
        return this;
    }

    public Object getRole() {
        return getList().get(Field.ROLE.ordinal());
    }

    public Disposition setFirst(Object o) {
        getList().set(Field.FIRST.ordinal(), o);
        return this;
    }

    public Object getFirst() {
        return getList().get(Field.FIRST.ordinal());
    }

    public Disposition setLast(Object o) {
        getList().set(Field.LAST.ordinal(), o);
        return this;
    }

    public Object getLast() {
        return getList().get(Field.LAST.ordinal());
    }

    public Disposition setSettled(Object o) {
        getList().set(Field.SETTLED.ordinal(), o);
        return this;
    }

    public Object getSettled() {
        return getList().get(Field.SETTLED.ordinal());
    }

    public Disposition setState(Object o) {
        getList().set(Field.STATE.ordinal(), o);
        return this;
    }

    public Object getState() {
        return getList().get(Field.STATE.ordinal());
    }

    public Disposition setBatchable(Object o) {
        getList().set(Field.BATCHABLE.ordinal(), o);
        return this;
    }

    public Object getBatchable() {
        return getList().get(Field.BATCHABLE.ordinal());
    }
}
