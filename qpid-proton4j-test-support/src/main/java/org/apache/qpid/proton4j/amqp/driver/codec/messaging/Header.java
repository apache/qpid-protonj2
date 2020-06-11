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
import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedLong;

public class Header extends ListDescribedType {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000070L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:header:list");

    /**
     * Enumeration which maps to fields in the Header Performative
     */
    public enum Field {
        DURABLE,
        PRIORITY,
        TTL,
        FIRST_ACQUIRER,
        DELIVERY_COUNT,
    }

    public Header() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Header(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Header(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Header setDurable(Boolean o) {
        getList().set(Field.DURABLE.ordinal(), o);
        return this;
    }

    public Boolean getDurable() {
        return (Boolean) getList().get(Field.DURABLE.ordinal());
    }

    public Header setPriority(UnsignedByte o) {
        getList().set(Field.PRIORITY.ordinal(), o);
        return this;
    }

    public UnsignedByte getPriority() {
        return (UnsignedByte) getList().get(Field.PRIORITY.ordinal());
    }

    public Header setTtl(UnsignedInteger o) {
        getList().set(Field.TTL.ordinal(), o);
        return this;
    }

    public UnsignedInteger getTtl() {
        return (UnsignedInteger) getList().get(Field.TTL.ordinal());
    }

    public Header setFirstAcquirer(Boolean o) {
        getList().set(Field.FIRST_ACQUIRER.ordinal(), o);
        return this;
    }

    public Boolean getFirstAcquirer() {
        return (Boolean) getList().get(Field.FIRST_ACQUIRER.ordinal());
    }

    public Header setDeliveryCount(UnsignedInteger o) {
        getList().set(Field.DELIVERY_COUNT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getDeliveryCount() {
        return (UnsignedInteger) getList().get(Field.DELIVERY_COUNT.ordinal());
    }
}
