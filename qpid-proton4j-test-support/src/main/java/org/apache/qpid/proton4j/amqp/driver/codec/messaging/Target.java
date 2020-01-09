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
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class Target extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:target:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000029L);

    /**
     * Enumeration which maps to fields in the Target Performative
     */
    public enum Field {
        ADDRESS,
        DURABLE,
        EXPIRY_POLICY,
        TIMEOUT,
        DYNAMIC,
        DYNAMIC_NODE_PROPERTIES,
        CAPABILITIES,
    }

    public Target() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Target(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Target(List<Object> described) {
        super(Field.values().length, described);
    }

    public Target(Target value) {
        super(Field.values().length, value);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Target setAddress(String o) {
        getList().set(Field.ADDRESS.ordinal(), o);
        return this;
    }

    public String getAddress() {
        return (String) getList().get(Field.ADDRESS.ordinal());
    }

    public Target setDurable(UnsignedInteger o) {
        getList().set(Field.DURABLE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getDurable() {
        return (UnsignedInteger) getList().get(Field.DURABLE.ordinal());
    }

    public Target setExpiryPolicy(Symbol o) {
        getList().set(Field.EXPIRY_POLICY.ordinal(), o);
        return this;
    }

    public Symbol getExpiryPolicy() {
        return (Symbol) getList().get(Field.EXPIRY_POLICY.ordinal());
    }

    public Target setTimeout(UnsignedInteger o) {
        getList().set(Field.TIMEOUT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getTimeout() {
        return (UnsignedInteger) getList().get(Field.TIMEOUT.ordinal());
    }

    public Target setDynamic(Boolean o) {
        getList().set(Field.DYNAMIC.ordinal(), o);
        return this;
    }

    public Boolean getDynamic() {
        return (Boolean) getList().get(Field.DYNAMIC.ordinal());
    }

    public Target setDynamicNodeProperties(Map<Symbol, Object> o) {
        getList().set(Field.DYNAMIC_NODE_PROPERTIES.ordinal(), o);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<Symbol, Object> getDynamicNodeProperties() {
        return (Map<Symbol, Object>) getList().get(Field.DYNAMIC_NODE_PROPERTIES.ordinal());
    }

    public Target setCapabilities(Symbol[] o) {
        getList().set(Field.CAPABILITIES.ordinal(), o);
        return this;
    }

    public Symbol[] getCapabilities() {
        return (Symbol[]) getList().get(Field.CAPABILITIES.ordinal());
    }
}
