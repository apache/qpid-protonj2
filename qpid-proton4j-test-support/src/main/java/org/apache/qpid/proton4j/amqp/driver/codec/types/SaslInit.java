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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public class SaslInit extends SaslDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-init:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000041L);

    /**
     * Enumeration which maps to fields in the SaslInit Performative
     */
    public enum Field {
        MECHANISM,
        INITIAL_RESPONSE,
        HOSTNAME
    }

    public SaslInit() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public SaslInit(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public SaslInit(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public SaslInit setMechanism(Symbol o) {
        getList().set(Field.MECHANISM.ordinal(), o);
        return this;
    }

    public Symbol getMechanism() {
        return (Symbol) getList().get(Field.MECHANISM.ordinal());
    }

    public SaslInit setInitialResponse(Binary o) {
        getList().set(Field.INITIAL_RESPONSE.ordinal(), o);
        return this;
    }

    public Binary getInitialResponse() {
        return (Binary) getList().get(Field.INITIAL_RESPONSE.ordinal());
    }

    public SaslInit setHostname(String o) {
        getList().set(Field.HOSTNAME.ordinal(), o);
        return this;
    }

    public String getHostname() {
        return (String) getList().get(Field.HOSTNAME.ordinal());
    }
}
