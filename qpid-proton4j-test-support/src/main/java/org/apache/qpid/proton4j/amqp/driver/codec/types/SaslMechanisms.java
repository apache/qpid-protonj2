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

public class SaslMechanisms extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-mechanisms:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000040L);

    /**
     * Enumeration which maps to fields in the SaslMechanisms Performative
     */
    public enum Field {
        SASL_SERVER_MECHANISMS,
    }

    public SaslMechanisms() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public SaslMechanisms(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public SaslMechanisms(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public SaslMechanisms setSaslServerMechanisms(Symbol... o) {
        getList().set(Field.SASL_SERVER_MECHANISMS.ordinal(), o);
        return this;
    }

    public Symbol[] getSaslServerMechanisms() {
        return (Symbol[]) getList().get(Field.SASL_SERVER_MECHANISMS.ordinal());
    }
}
