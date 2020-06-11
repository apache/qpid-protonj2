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
package org.apache.qpid.proton4j.amqp.driver.codec.security;

import java.util.List;

import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.UnsignedLong;

public class SaslOutcome extends SaslDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-outcome:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000044L);

    /**
     * Enumeration which maps to fields in the Rejected Performative
     */
    public enum Field {
        CODE,
        ADDITIONAL_DATA
    }

    public SaslOutcome() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public SaslOutcome(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public SaslOutcome(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public UnsignedLong getDescriptor() {
        return DESCRIPTOR_CODE;
    }

    public SaslOutcome setCode(UnsignedByte o) {
        getList().set(Field.CODE.ordinal(), o);
        return this;
    }

    public UnsignedByte getCode() {
        return (UnsignedByte) getList().get(Field.CODE.ordinal());
    }

    public SaslOutcome setAdditionalData(Binary o) {
        getList().set(Field.ADDITIONAL_DATA.ordinal(), o);
        return this;
    }

    public Binary getAdditionalData() {
        return (Binary) getList().get(Field.ADDITIONAL_DATA.ordinal());
    }

    @Override
    public SaslPerformativeType getPerformativeType() {
        return SaslPerformativeType.OUTCOME;
    }

    @Override
    public <E> void invoke(SaslPerformativeHandler<E> handler, E context) {
        handler.handleOutcome(this, context);
    }
}
