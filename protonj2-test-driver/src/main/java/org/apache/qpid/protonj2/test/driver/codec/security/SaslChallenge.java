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
package org.apache.qpid.protonj2.test.driver.codec.security;

import java.util.List;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;

public class SaslChallenge extends SaslDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-challenge:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000042L);

    /**
     * Enumeration which maps to fields in the SaslChallenge Performative
     */
    public enum Field {
        CHALLENGE,
    }

    public SaslChallenge() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public SaslChallenge(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public SaslChallenge(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public SaslChallenge setChallenge(Binary o) {
        getList().set(Field.CHALLENGE.ordinal(), o);
        return this;
    }

    public Binary getChallenge() {
        return (Binary) getList().get(Field.CHALLENGE.ordinal());
    }

    @Override
    public SaslPerformativeType getPerformativeType() {
        return SaslPerformativeType.CHALLENGE;
    }

    @Override
    public <E> void invoke(SaslPerformativeHandler<E> handler, int frameSize, E context) {
        handler.handleChallenge(frameSize, this, context);
    }

    @Override
    public String toString() {
        return "SaslChallenge{" + "challenge=" + TypeMapper.toQuotedString(getChallenge()) + '}';
    }
}
