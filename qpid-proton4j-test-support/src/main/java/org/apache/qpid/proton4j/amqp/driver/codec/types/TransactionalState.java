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
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class TransactionalState extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transactional-state:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000034L);

    /**
     * Enumeration which maps to fields in the TransactionalState Performative
     */
    public enum Field {
        TXN_ID,
        OUTCOME
    }

    public TransactionalState() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public TransactionalState(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public TransactionalState(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public TransactionalState setTxnId(Binary o) {
        getList().set(Field.TXN_ID.ordinal(), o);
        return this;
    }

    public Binary getTxnId() {
        return (Binary) getList().get(Field.TXN_ID.ordinal());
    }

    public TransactionalState setOutcome(DescribedType o) {
        getList().set(Field.OUTCOME.ordinal(), o);
        return this;
    }

    public DescribedType getOutcome() {
        return (DescribedType) getList().get(Field.OUTCOME.ordinal());
    }
}
