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
package org.apache.qpid.proton4j.engine.test.describedtypes;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.engine.test.ListDescribedType;

public class TransactionalState extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transactional-state:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000034L);

    private static final int FIELD_TXN_ID = 0;
    private static final int FIELD_OUTCOME = 1;

    public TransactionalState(Object... fields) {
        super(2);
        int i = 0;
        for (Object field : fields) {
            getFields()[i++] = field;
        }
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public TransactionalState setTxnId(Object o) {
        getFields()[FIELD_TXN_ID] = o;
        return this;
    }

    public TransactionalState setOutcome(Object o) {
        getFields()[FIELD_OUTCOME] = o;
        return this;
    }
}
