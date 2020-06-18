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
package org.apache.qpid.proton4j.test.driver.matchers.transactions;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.test.driver.codec.transactions.TransactionalState;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.hamcrest.Matcher;

public class TransactionalStateMatcher extends ListDescribedTypeMatcher {

    public TransactionalStateMatcher() {
        super(TransactionalState.Field.values().length, TransactionalState.DESCRIPTOR_CODE, TransactionalState.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return TransactionalState.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public TransactionalStateMatcher withTxnId(byte[] txnId) {
        return withTxnId(equalTo(new Binary(txnId)));
    }

    public TransactionalStateMatcher withTxnId(Binary txnId) {
        return withTxnId(equalTo(txnId));
    }

    public TransactionalStateMatcher withFail(DeliveryState outcome) {
        return withOutcome(equalTo(outcome));
    }

    //----- Matcher based with methods for more complex validation

    public TransactionalStateMatcher withTxnId(Matcher<?> m) {
        addFieldMatcher(TransactionalState.Field.TXN_ID, m);
        return this;
    }

    public TransactionalStateMatcher withOutcome(Matcher<?> m) {
        addFieldMatcher(TransactionalState.Field.OUTCOME, m);
        return this;
    }
}
