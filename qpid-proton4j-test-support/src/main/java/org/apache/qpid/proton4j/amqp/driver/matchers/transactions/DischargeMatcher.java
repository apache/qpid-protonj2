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
package org.apache.qpid.proton4j.amqp.driver.matchers.transactions;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.proton4j.types.Binary;
import org.hamcrest.Matcher;

public class DischargeMatcher extends ListDescribedTypeMatcher {

    public DischargeMatcher() {
        super(Discharge.Field.values().length, Discharge.DESCRIPTOR_CODE, Discharge.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Discharge.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public DischargeMatcher withTxnId(byte[] txnId) {
        return withTxnId(equalTo(new Binary(txnId)));
    }

    public DischargeMatcher withTxnId(Binary txnId) {
        return withTxnId(equalTo(txnId));
    }

    public DischargeMatcher withFail(boolean fail) {
        return withFail(equalTo(fail));
    }

    //----- Matcher based with methods for more complex validation

    public DischargeMatcher withTxnId(Matcher<?> m) {
        addFieldMatcher(Discharge.Field.TXN_ID, m);
        return this;
    }

    public DischargeMatcher withFail(Matcher<?> m) {
        addFieldMatcher(Discharge.Field.FAIL, m);
        return this;
    }
}
