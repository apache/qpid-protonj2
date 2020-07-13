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
package org.apache.qpid.protonj2.test.driver.actions;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Discharge;

/**
 * Inject a Transaction Discharge frame.
 */
public class DischargeInjectAction extends TransferInjectAction {

    private Discharge discharge = new Discharge();

    public DischargeInjectAction(AMQPTestDriver driver) {
        super(driver);

        withBody().withDescribed(discharge);
    }

    public DischargeInjectAction withFail(boolean fail) {
        discharge.setFail(fail);
        return this;
    }

    public DischargeInjectAction withTxnId(byte[] txnId) {
        discharge.setTxnId(new Binary(txnId));
        return this;
    }

    public DischargeInjectAction withTxnId(Binary txnId) {
        discharge.setTxnId(txnId);
        return this;
    }

    public DischargeInjectAction withDischarge(Discharge discharge) {
        withBody().withDescribed(discharge);
        return this;
    }
}
