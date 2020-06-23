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
package org.apache.qpid.proton4j.test.driver.expectations;

import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Binary;
import org.apache.qpid.proton4j.test.driver.codec.transactions.Discharge;
import org.apache.qpid.proton4j.test.driver.matchers.transactions.DischargeMatcher;
import org.apache.qpid.proton4j.test.driver.matchers.types.EncodedAmqpValueMatcher;

/**
 * Expectation used to script incoming transaction declarations.
 */
public class DischargeExpectation extends TransferExpectation {

    private DischargeMatcher discharge = new DischargeMatcher();
    private EncodedAmqpValueMatcher matcher = new EncodedAmqpValueMatcher(discharge);

    public DischargeExpectation(AMQPTestDriver driver) {
        super(driver);

        withPayload(matcher);
    }

    @Override
    public DischargeExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DischargeExpectation withFail(boolean fail) {
        discharge.withFail(fail);
        return this;
    }

    public DischargeExpectation withTxnId(byte[] txnId) {
        discharge.withTxnId(new Binary(txnId));
        return this;
    }

    public DischargeExpectation withTxnId(Binary txnId) {
        discharge.withTxnId(txnId);
        return this;
    }

    public DischargeExpectation withDischarge(Discharge discharge) {
        withPayload(new EncodedAmqpValueMatcher(discharge));
        return this;
    }

    public DischargeExpectation withNullDischarge() {
        withPayload(new EncodedAmqpValueMatcher(null));
        return this;
    }
}
