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
package org.apache.qpid.proton4j.amqp.driver.expectations;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Expectation used to script incoming transaction declarations.
 */
public class DischargeExpectation extends TransferExpectation {

    private Discharge discharge;
    private EncodedAmqpValueMatcher matcher;

    public DischargeExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public DischargeExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DischargeExpectation withFail(boolean fail) {
        if (discharge == null) {
            discharge = new Discharge();
            matcher = new EncodedAmqpValueMatcher(discharge);
        }

        discharge.setFail(fail);

        withPayload(matcher);
        return this;
    }

    public DischargeExpectation withTxnId(byte[] txnId) {
        if (discharge == null) {
            discharge = new Discharge();
            matcher = new EncodedAmqpValueMatcher(discharge);
        }

        discharge.setTxnId(new Binary(txnId));

        withPayload(matcher);
        return this;
    }

    public DischargeExpectation withTxnId(ProtonBuffer txnId) {
        if (discharge == null) {
            discharge = new Discharge();
            matcher = new EncodedAmqpValueMatcher(discharge);
        }

        discharge.setTxnId(new Binary(txnId));

        withPayload(matcher);
        return this;
    }

    public DischargeExpectation withTxnId(Binary txnId) {
        if (discharge == null) {
            discharge = new Discharge();
            matcher = new EncodedAmqpValueMatcher(discharge);
        }

        discharge.setTxnId(txnId);

        withPayload(matcher);
        return this;
    }

    public DischargeExpectation withDischarge(org.apache.qpid.proton4j.amqp.transactions.Discharge discharge) {
        withPayload(new EncodedAmqpValueMatcher(TypeMapper.mapFromProtonType(discharge)));
        return this;
    }
}
