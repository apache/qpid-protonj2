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

import java.util.Random;

import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.actions.DispositionInjectAction;
import org.apache.qpid.proton4j.test.driver.codec.transactions.Declare;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.transactions.Declared;

/**
 * Expectation used to script incoming transaction declarations.
 */
public class DeclareExpectation extends TransferExpectation {

    private final EncodedAmqpValueMatcher defaultPayloadMatcher = new EncodedAmqpValueMatcher(new Declare());

    public DeclareExpectation(AMQPTestDriver driver) {
        super(driver);

        withPayload(defaultPayloadMatcher);
    }

    @Override
    public DispositionInjectAction accept() {
        final byte[] txnId = new byte[4];

        Random rand = new Random();
        rand.setSeed(System.nanoTime());
        rand.nextBytes(txnId);

        return accept(txnId);
    }

    public DispositionInjectAction accept(byte[] txnId) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        if (txnId != null) {
            response.withState(new Declared().setTxnId(new Binary(txnId)));
        } else {
            response.withState(new Declared());
        }

        driver.addScriptedElement(response);
        return response;
    }

    @Override
    public DeclareExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DeclareExpectation withDeclare(org.apache.qpid.proton4j.types.transactions.Declare declare) {
        withPayload(new EncodedAmqpValueMatcher(TypeMapper.mapFromProtonType(declare)));
        return this;
    }
}
