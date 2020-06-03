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

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Declare;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.types.EncodedAmqpValueMatcher;

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
    public DeclareExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DeclareExpectation withDeclare(org.apache.qpid.proton4j.amqp.transactions.Declare declare) {
        withPayload(new EncodedAmqpValueMatcher(TypeMapper.mapFromProtonType(declare)));
        return this;
    }
}
