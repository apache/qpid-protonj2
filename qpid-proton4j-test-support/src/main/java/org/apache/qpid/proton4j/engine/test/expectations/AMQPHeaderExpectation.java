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
package org.apache.qpid.proton4j.engine.test.expectations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.test.EngineTestDriver;
import org.apache.qpid.proton4j.engine.test.ScriptedAction;
import org.apache.qpid.proton4j.engine.test.ScriptedExpectation;
import org.apache.qpid.proton4j.engine.test.actions.AMQPHeaderInjectAction;

/**
 * Expectation entry for AMQP Headers
 */
public class AMQPHeaderExpectation implements ScriptedExpectation {

    private AMQPHeader expected;
    private AMQPHeader response;

    public AMQPHeaderExpectation(AMQPHeader expected) {
        this.expected = expected;
    }

    @Override
    public ScriptedAction performAfterwards() {
        return response != null ? new AMQPHeaderInjectAction(response) : null;
    }

    @Override
    public void handleAMQPHeader(AMQPHeader header, EngineTestDriver driver) {
        assertThat("AMQP Header should match expected.", expected, equalTo(header));
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineTestDriver driver) {
        assertThat("SASL Header should match expected.", expected, equalTo(header));
    }
}
