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
package org.apache.qpid.protonj2.test.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.ScriptedAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslCode;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslOutcome;
import org.apache.qpid.protonj2.test.driver.matchers.security.SaslOutcomeMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslOutcome performative
 */
public class SaslOutcomeExpectation extends AbstractExpectation<SaslOutcome> {

    private final SaslOutcomeMatcher matcher = new SaslOutcomeMatcher();

    public SaslOutcomeExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public ScriptedAction performAfterwards() {
        return new ScriptedAction() {

            @Override
            public ScriptedAction queue() {
                throw new UnsupportedOperationException("Cannot be called on this action");
            }

            @Override
            public ScriptedAction perform(AMQPTestDriver driver) {
                driver.resetToExpectingAMQPHeader();
                return null;
            }

            @Override
            public ScriptedAction now() {
                throw new UnsupportedOperationException("Cannot be called on this action");
            }

            @Override
            public ScriptedAction later(int waitTime) {
                throw new UnsupportedOperationException("Cannot be called on this action");
            }
        };
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslOutcomeExpectation withCode(byte code) {
        return withCode(equalTo(UnsignedByte.valueOf(code)));
    }

    public SaslOutcomeExpectation withCode(SaslCode code) {
        return withCode(equalTo(code.getValue()));
    }

    public SaslOutcomeExpectation withAdditionalData(byte[] additionalData) {
        return withAdditionalData(equalTo(new Binary(additionalData)));
    }

    public SaslOutcomeExpectation withAdditionalData(Binary additionalData) {
        return withAdditionalData(equalTo(additionalData));
    }

    //----- Matcher based with methods for more complex validation

    public SaslOutcomeExpectation withCode(Matcher<?> m) {
        matcher.addFieldMatcher(SaslOutcome.Field.CODE, m);
        return this;
    }

    public SaslOutcomeExpectation withAdditionalData(Matcher<?> m) {
        matcher.addFieldMatcher(SaslOutcome.Field.ADDITIONAL_DATA, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<SaslOutcome> getExpectedTypeClass() {
        return SaslOutcome.class;
    }
}
