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

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.driver.matchers.security.SaslOutcomeMatcher;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.security.SaslCode;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslOutcome performative
 */
public class SaslOutcomeExpectation extends AbstractExpectation<SaslOutcome> {

    private final SaslOutcomeMatcher matcher = new SaslOutcomeMatcher();

    public SaslOutcomeExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslOutcomeExpectation withCode(SaslCode code) {
        return withCode(equalTo(code));
    }

    public SaslOutcomeExpectation withAdditionalData(byte[] additionalData) {
        return withAdditionalData(equalTo(new Binary(additionalData)));
    }

    public SaslOutcomeExpectation withAdditionalData(ProtonBuffer additionalData) {
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
    protected Object getFieldValue(SaslOutcome saslOutcome, Enum<?> performativeField) {
        return saslOutcome.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return SaslOutcome.Field.values()[fieldIndex];
    }

    @Override
    protected Class<SaslOutcome> getExpectedTypeClass() {
        return SaslOutcome.class;
    }
}
