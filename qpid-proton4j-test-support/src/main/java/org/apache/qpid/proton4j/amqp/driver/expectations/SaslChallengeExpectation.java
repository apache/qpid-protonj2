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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.SaslResponseInjectAction;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.driver.matchers.security.SaslChallengeMatcher;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslChallenge performative
 */
public class SaslChallengeExpectation extends AbstractExpectation<SaslChallenge> {

    private final SaslChallengeMatcher matcher = new SaslChallengeMatcher();

    public SaslChallengeExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    public SaslResponseInjectAction respond() {
        SaslResponseInjectAction response = new SaslResponseInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslChallengeExpectation withChallenge(ProtonBuffer challenge) {
        return withChallenge(equalTo(challenge));
    }

    public SaslChallengeExpectation withChallenge(Binary challenge) {
        return withChallenge(equalTo(challenge.asProtonBuffer()));
    }

    //----- Matcher based with methods for more complex validation

    public SaslChallengeExpectation withChallenge(Matcher<?> m) {
        matcher.addFieldMatcher(SaslChallenge.Field.CHALLENGE, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Object getFieldValue(SaslChallenge saslChallenge, Enum<?> performativeField) {
        return saslChallenge.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return SaslChallenge.Field.values()[fieldIndex];
    }

    @Override
    protected Class<SaslChallenge> getExpectedTypeClass() {
        return SaslChallenge.class;
    }
}
