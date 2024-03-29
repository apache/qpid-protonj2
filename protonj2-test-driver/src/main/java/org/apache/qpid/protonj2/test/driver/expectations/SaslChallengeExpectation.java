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

import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.actions.SaslResponseInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslChallenge;
import org.apache.qpid.protonj2.test.driver.matchers.security.SaslChallengeMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslChallenge performative
 */
public class SaslChallengeExpectation extends AbstractExpectation<SaslChallenge> {

    private final SaslChallengeMatcher matcher = new SaslChallengeMatcher();

    public SaslChallengeExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public SaslChallengeExpectation withPredicate(Predicate<SaslChallenge> predicate) {
        super.withPredicate(predicate);
        return this;
    }

    @Override
    public SaslChallengeExpectation withCapture(Consumer<SaslChallenge> capture) {
        super.withCapture(capture);
        return this;
    }

    public SaslResponseInjectAction respond() {
        SaslResponseInjectAction response = new SaslResponseInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslChallengeExpectation withChallenge(byte[] challenge) {
        return withChallenge(equalTo(new Binary(challenge)));
    }

    public SaslChallengeExpectation withChallenge(Binary challenge) {
        return withChallenge(equalTo(challenge));
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
    protected Class<SaslChallenge> getExpectedTypeClass() {
        return SaslChallenge.class;
    }
}
