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
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslInit;
import org.apache.qpid.proton4j.amqp.driver.matchers.security.SaslInitMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslInit performative
 */
public class SaslInitExpectation extends AbstractExpectation<SaslInit> {

    private final SaslInitMatcher matcher = new SaslInitMatcher();

    public SaslInitExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslInitExpectation withMechanism(Symbol mechanism) {
        return withMechanism(equalTo(mechanism));
    }

    public SaslInitExpectation withInitialResponse(Binary initialResponse) {
        return withInitialResponse(equalTo(initialResponse));
    }

    public SaslInitExpectation withHostname(String hostname) {
        return withHostname(equalTo(hostname));
    }

    //----- Matcher based with methods for more complex validation

    public SaslInitExpectation withMechanism(Matcher<?> m) {
        matcher.addFieldMatcher(SaslInit.Field.MECHANISM, m);
        return this;
    }

    public SaslInitExpectation withInitialResponse(Matcher<?> m) {
        matcher.addFieldMatcher(SaslInit.Field.INITIAL_RESPONSE, m);
        return this;
    }

    public SaslInitExpectation withHostname(Matcher<?> m) {
        matcher.addFieldMatcher(SaslInit.Field.HOSTNAME, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Object getFieldValue(SaslInit saslInit, Enum<?> performativeField) {
        return saslInit.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return SaslInit.Field.values()[fieldIndex];
    }

    @Override
    protected Class<SaslInit> getExpectedTypeClass() {
        return SaslInit.class;
    }
}
