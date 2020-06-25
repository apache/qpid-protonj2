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
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslResponse;
import org.apache.qpid.protonj2.test.driver.matchers.security.SaslResponseMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslResponse performative
 */
public class SaslResponseExpectation extends AbstractExpectation<SaslResponse> {

    private final SaslResponseMatcher matcher = new SaslResponseMatcher();

    public SaslResponseExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslResponseExpectation withResponse(byte[] response) {
        return withResponse(equalTo(new Binary(response)));
    }

    public SaslResponseExpectation withResponse(Binary response) {
        return withResponse(equalTo(response));
    }

    //----- Matcher based with methods for more complex validation

    public SaslResponseExpectation withResponse(Matcher<?> m) {
        matcher.addFieldMatcher(SaslResponse.Field.RESPONSE, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<SaslResponse> getExpectedTypeClass() {
        return SaslResponse.class;
    }
}
