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
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslResponse performative
 */
public class SaslResponseExpectation extends AbstractExceptation<SaslResponse> {

    /**
     * Enumeration which maps to fields in the SaslResponse Performative
     */
    public enum Field {
        RESPONSE
    }

    public SaslResponseExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslResponseExpectation withResponse(Binary response) {
        return withResponse(equalTo(response));
    }

    //----- Matcher based with methods for more complex validation

    public SaslResponseExpectation withResponse(Matcher<?> m) {
        getMatchers().put(Field.RESPONSE, m);
        return this;
    }

    @Override
    protected Object getFieldValue(SaslResponse saslResponse, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.RESPONSE) {
            result = saslResponse.getResponse();
        } else {
            throw new AssertionError("Request for unknown field in type SaslResponse");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<SaslResponse> getExpectedTypeClass() {
        return SaslResponse.class;
    }
}
