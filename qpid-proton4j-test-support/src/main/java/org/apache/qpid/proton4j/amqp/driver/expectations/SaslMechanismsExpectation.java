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
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslMechanisms performative
 */
public class SaslMechanismsExpectation extends AbstractExceptation<SaslMechanisms> {

    /**
     * Enumeration which maps to fields in the SaslMechanisms Performative
     */
    public enum Field {
        SASL_SERVER_MECHANISMS
    }

    public SaslMechanismsExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    public SaslMechanismsExpectation withSaslServerMechanisms(Matcher<?> m) {
        getMatchers().put(Field.SASL_SERVER_MECHANISMS, m);
        return this;
    }

    @Override
    protected Object getFieldValue(SaslMechanisms saslMechanisms, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.SASL_SERVER_MECHANISMS) {
            result = saslMechanisms.getSaslServerMechanisms();
        } else {
            throw new AssertionError("Request for unknown field in type SaslMechanisms");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<SaslMechanisms> getExpectedTypeClass() {
        return SaslMechanisms.class;
    }
}
