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

import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP SaslInit performative
 */
public class SaslInitExpectation extends AbstractExceptation<SaslInit> {

    /**
     * Enumeration which maps to fields in the SaslInit Performative
     */
    public enum Field {
        MECHANISM,
        INITIAL_RESPONSE,
        HOSTNAME,
    }

    public SaslInitExpectation withMechanism(Matcher<?> m) {
        getMatchers().put(Field.MECHANISM, m);
        return this;
    }

    public SaslInitExpectation withInitialResponse(Matcher<?> m) {
        getMatchers().put(Field.INITIAL_RESPONSE, m);
        return this;
    }

    public SaslInitExpectation withHostname(Matcher<?> m) {
        getMatchers().put(Field.HOSTNAME, m);
        return this;
    }

    @Override
    protected Object getFieldValue(SaslInit saslInit, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.MECHANISM) {
            result = saslInit.getMechanism();
        } else if (performativeField == Field.INITIAL_RESPONSE) {
            result = saslInit.getInitialResponse();
        } else if (performativeField == Field.HOSTNAME) {
            result = saslInit.getHostname();
        } else {
            throw new AssertionError("Request for unknown field in type SaslInit");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<SaslInit> getExpectedTypeClass() {
        return SaslInit.class;
    }
}
