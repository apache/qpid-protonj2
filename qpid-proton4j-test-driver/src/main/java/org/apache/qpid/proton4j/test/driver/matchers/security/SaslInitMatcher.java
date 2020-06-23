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
package org.apache.qpid.proton4j.test.driver.matchers.security;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.test.driver.codec.primitives.Binary;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.security.SaslInit;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class SaslInitMatcher extends ListDescribedTypeMatcher {

    public SaslInitMatcher() {
        super(SaslInit.Field.values().length, SaslInit.DESCRIPTOR_CODE, SaslInit.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return SaslInit.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslInitMatcher withMechanism(String mechanism) {
        return withMechanism(equalTo(Symbol.valueOf(mechanism)));
    }

    public SaslInitMatcher withMechanism(Symbol mechanism) {
        return withMechanism(equalTo(mechanism));
    }

    public SaslInitMatcher withInitialResponse(byte[] initialResponse) {
        return withInitialResponse(equalTo(new Binary(initialResponse)));
    }

    public SaslInitMatcher withInitialResponse(Binary initialResponse) {
        return withInitialResponse(equalTo(initialResponse));
    }

    public SaslInitMatcher withHostname(String hostname) {
        return withHostname(equalTo(hostname));
    }

    //----- Matcher based with methods for more complex validation

    public SaslInitMatcher withMechanism(Matcher<?> m) {
        addFieldMatcher(SaslInit.Field.MECHANISM, m);
        return this;
    }

    public SaslInitMatcher withInitialResponse(Matcher<?> m) {
        addFieldMatcher(SaslInit.Field.INITIAL_RESPONSE, m);
        return this;
    }

    public SaslInitMatcher withHostname(Matcher<?> m) {
        addFieldMatcher(SaslInit.Field.HOSTNAME, m);
        return this;
    }
}
