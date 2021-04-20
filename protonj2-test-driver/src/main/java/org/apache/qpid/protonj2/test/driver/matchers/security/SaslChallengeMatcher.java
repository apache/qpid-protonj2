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
package org.apache.qpid.protonj2.test.driver.matchers.security;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslChallenge;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class SaslChallengeMatcher extends ListDescribedTypeMatcher {

    public SaslChallengeMatcher() {
        super(SaslChallenge.Field.values().length, SaslChallenge.DESCRIPTOR_CODE, SaslChallenge.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return SaslChallenge.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslChallengeMatcher withChallenge(byte[] challenge) {
        return withChallenge(equalTo(new Binary(challenge)));
    }

    public SaslChallengeMatcher withChallenge(Binary challenge) {
        return withChallenge(equalTo(challenge));
    }

    //----- Matcher based with methods for more complex validation

    public SaslChallengeMatcher withChallenge(Matcher<?> m) {
        addFieldMatcher(SaslChallenge.Field.CHALLENGE, m);
        return this;
    }
}
