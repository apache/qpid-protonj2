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
import org.apache.qpid.protonj2.test.driver.codec.security.SaslResponse;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class SaslResponseMatcher extends ListDescribedTypeMatcher {

    public SaslResponseMatcher() {
        super(SaslResponse.Field.values().length, SaslResponse.DESCRIPTOR_CODE, SaslResponse.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return SaslResponse.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public SaslResponseMatcher withResponse(byte[] response) {
        return withResponse(equalTo(new Binary(response)));
    }

    public SaslResponseMatcher withResponse(Binary response) {
        return withResponse(equalTo(response));
    }

    //----- Matcher based with methods for more complex validation

    public SaslResponseMatcher withResponse(Matcher<?> m) {
        addFieldMatcher(SaslResponse.Field.RESPONSE, m);
        return this;
    }
}
