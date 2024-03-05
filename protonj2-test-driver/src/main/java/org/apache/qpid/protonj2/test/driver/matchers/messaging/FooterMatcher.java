/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.protonj2.test.driver.matchers.messaging;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

public class FooterMatcher extends AbstractMapSectionMatcher<FooterMatcher> {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:footer:map");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000078L);

    public FooterMatcher(boolean expectTrailingBytes) {
        super(DESCRIPTOR_CODE, DESCRIPTOR_SYMBOL, new HashMap<Object, Matcher<?>>(), expectTrailingBytes);
    }

    public FooterMatcher withEntry(String key, Matcher<?> m) {
        return super.withEntry(Symbol.valueOf(key), m);
    }

    public FooterMatcher withEntry(String key, Object value) {
        return super.withEntry(Symbol.valueOf(key), Matchers.equalTo(value));
    }

    public FooterMatcher withEntry(Symbol key, Object value) {
        return super.withEntry(key, Matchers.equalTo(value));
    }

    public boolean keyExistsInReceivedAnnotations(Object key) {
        validateMepKeyType(key);

        Map<Object, Object> receivedFields = super.getReceivedFields();

        if (receivedFields != null) {
            return receivedFields.containsKey(key);
        } else {
            return false;
        }
    }

    public Object getReceivedAnnotation(Symbol key) {
        Map<Object, Object> receivedFields = super.getReceivedFields();

        return receivedFields.get(key);
    }

    @Override
    protected void validateMepKeyType(Object key) {
        if (!(key instanceof Long || key instanceof Symbol)) {
            throw new IllegalArgumentException("Footer keys must be of type Symbol or long (reserved)");
        }
    }

    @Override
    protected FooterMatcher self() {
        return this;
    }
}
