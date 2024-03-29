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

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Matcher;

public abstract class AbstractMapSectionMatcher<T extends AbstractMapSectionMatcher<T>> extends AbstractMessageSectionMatcher<T> {

    public AbstractMapSectionMatcher(UnsignedLong numericDescriptor, Symbol symbolicDescriptor, Map<Object, Matcher<?>> fieldMatchers, boolean expectTrailingBytes) {
        super(numericDescriptor, symbolicDescriptor, fieldMatchers, expectTrailingBytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void verifyReceivedDescribedObject(Object described) {
        if (!(described instanceof Map)) {
            throw new IllegalArgumentException(
                "Unexpected section contents. Expected Map, but got: " + (described == null ? "null" : described.getClass()));
        }

        verifyReceivedFields((Map<Object, Object>) described);
    }

    protected abstract T self();

    public T withEntry(Object key, Matcher<?> m) {
        validateMepKeyType(key);
        getMatchers().put(key, m);
        return self();
    }

    /**
     * Validate the map entry key type and throw an error if the type is not allowed in the map
     *
     * @param key
     * 		The key that will be used for matching map entries
     */
    protected abstract void validateMepKeyType(Object key);

}