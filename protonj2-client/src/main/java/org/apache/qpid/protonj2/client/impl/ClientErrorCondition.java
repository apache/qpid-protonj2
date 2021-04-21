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
package org.apache.qpid.protonj2.client.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Client implementation of the {@link ErrorCondition} type that wraps a
 * Proton specific AMQP {@link org.apache.qpid.protonj2.types.transport.ErrorCondition}.
 */
public final class ClientErrorCondition implements ErrorCondition {

    private final org.apache.qpid.protonj2.types.transport.ErrorCondition error;

    public ClientErrorCondition(ErrorCondition condition) {
        Objects.requireNonNull(condition, "The error condition value cannot be null");

        error = new org.apache.qpid.protonj2.types.transport.ErrorCondition(
            Symbol.valueOf(condition.condition()), condition.description(), ClientConversionSupport.toSymbolKeyedMap(condition.info()));
    }

    public ClientErrorCondition(String condition, String description, Map<String, Object> info) {
        Objects.requireNonNull(condition, "The error condition value cannot be null");

        error = new org.apache.qpid.protonj2.types.transport.ErrorCondition(
            Symbol.valueOf(condition), description, ClientConversionSupport.toSymbolKeyedMap(info));
    }

    ClientErrorCondition(org.apache.qpid.protonj2.types.transport.ErrorCondition condition) {
        Objects.requireNonNull(condition, "The error condition value cannot be null");

        error = condition;
    }

    @Override
    public String condition() {
        return error.getCondition().toString();
    }

    @Override
    public String description() {
        return error.getDescription();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> info() {
        return error.getInfo() == null ? Collections.EMPTY_MAP : ClientConversionSupport.toStringKeyedMap(error.getInfo());
    }

    //----- Internal methods used by Client resources

    org.apache.qpid.protonj2.types.transport.ErrorCondition getProtonErrorCondition() {
        return error;
    }

    static org.apache.qpid.protonj2.types.transport.ErrorCondition asProtonErrorCondition(ErrorCondition condition) {
        if (condition == null) {
            return null;
        } else if (condition instanceof ClientErrorCondition) {
            return ((ClientErrorCondition) condition).getProtonErrorCondition();
        } else {
            return new ClientErrorCondition(condition).getProtonErrorCondition();
        }
    }
}
