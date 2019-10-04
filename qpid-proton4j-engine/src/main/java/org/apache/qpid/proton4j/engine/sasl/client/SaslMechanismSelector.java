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
package org.apache.qpid.proton4j.engine.sasl.client;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.Symbol;

/**
 * Client side mechanism used to select a matching mechanism from the server offered list of
 * mechanisms.  The client configures the list of allowed {@link Mechanism} names and when the
 * server mechanisms are offered mechanism is chosen from the allowed set.
 */
public final class SaslMechanismSelector {

    private final Set<Symbol> allowedMechanisms;

    /**
     * Creates a new {@link Mechanism} selector that will choose a match from all supported {@link Mechanism} types.
     */
    public SaslMechanismSelector() {
        this(null);
    }

    /**
     * Creates a new {@link Mechanism} selector configured with the given set of allowed {@link Mechanism} names.
     * @param allowed
     */
    @SuppressWarnings("unchecked")
    public SaslMechanismSelector(Set<Symbol> allowed) {
        this.allowedMechanisms = allowed != null ? allowed : Collections.EMPTY_SET;
    }

    /**
     * @return the configured set of allowed SASL {@link Mechanism} names.
     */
    public Set<Symbol> getAllowedMechanisms() {
        return Collections.unmodifiableSet(allowedMechanisms);
    }

    /**
     * Given a list of SASL mechanism names select a match from the supported types using the
     * configured allowed list and the given credentials.
     *
     * @param serverMechs
     *      The list of mechanisms the server indicates it supports.
     * @param credentials
     *      A {@link SaslCredentialsProvider} used to choose an matching applicable SASL {@link Mechanism}.
     *
     * @return a selected SASL {@link Mechanism} instance or null of no match is possible.
     */
    public Mechanism select(Symbol[] serverMechs, SaslCredentialsProvider credentials) {
        Mechanism selected = null;

        Set<Symbol> matching = new LinkedHashSet<>(serverMechs.length);
        for (Symbol serverMech : serverMechs) {
            matching.add(serverMech);
        }

        if (!allowedMechanisms.isEmpty()) {
            matching.retainAll(allowedMechanisms);
        }

        for (Symbol match : matching) {
            SaslMechanisms option = SaslMechanisms.valueOf(match);
            if (option.isApplicable(credentials)) {
                selected = option.createMechanism();
                break;
            }
        }

        return selected;
    }
}
