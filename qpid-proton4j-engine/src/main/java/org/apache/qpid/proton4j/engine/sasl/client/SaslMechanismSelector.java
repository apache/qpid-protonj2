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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.engine.util.StringUtils;

/**
 * Client side mechanism used to select a matching mechanism from the server offered list of
 * mechanisms.  The client configures the list of allowed {@link Mechanism} names and when the
 * server mechanisms are offered mechanism is chosen from the allowed set.  If the client does
 * not configure any mechanisms then the selector chooses from all supported {@link Mechanism}
 * types.
 */
public class SaslMechanismSelector {

    private final Set<Symbol> allowedMechanisms;

    /**
     * Creates a new {@link Mechanism} selector that will choose a match from all supported {@link Mechanism} types.
     */
    public SaslMechanismSelector() {
        this((Set<Symbol>) null);
    }

    /**
     * Creates a new {@link Mechanism} selector configured with the given set of allowed {@link Mechanism} names.
     *
     * @param allowed
     *      A {@link Collection} of SASL mechanism names that are allowed to be used when selecting a matching mechanism.
     */
    @SuppressWarnings("unchecked")
    public SaslMechanismSelector(Collection<String> allowed) {
        this.allowedMechanisms = allowed != null ? StringUtils.toSymbolSet(allowed) : Collections.EMPTY_SET;
    }

    /**
     * Creates a new {@link Mechanism} selector configured with the given set of allowed {@link Mechanism} names.
     *
     * @param allowed
     *      A {@link Set} of SASL mechanism names that are allowed to be used when selecting a matching mechanism.
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

        Set<Symbol> candidates = new LinkedHashSet<>(serverMechs.length);
        for (Symbol serverMech : serverMechs) {
            candidates.add(serverMech);
        }

        if (!allowedMechanisms.isEmpty()) {
            candidates.retainAll(allowedMechanisms);
        }

        for (Symbol match : candidates) {
            selected = evaluateMatchingMechanism(match, credentials);

            if (selected != null) {
                break;
            }
        }

        return selected;
    }

    /**
     * If a configured allowed mechanism matches with a mechanism offered by the remote then we must
     * evaluate it to determine if it is applicable and supported.  If we are able to both create a
     * {@link Mechanism} instance from the match and it reports that it is applicable using the configured
     * credentials we can return the matching {@link Mechanism} and allow SASL authentication to proceed
     * using that {@link Mechanism}.
     *
     * @param mechanism
     *      The name of the SASL mechanism that matches both the allowed and the server offered lists.
     * @param credentials
     *      The provided SASL credentials which will be used when authenticating with the remote.
     *
     * @return a new {@link Mechanism} instance if the candidate is applicable and supported by this selector.
     */
    protected Mechanism evaluateMatchingMechanism(Symbol mechanism, SaslCredentialsProvider credentials) {
        try {
            final SaslMechanisms potential = SaslMechanisms.valueOf(mechanism);

            // If a match is found we still may skip it if the credentials given do not
            // match with what is needed in order for it to operate.
            if (potential.isApplicable(credentials)) {
                return potential.createMechanism();
            }
        } catch (Throwable e) {
            // No match in supported mechanisms or not applicable for given credentials
        }

        return null;
    }
}
