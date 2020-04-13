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
import java.util.Objects;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.util.StringUtils;

/**
 * Client side mechanism used to select a matching mechanism from the server offered list of
 * mechanisms.  The client configures the list of allowed {@link Mechanism} names and when the
 * server mechanisms are offered mechanism is chosen from the allowed set.  If the client does
 * not configure any mechanisms then the selector chooses from all supported {@link Mechanism}
 * types.
 */
public class SaslMechanismSelector {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SaslMechanismSelector.class);

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
        Set<Symbol> candidates = new LinkedHashSet<>(serverMechs.length);
        for (Symbol serverMech : serverMechs) {
            candidates.add(serverMech);
        }

        if (!allowedMechanisms.isEmpty()) {
            candidates.retainAll(allowedMechanisms);
        }

        for (Symbol match : candidates) {
            LOG.trace("Attempting to match offered mechanism {} with supported and configured mechanisms", match);

            try {
                final Mechanism mechanism = createMechanism(match, credentials);
                if (mechanism == null) {
                    LOG.debug("Skipping {} mechanism as no implementation could be created to support it", match);
                    continue;
                }

                if (!isApplicable(mechanism, credentials)) {
                    LOG.trace("Skipping {} mechanism as it is not applicable", mechanism);
                } else {
                    return mechanism;
                }
            } catch (Exception error) {
                LOG.warn("Caught exception while trying to create SASL mechanism {}: {}", match, error.getMessage());
            }
        }

        return null;
    }

    /**
     * Using the given {@link Mechanism} name and the provided credentials create and configure a
     * {@link Mechanism} for evaluation by the selector.
     *
     * @param name
     *      A mechanism name that matches one of the supported offerings by the remote
     * @param credentials
     *      The provided credentials that will be used to perform authentication with the remote.
     *
     * @return a new {@link Mechanism} instance or null the offered mechanism is unsupported.
     */
    protected Mechanism createMechanism(Symbol name, SaslCredentialsProvider credentials) {
        return SaslMechanisms.valueOf(name).createMechanism();
    }

    /**
     * Tests a given {@link Mechanism} instance to determine if it is applicable given the selector
     * configuration and the provided credentials.
     *
     * @param candidate
     *      The SASL mechanism that matches both the allowed and the server offered lists.
     * @param credentials
     *      The provided SASL credentials which will be used when authenticating with the remote.
     *
     * @return true if the candidate {@link Mechanism} is applicable given the provide credentials.
     */
    protected boolean isApplicable(Mechanism candidate, SaslCredentialsProvider credentials) {
        Objects.requireNonNull(candidate, "Candidate Mechanism to validate must not be null");

        // If a match is found we still may skip it if the credentials given do not match with
        // what is needed in order for it to operate.  We also need to check that when working
        // from a wide open mechanism selection range we check is the Mechanism supports use in
        // a default configuration with no pre-configuration.

        if (!candidate.isApplicable(credentials)) {
            LOG.debug("Skipping {} mechanism because the available credentials are not sufficient", candidate.getName());
            return false;
        }

        if (allowedMechanisms.isEmpty()) {
            return candidate.isEnabledByDefault();
        } else {
            return allowedMechanisms.contains(candidate.getName());
        }
    }
}
