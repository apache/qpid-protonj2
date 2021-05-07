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
package org.apache.qpid.protonj2.engine.sasl.client;

import java.util.Objects;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.sasl.SaslClientContext;
import org.apache.qpid.protonj2.engine.sasl.SaslClientListener;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Handles SASL traffic from the proton engine and drives the authentication process
 */
public class SaslAuthenticator implements SaslClientListener {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SaslAuthenticator.class);

    private final SaslMechanismSelector selector;
    private final SaslCredentialsProvider credentials;

    private EventHandler<SaslOutcome> saslCompleteHandler;
    private Mechanism chosenMechanism;

    /**
     * Creates a new SASL Authenticator initialized with the given credentials provider instance.  Because no
     * {@link Mechanism} selector is given the full set of supported SASL mechanisms will be chosen from when
     * attempting to match one to the server offered SASL mechanisms.
     *
     * @param credentials
     *      The credentials that will be used when the SASL negotiation is in progress.
     */
    public SaslAuthenticator(SaslCredentialsProvider credentials) {
        this(new SaslMechanismSelector(), credentials);
    }

    /**
     * Creates a new client SASL Authenticator with the given {@link Mechanism} and client credentials
     * provider instances.  The configured {@link Mechanism} selector is used when attempting to match
     * a SASL {@link Mechanism} with the server offered set of supported SASL mechanisms.
     *
     * @param selector
     *     The {@link SaslMechanismSelector} that will be called upon to choose a server supported mechanism.
     * @param credentials
     *      The credentials that will be used when the SASL negotiation is in progress.
     */
    public SaslAuthenticator(SaslMechanismSelector selector, SaslCredentialsProvider credentials) {
        Objects.requireNonNull(selector, "A SASL Mechanism selector implementation is required");
        Objects.requireNonNull(credentials, "A SASL Credentials provider implementation is required");

        this.credentials = credentials;
        this.selector = selector;
    }

    /**
     * Sets a completion handler that will be notified once the SASL exchange has completed.  The notification
     * includes the {@link SaslOutcome} value which indicates if authentication succeeded or failed.
     *
     * @param saslCompleteEventHandler
     * 		The {@link EventHandler} that will receive notification when SASL authentication has completed.
     *
     * @return this {@link SaslAuthenticator} instance.
     */
    public SaslAuthenticator saslComplete(EventHandler<SaslOutcome> saslCompleteEventHandler) {
        this.saslCompleteHandler = saslCompleteEventHandler;
        return this;
    }

    @Override
    public void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms) {
        chosenMechanism = selector.select(mechanisms, credentials);

        if (chosenMechanism == null) {
            context.saslFailure(new SaslException(
                "Could not find a suitable SASL Mechanism. No supported mechanism, or none usable with " +
                "the available credentials. Server offered: " + StringUtils.toStringSet(mechanisms)));
            return;
        }

        LOG.debug("SASL Negotiations proceeding using selected mechanisms: {}", chosenMechanism);

        ProtonBuffer initialResponse = null;
        try {
            initialResponse = chosenMechanism.getInitialResponse(credentials);
        } catch (SaslException se) {
            context.saslFailure(se);
        } catch (Throwable unknown) {
            context.saslFailure(new SaslException("Unknown error while fetching initial response", unknown));
        }

        context.sendChosenMechanism(chosenMechanism.getName(), credentials.vhost(), initialResponse);
    }

    @Override
    public void handleSaslChallenge(SaslClientContext context, ProtonBuffer challenge) {
        ProtonBuffer response = null;
        try {
            response = chosenMechanism.getChallengeResponse(credentials, challenge);
        } catch (SaslException se) {
            context.saslFailure(se);
        } catch (Exception unknown) {
            context.saslFailure(new SaslException("Unknown error while fetching challenge response", unknown));
        }

        context.sendResponse(response);
    }

    @Override
    public void handleSaslOutcome(SaslClientContext context, SaslOutcome outcome, ProtonBuffer additional) {
        try {
            chosenMechanism.verifyCompletion();
            if (saslCompleteHandler != null) {
                saslCompleteHandler.handle(outcome);
            }
        } catch (SaslException se) {
            context.saslFailure(se);
        } catch (Exception unknown) {
            context.saslFailure(new SaslException("Unknown error while verifying SASL negotiations completion", unknown));
        }
    }
}
