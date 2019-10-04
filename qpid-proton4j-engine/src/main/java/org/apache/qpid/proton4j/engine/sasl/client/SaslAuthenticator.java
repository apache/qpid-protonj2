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

import java.util.Objects;

import javax.security.sasl.SaslException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientListener;
import org.apache.qpid.proton4j.engine.sasl.SaslOutcome;

/**
 * Handles SASL traffic from the proton engine and drives the authentication process
 */
public class SaslAuthenticator implements SaslClientListener {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(SaslAuthenticator.class);

    private final SaslMechanismSelector selector;
    private final SaslCredentialsProvider credentials;

    private Mechanism chosenMechanism;

    public SaslAuthenticator(SaslMechanismSelector selector, SaslCredentialsProvider credentials) {
        Objects.requireNonNull(selector, "A SASL Mechanism selector implementation is required");
        Objects.requireNonNull(credentials, "A SASL Credentials provider implementation is required");

        this.credentials = credentials;
        this.selector = selector;
    }

    @Override
    public void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms) {
        chosenMechanism = selector.select(mechanisms, credentials);

        if (chosenMechanism == null) {
            context.saslFailure(new SaslException("Could not find a matching mechanism to begin SASL Negotiations"));
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
        } catch (Throwable unknown) {
            context.saslFailure(new SaslException("Unknown error while fetching challenge response", unknown));
        }

        context.sendResponse(response);
    }

    @Override
    public void handleSaslOutcome(SaslClientContext context, SaslOutcome outcome, ProtonBuffer additional) {
        try {
            chosenMechanism.verifyCompletion();
        } catch (SaslException se) {
            context.saslFailure(se);
        } catch (Throwable unknown) {
            context.saslFailure(new SaslException("Unknown error while verifying SASL negotiations completion", unknown));
        }
    }
}
