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
package org.messaginghub.amqperative.impl.sasl;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientListener;
import org.apache.qpid.proton4j.engine.sasl.SaslOutcome;

/**
 * Handles SASL traffic from the proton engine and drives the authentication process
 */
public class SaslAuthenticator implements SaslClientListener {

    private final SaslMechanismSelector selector;
    private final SaslCredentialsProvider credentials;

    private Mechanism chosenMechanism;

    public SaslAuthenticator(SaslMechanismSelector selector, SaslCredentialsProvider credentials) {
        this.credentials = credentials;
        this.selector = selector;
    }

    @Override
    public void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms) {
        chosenMechanism = selector.select(mechanisms, credentials);

        if (chosenMechanism == null) {
            // TODO - Locally fail the engine we cannot proceed without a mechanism.
        }

        // Configure the mechanism based on provided credentials
        chosenMechanism.setUsername(credentials.username());
        chosenMechanism.setPassword(credentials.password());

        ProtonBuffer initialResponse = null;
        try {
            initialResponse = chosenMechanism.getInitialResponse();
        } catch (Throwable t) {
            // TODO - Failure generating response, fail locally
        }

        context.sendChosenMechanism(chosenMechanism.getName(), credentials.vhost(), initialResponse);
    }

    @Override
    public void handleSaslChallenge(SaslClientContext context, ProtonBuffer challenge) {
        ProtonBuffer response = null;
        try {
            response = chosenMechanism.getChallengeResponse(challenge);
        } catch (Throwable t) {
            // TODO - Failure generating response, fail locally
        }

        context.sendResponse(response);
    }

    @Override
    public void handleSaslOutcome(SaslClientContext context, SaslOutcome outcome, ProtonBuffer additional) {
        // TODO - If this throws then we need a way to fail the engine or the client
        chosenMechanism.verifyCompletion();

        switch (outcome) {
            case SASL_TEMP:
                // TODO - signal temporary failure
                break;
            case SASL_AUTH:
                // TODO - signal credentials failure
                break;
            case SASL_PERM:
                // TODO - signal some unrecoverable failure
                break;
            case SASL_SYS:
                // TODO - signal some other system failure
                break;
            default:
                break;
        }
    }
}
