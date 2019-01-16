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
package org.apache.qpid.proton4j.transport.sasl;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslCode;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslStates;

public class SaslServerContext extends SaslContext {

    private final SaslServerListener listener;

    private boolean allowNonSasl;

    public SaslServerContext(SaslHandler handler, SaslServerListener listener) {
        super(handler);

        this.listener = listener;
    }

    @Override
    Role getRole() {
        return Role.SERVER;
    }

    /**
     * @return the SASL server listener.
     */
    public SaslServerListener getServerListener() {
        return listener;
    }

    //----- Remote Client state ----------------------------------------------//

    public String getClientMechanism() {
        return chosenMechanism.toString();
    }

    public String getClientHostname() {
        return hostname;
    }

    //----- Context mutable state --------------------------------------------//

    public String[] getMechanisms() {
        String[] mechanisms = null;

        if (serverMechanisms != null) {
            mechanisms = new String[serverMechanisms.length];
            for (int i = 0; i < serverMechanisms.length; i++) {
                mechanisms[i] = serverMechanisms[i].toString();
            }
        }

        return mechanisms;
    }

    public void setMechanisms(String... mechanisms) {
        if (!mechanismsSent) {
            Symbol[] serverMechanisms = new Symbol[mechanisms.length];
            for (int i = 0; i < mechanisms.length; i++) {
                serverMechanisms[i] = Symbol.valueOf(mechanisms[i]);
            }

            this.serverMechanisms = serverMechanisms;
        } else {
            // TODO What is the right error here.
            throw new IllegalStateException("Server Mechanisms arlready sent to remote");
        }
    }

    public Binary getChallenge() {
        return challenge;
    }

    public void setChallenge(Binary challenge) {
        this.challenge = challenge;
    }

    public Binary getAdditionalData() {
        return additionalData;
    }

    public void setAdditionalData(Binary additionalData) {
        this.additionalData = additionalData;
    }

    /**
     * @return whether this Server allows non-sasl connection attempts
     */
    public boolean isAllowNonSasl() {
        return allowNonSasl;
    }

    /**
     * Determines if the server allows non-SASL connection attempts.
     *
     * @param allowNonSasl
     *      the configuration for allowing non-sasl connections
     */
    public void setAllowNonSasl(boolean allowNonSasl) {
        this.allowNonSasl = allowNonSasl;
    }

    /**
     * @return the currently set SASL outcome.
     */
    public SaslOutcomes getOutcome() {
        return outcome;
    }

    /**
     * Sets the SASL outcome to return to the remote.
     *
     * @param outcome
     *      The SASL outcome that should be returned to the remote.
     */
    public void setOutcome(SaslOutcomes outcome) {
        this.outcome = outcome;
    }

    //----- Transport event handlers -----------------------------------------//

    @Override
    public void handleHeaderFrame(TransportHandlerContext context, HeaderFrame header) {
        if (header.getBody().isSaslHeader()) {
            handleSaslHeader(context, header);
        } else {
            handleNonSaslHeader(context, header);
        }
    }

    private void handleNonSaslHeader(TransportHandlerContext context, HeaderFrame header) {
        if (!headerReceived) {
            if (isAllowNonSasl()) {
                // Set proper outcome etc.
                classifyStateFromOutcome(SaslOutcomes.PN_SASL_OK);
                done = true;
                saslHandler.handleRead(context, header);
            } else {
                // TODO - Error type ?
                classifyStateFromOutcome(SaslOutcomes.PN_SASL_SKIPPED);
                done = true;
                saslHandler.handleWrite(context, AMQPHeader.getSASLHeader());
                saslHandler.transportFailed(context, new IllegalStateException(
                    "Unexpected AMQP Header before SASL Authentication completed."));
            }
        } else {
            // Report the variety of errors that exist in this state such as the
            // fact that we shouldn't get an AMQP header before sasl is done, and when
            // it is done we are currently bypassing this call in the parent SaslHandler.
            saslHandler.transportFailed(context, new IllegalStateException(
                "Unexpected AMQP Header before SASL Authentication completed."));
        }
    }

    private void handleSaslHeader(TransportHandlerContext context, HeaderFrame header) {
        if (!headerWritten) {
            saslHandler.handleWrite(context, header.getBody());
            headerWritten = true;
        }

        if (headerReceived) {
            // TODO - Error out on receive of another SASL Header.
            saslHandler.transportFailed(context, new IllegalStateException(
                "Unexpected second SASL Header read before SASL Authentication completed."));
        } else {
            headerReceived = true;
        }

        // Give the callback handler a chance to configure this handler
        listener.onSaslHeader(this, header.getBody());

        // TODO - When to fail when no mechanisms set, now or on some earlier started / connected event ?
        //        Or allow it to be empty and await an async write of a SaslInit frame etc ?
        if (serverMechanisms == null || serverMechanisms.length == 0) {
            saslHandler.transportFailed(context, new IllegalStateException("SASL Server has no configured mechanisms"));
        }

        // TODO - Check state, then send mechanisms
        SaslMechanisms mechanisms = new SaslMechanisms();
        mechanisms.setSaslServerMechanisms(serverMechanisms);

        // Send the server mechanisms now.
        saslHandler.handleWrite(context, mechanisms);
        mechanismsSent = true;
        state = SaslStates.PN_SASL_STEP;
    }

    @Override
    public void handleInit(SaslInit saslInit, TransportHandlerContext context) {
        if (initReceived) {
            // TODO - Handle SaslInit already read with better error
            saslHandler.transportFailed(context, new IllegalStateException("SASL Handler received second SASL Init"));
            return;
        }

        hostname = saslInit.getHostname();
        chosenMechanism = saslInit.getMechanism();
        initReceived = true;

        // TODO - Should we use ProtonBuffer slices as response containers ?
        listener.onSaslInit(this, saslInit.getInitialResponse());

        pumpServerState(context);
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, TransportHandlerContext context) {
        // TODO - Should we use ProtonBuffer slices as response containers ?
        listener.onSaslResponse(this, saslResponse.getResponse());

        pumpServerState(context);
    }

    private void pumpServerState(TransportHandlerContext context) {
        if (state == SaslStates.PN_SASL_STEP && getChallenge() != null) {
            SaslChallenge challenge = new SaslChallenge();
            challenge.setChallenge(getChallenge());
            setChallenge(null);
            saslHandler.handleWrite(context, challenge);
        }

        if (getOutcome() != SaslOutcomes.PN_SASL_NONE) {
            SaslOutcome outcome = new SaslOutcome();
            // TODO Clean up SaslCode mechanics
            outcome.setCode(SaslCode.values()[getOutcome().getCode()]);
            outcome.setAdditionalData(additionalData);
            setAdditionalData(null);
            done = true;
            saslHandler.handleWrite(context, outcome);
        }
    }
}
