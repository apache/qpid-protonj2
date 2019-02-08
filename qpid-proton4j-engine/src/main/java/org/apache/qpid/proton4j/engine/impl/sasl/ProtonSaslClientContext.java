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
package org.apache.qpid.proton4j.engine.impl.sasl;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.engine.sasl.SaslConstants.SaslStates;

public class ProtonSaslClientContext extends ProtonSaslContext {

    private final SaslClientListener listener;

    public ProtonSaslClientContext(ProtonSaslHandler handler, SaslClientListener listener) {
        super(handler);

        this.listener = listener;
    }

    @Override
    Role getRole() {
        return Role.SERVER;
    }

    /**
     * @return the SASL client listener.
     */
    public SaslClientListener getClientListener() {
        return listener;
    }

    //----- Remote Server state information ----------------------------------//

    public String[] getServerMechanisms() {
        String[] mechanisms = new String[serverMechanisms.length];
        for (int i = 0; i < serverMechanisms.length; i++) {
            mechanisms[i] = serverMechanisms[i].toString();
        }
        return mechanisms;
    }

    //----- Mutable state ----------------------------------------------------//

    // TODO - Remove these now ? or do something that checks these and does pipeline ?

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getMechanism() {
        return chosenMechanism.toString();
    }

    public void setMechanism(String mechanism) {
        chosenMechanism = Symbol.valueOf(mechanism);
    }

    public Binary getResponse() {
        return response;
    }

    public void setResponse(Binary response) {
        this.response = response;
    }

    //----- Event response methods -------------------------------------------//

    /**
     * Sends a response to the SASL server indicating the chosen mechanism for this
     * client and the host-name that this client is identifying itself as.
     *
     * @param mechanism
     *      The chosen mechanism selected from the list the server provided.
     * @param host
     *      The host-name that the client is identified as.
     */
    public void sendChosenMechanism(String mechanism, String host) {
        // TODO - Possible means of async trigger of mechanism send
    }

    /**
     * Sends a response to a server side challenge that comprises the challenge / response
     * exchange for the chosen SASL mechanism.
     *
     * @param response
     *      The response bytes to be sent to the server for this cycle.
     */
    public void sendResponse(ProtonBuffer response) {
        // TODO - Possible means of async challenge response send
    }

    //----- SASL Frame event handlers ----------------------------------------//

    @Override
    public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
        // TODO - Error on server not supporting SASL
        saslHandler.transportFailed(context, new IllegalStateException(
            "Remote does not support SASL authentication."));
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
        // TODO Auto-generated method stub
    }

    @Override
    public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
        serverMechanisms = saslMechanisms.getSaslServerMechanisms();

        // TODO - Should we use ProtonBuffer slices as response containers ?
        listener.onSaslMechanisms(this, getServerMechanisms());

        // TODO - How is the listener driving output, send methods ?
        //        We probably want to support asynchronous triggering
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
        // TODO - Should we use ProtonBuffer slices as response containers ?
        listener.onSaslChallenge(this, saslChallenge.getChallenge());

        if (state == SaslStates.PN_SASL_STEP && getResponse() != null) {
            SaslResponse response = new SaslResponse();
            response.setResponse(getResponse());
            setResponse(null);
            saslHandler.handleWrite(context, response);
        }

        // TODO - We probably want to support asynchronous triggering
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
        for (SaslOutcomes outcome : SaslOutcomes.values()) {
            if (outcome.getCode() == saslOutcome.getCode().ordinal()) {
                this.outcome = outcome;
                if (state != SaslStates.PN_SASL_IDLE) {
                    state = classifyStateFromOutcome(outcome);
                }
                break;
            }
        }

        done = true;

        listener.onSaslOutcome(this, saslOutcome.getAdditionalData());
    }
}
