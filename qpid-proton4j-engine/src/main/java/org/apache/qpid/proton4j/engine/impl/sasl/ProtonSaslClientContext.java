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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.impl.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.engine.impl.sasl.SaslConstants.SaslStates;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientListener;

public class ProtonSaslClientContext extends ProtonSaslContext implements SaslClientContext {

    private SaslClientListener listener;

    private boolean mechanismsReceived;
    private boolean mechanismChosen;

    public ProtonSaslClientContext(ProtonSaslHandler handler) {
        super(handler);
    }

    @Override
    public Role getRole() {
        return Role.SERVER;
    }

    @Override
    public SaslClientListener getClientListener() {
        return listener;
    }

    @Override
    public SaslClientContext setClientListener(SaslClientListener listener) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SaslClientContext sendChosenMechanism(String mechanism, String host) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SaslClientContext sendResponse(ProtonBuffer response) {
        // TODO Auto-generated method stub
        return null;
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

    // TODO - Remove these setters and require listener to initiate work
    //        we can leave accessors to fetch what was done.

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
//        listener.onSaslMechanisms(this, getServerMechanisms());

        // TODO - How is the listener driving output, send methods ?
        //        We probably want to support asynchronous triggering
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
        // TODO - Should we use ProtonBuffer slices as response containers ?
//        listener.onSaslChallenge(this, saslChallenge.getChallenge());

//        if (state == SaslStates.SASL_STEP && getResponse() != null) {
//            SaslResponse response = new SaslResponse();
//            response.setResponse(getResponse());
//            setResponse(null);
//            saslHandler.handleWrite(context, response);
//        }

        // TODO - We probably want to support asynchronous triggering
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
        this.outcome = SaslOutcomes.valueOf(outcome.getCode());
        if (state != SaslStates.SASL_IDLE) {
            state = classifyStateFromOutcome(outcome);
        }

        done = true;

//        listener.onSaslOutcome(this, saslOutcome.getAdditionalData());
    }
}
