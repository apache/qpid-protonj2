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

import java.util.Arrays;
import java.util.Objects;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientListener;

final class ProtonSaslClientContext extends ProtonSaslContext implements SaslClientContext {

    // TODO - Update SASL State in driver

    private SaslClientListener client = new ProtonDefaultSaslClientListener();

    // Work state trackers
    private boolean headerWritten;
    private boolean headerReceived;
    private boolean mechanismsReceived;
    private boolean mechanismChosen;
    private boolean responseRequired;

    public ProtonSaslClientContext(ProtonSaslHandler handler) {
        super(handler);
    }

    @Override
    public Role getRole() {
        return Role.CLIENT;
    }

    @Override
    public SaslClientContext setListener(SaslClientListener listener) {
        Objects.requireNonNull(listener, "Cannot configure a null SaslClientListener");
        this.client = listener;
        return this;
    }

    @Override
    public SaslClientListener getListener() {
        return client;
    }

    //----- SASL negotiations API

    @Override
    public SaslClientContext sendSASLHeader() {
        if (!headerWritten) {
            saslHandler.context().fireWrite(AMQPHeader.getSASLHeader());
            headerWritten = true;
        } else {
            throw new IllegalStateException("SASL Header already sent to the remote SASL server");
        }

        return this;
    }

    @Override
    public SaslClientContext sendChosenMechanism(Symbol mechanism, String hostname, Binary initialResponse) {
        if (!mechanismChosen) {
            Objects.requireNonNull(mechanism, "Cannot send an initial response with no chosen mechanism.");

            this.chosenMechanism = mechanism;
            this.hostname = hostname;

            SaslInit init = new SaslInit().setHostname(hostname)
                                          .setMechanism(mechanism)
                                          .setInitialResponse(initialResponse);

            saslHandler.context().fireWrite(init);
            mechanismChosen = true;
        } else {
            throw new IllegalStateException("SASL Init already sent to the remote SASL server");
        }
        return this;
    }

    @Override
    public SaslClientContext sendResponse(Binary response) {
        if (responseRequired) {
            saslHandler.context().fireWrite(new SaslResponse().setResponse(response));
            responseRequired = false;
        } else {
            throw new IllegalStateException("SASL Response is not currently expected by remote server");
        }
        return this;
    }

    //----- SASL Frame event handlers for Client negotiations

    @Override
    public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
        state = SaslState.AUTHENTICATION_FAILED;
        saslHandler.transportFailed(context, new IllegalStateException(
            "Remote does not support SASL authentication."));
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
        if (!headerReceived) {
            headerReceived = true;
            state = SaslState.AUTHENTICATING;
            if (!headerWritten) {
                context.fireWrite(AMQPHeader.getSASLHeader());
                headerWritten = true;
            }
        } else {
            saslHandler.transportFailed(context, new IllegalStateException(
                "Remote sent illegal additional SASL headers."));
        }
    }

    @Override
    public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
        if (!mechanismsReceived) {
            serverMechanisms = saslMechanisms.getSaslServerMechanisms();
            client.handleSaslMechanisms(this, getServerMechanisms());
        } else {
            saslHandler.transportFailed(context, new IllegalStateException(
                "Remote sent illegal additional SASL Mechanisms frame."));
        }
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
        if (mechanismsReceived) {
            client.handleSaslChallenge(this, saslChallenge.getChallenge());
        } else {
            saslHandler.transportFailed(context, new IllegalStateException(
                "Remote sent unexpected SASL Challenge frame."));
        }
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
        done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.values()[saslOutcome.getCode().ordinal()]);
        client.handleSaslOutcome(this, getSaslOutcome(), saslOutcome.getAdditionalData());
    }

    //----- Registration of SASL client event handlers


    //----- Internal methods and super overrides

    @Override
    ProtonSaslClientContext handleContextInitialization(ProtonEngine engine) {
        getListener().initialize(this);
        return this;
    }

    //----- Default SASL Client listener fails the exchange

    private static class ProtonDefaultSaslClientListener implements SaslClientListener {

        private final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");

        @Override
        public void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms) {
           if (mechanisms != null && Arrays.binarySearch(mechanisms, ANONYMOUS) > 0) {
               context.sendChosenMechanism(ANONYMOUS, null, null);
           } else {
               ProtonSaslContext sasl = (ProtonSaslContext) context;
               sasl.done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_SYS);
               // TODO - Fail engine
           }
       }

        @Override
        public void handleSaslChallenge(SaslClientContext context, Binary challenge) {
            // TODO - Fail engine
        }

        @Override
        public void handleSaslOutcome(SaslClientContext context, org.apache.qpid.proton4j.engine.sasl.SaslOutcome outcome, Binary additional) {
        }
    }
}
