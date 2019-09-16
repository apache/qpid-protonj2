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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.sasl.SaslServerContext;

public class ProtonSaslServerContext extends ProtonSaslContext implements SaslServerContext {

    // Work state trackers
    private boolean headerWritten;
    private boolean headerReceived;
    private boolean mechanismsSent;
    private boolean mechanismChosen;

    public ProtonSaslServerContext(ProtonSaslHandler handler) {
        super(handler);
    }

    @Override
    public Role getRole() {
        return Role.SERVER;
    }

    @Override
    public SaslServerContext sendMechanisms(Symbol[] mechanisms) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SaslServerContext sendChallenge(Binary challenge) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SaslServerContext sendOutcome(org.apache.qpid.proton4j.engine.sasl.SaslOutcome outcome, Binary additional) {
        // TODO Auto-generated method stub
        return null;
    }

    //----- Transport event handlers -----------------------------------------//

    @Override
    public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
        context.fireFailed(new IllegalStateException(
            "Unexpected AMQP Header before SASL Authentication completed."));
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
        if (!headerWritten) {
            context.fireWrite(header);
            headerWritten = true;
        }

        if (headerReceived) {
            context.fireFailed(new IllegalStateException(
                "Unexpected second SASL Header read before SASL Authentication completed."));
        } else {
            headerReceived = true;
        }

        // TODO - When to fail when no mechanisms set, now or on some earlier started / connected event ?
        //        Or allow it to be empty and await an async write of a SaslInit frame etc ?
        if (serverMechanisms == null || serverMechanisms.length == 0) {
            context.fireFailed(new IllegalStateException("SASL Server has no configured mechanisms"));
        }

        // TODO - Check state, then send mechanisms
        SaslMechanisms mechanisms = new SaslMechanisms();
        mechanisms.setSaslServerMechanisms(serverMechanisms);

        // Send the server mechanisms now.
        context.fireWrite(mechanisms);
        mechanismsSent = true;
//        state = SaslStates.SASL_STEP;
    }

    @Override
    public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
        if (mechanismChosen) {
            // TODO - Handle SaslInit already read with better error
            context.fireFailed(new IllegalStateException("SASL Handler received second SASL Init"));
            return;
        }

        hostname = saslInit.getHostname();
        chosenMechanism = saslInit.getMechanism();
        mechanismChosen = true;

        // TODO - Should we use ProtonBuffer slices as response containers ?
//        listener.onSaslInit(this, saslInit.getInitialResponse());

        pumpServerState(context);
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
        // TODO - Should we use ProtonBuffer slices as response containers ?
//        listener.onSaslResponse(this, saslResponse.getResponse());

        pumpServerState(context);
    }

    private void pumpServerState(EngineHandlerContext context) {
//        if (state == SaslStates.SASL_STEP && getChallenge() != null) {
//            SaslChallenge challenge = new SaslChallenge();
//            challenge.setChallenge(getChallenge());
//            setChallenge(null);
//            context.fireWrite(challenge);
//        }
//
//        if (getOutcome() != SaslOutcomes.SASL_NONE) {
//            SaslOutcome outcome = new SaslOutcome();
//            // TODO Clean up SaslCode mechanics
//            outcome.setCode(SaslCode.values()[getOutcome().getCode()]);
//            outcome.setAdditionalData(additionalData);
//            setAdditionalData(null);
//            done = true;
//            context.fireWrite(outcome);
//        }
    }

    //----- Registration of SASL server event handlers

    private Consumer<SaslServerContext> initializationHandler; // TODO - Change to engine started handler ?

    // TODO - Defaults that will respond but eventually fail the SASL exchange.

    private BiConsumer<Symbol, Binary> initHandler;
    private Consumer<Binary> responseHandler;

    @Override
    public void initializationHandler(Consumer<SaslServerContext> handler) {
        if (handler != null) {
            this.initializationHandler = handler;
        } else {
            this.initializationHandler = (context) -> {};
        }
    }

    @Override
    public void saslInitHandler(BiConsumer<Symbol, Binary> handler) {
        Objects.requireNonNull(handler);
        this.initHandler = handler;
    }

    @Override
    public void saslResponseHandler(Consumer<Binary> handler) {
        Objects.requireNonNull(handler);
        this.responseHandler = handler;
    }
}
