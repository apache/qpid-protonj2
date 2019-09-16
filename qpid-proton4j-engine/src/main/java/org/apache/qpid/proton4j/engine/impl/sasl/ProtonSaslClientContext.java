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
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;

public class ProtonSaslClientContext extends ProtonSaslContext implements SaslClientContext {

    // Work state trackers
    private boolean headerWritten;
    private boolean headerReceived;
    private boolean mechanismsReceived;
    private boolean mechanismChosen;

    public ProtonSaslClientContext(ProtonSaslHandler handler) {
        super(handler);
    }

    @Override
    public Role getRole() {
        return Role.CLIENT;
    }

    @Override
    public SaslClientContext sendSASLHeader() {
        // TODO Auto-generated method stub
        if (!headerWritten) {

        }
        return this;
    }

    @Override
    public SaslClientContext sendChosenMechanism(Symbol mechanism, String host, Binary initialResponse) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public SaslClientContext sendResponse(Binary response) {
        // TODO Auto-generated method stub
        return this;
    }

    //----- SASL Frame event handlers for Client negotiations

    @Override
    public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
        saslHandler.transportFailed(context, new IllegalStateException(
            "Remote does not support SASL authentication."));
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
        if (!headerReceived) {
            headerReceived = true;
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
            // TODO - Track state of the SASL negotiations
            serverMechanisms = saslMechanisms.getSaslServerMechanisms();
            mechanismsHandler.accept(getServerMechanisms());
        } else {
            saslHandler.transportFailed(context, new IllegalStateException(
                "Remote sent illegal additional SASL Mechanisms frame."));
        }
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
        // TODO - Check state, are we ready
        challengeHandler.accept(saslChallenge.getChallenge());
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
//        this.outcome = SaslOutcomes.valueOf(outcome.getCode());
//        if (state != SaslStates.SASL_IDLE) {
//            state = classifyStateFromOutcome(outcome);
//        }

        done = true;
        outcomeHandler.accept(saslOutcome.getAdditionalData());
    }

    //----- Registration of SASL client event handlers

    private Consumer<SaslClientContext> initializationHandler; // TODO - Change to engine started handler ?

    // TODO - Defaults that will respond but eventually fail the SASL exchange.

    private Consumer<Symbol[]> mechanismsHandler;
    private Consumer<Binary> challengeHandler;
    private Consumer<Binary> outcomeHandler;

    @Override
    public void initializationHandler(Consumer<SaslClientContext> handler) {
        if (handler != null) {
            this.initializationHandler = handler;
        } else {
            this.initializationHandler = (context) -> {};
        }
    }

    @Override
    public void saslMechanismsHandler(Consumer<Symbol[]> handler) {
        Objects.requireNonNull(handler);
        this.mechanismsHandler = handler;
    }

    @Override
    public void saslChallengeHandler(Consumer<Binary> handler) {
        Objects.requireNonNull(handler);
        this.challengeHandler = handler;
    }

    @Override
    public void saslOutcomeHandler(Consumer<Binary> handler) {
        Objects.requireNonNull(handler);
        this.outcomeHandler = handler;
    }
}
