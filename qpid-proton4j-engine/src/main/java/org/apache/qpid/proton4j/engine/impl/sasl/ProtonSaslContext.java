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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.sasl.SaslContext;
import org.apache.qpid.proton4j.engine.sasl.SaslOutcome;

/**
 * The State engine for a SASL exchange.
 */
abstract class ProtonSaslContext implements SaslContext, AMQPHeader.HeaderHandler<EngineHandlerContext>, SaslPerformative.SaslPerformativeHandler<EngineHandlerContext> {

    protected ProtonSaslHandler saslHandler;

    // Client negotiations tracking.
    protected Symbol[] serverMechanisms;
    protected Symbol chosenMechanism;
    protected String hostname;
    protected SaslState state = SaslState.IDLE;
    protected SaslOutcome outcome;

    private boolean done;

    ProtonSaslContext(ProtonSaslHandler handler) {
        this.saslHandler = handler;
    }

    /**
     * Return the Role of the context implementation.
     *
     * @return the Role of this SASL Context
     */
    @Override
    public abstract Role getRole();

    /**
     * @return true if this is a SASL server context.
     */
    @Override
    public boolean isServer() {
        return getRole() == Role.SERVER;
    }

    /**
     * @return true if this is a SASL client context.
     */
    @Override
    public boolean isClient() {
        return getRole() == Role.SERVER;
    }

    @Override
    public SaslOutcome getSaslOutcome() {
        return outcome;
    }

    @Override
    public SaslState getSaslState() {
        return state;
    }

    /**
     * @return true if SASL authentication has completed
     */
    @Override
    public boolean isDone() {
        return done;
    }

    ProtonSaslContext done(SaslOutcome outcome) {
        this.done = true;
        this.outcome = outcome;
        return this;
    }

    @Override
    public Symbol getChosenMechanism() {
        return chosenMechanism;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public Symbol[] getServerMechanisms() {
        return serverMechanisms != null ? Arrays.copyOf(serverMechanisms, serverMechanisms.length) : null;
    }

    ProtonSaslHandler getHandler() {
        return saslHandler;
    }

    //----- Handle AMQP Header input

    @Override
    public abstract void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context);

    @Override
    public abstract void handleSASLHeader(AMQPHeader header, EngineHandlerContext context);

    //----- Entry point for SASL Performative processing

    @Override
    public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
        context.fireFailed(new IllegalStateException(
            "Unexpected SASL Mechanisms Frame received."));
    }

    @Override
    public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
        context.fireFailed(new IllegalStateException(
            "Unexpected SASL Init Frame received."));
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
        context.fireFailed(new IllegalStateException(
            "Unexpected SASL Challenge Frame received."));
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
        context.fireFailed(new IllegalStateException(
            "Unexpected SASL Response Frame received."));
    }

    @Override
    public void handleOutcome(org.apache.qpid.proton4j.amqp.security.SaslOutcome saslOutcome, EngineHandlerContext context) {
        context.fireFailed(new IllegalStateException(
            "Unexpected SASL Outcome Frame received."));
    }

    //----- Internal events for the specific contexts to respond to

    abstract void handleEngineStarting(ProtonEngine engine);

}
