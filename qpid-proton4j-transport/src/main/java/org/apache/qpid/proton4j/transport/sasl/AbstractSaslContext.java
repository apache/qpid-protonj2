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
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslStates;

/**
 * The State engine for a Sasl exchange.
 */
public abstract class AbstractSaslContext implements SaslPerformative.SaslPerformativeHandler<TransportHandlerContext> {

    enum Role { CLIENT, SERVER };

    protected SaslHandler saslHandler;

    protected SaslOutcomes outcome = SaslOutcomes.PN_SASL_NONE;
    protected SaslStates state = SaslStates.PN_SASL_IDLE;

    protected Symbol[] serverMechanisms;
    protected Symbol chosenMechanism;
    protected String hostname;

    protected boolean done;

    protected ProtonBuffer pending;

    protected boolean mechanismsSent;
    protected boolean mechanismsReceived;

    protected boolean headerWritten;
    protected boolean headerReceived;

    protected boolean initReceived;
    protected boolean initSent;

    protected Binary challengeResponse;

    public AbstractSaslContext(SaslHandler handler) {
        this.saslHandler = handler;
    }

    /**
     * Return the Role of the context implementation.
     *
     * @return the Role of this SASL Context
     */
    abstract Role getRole();

    /**
     * @return true if this is a SASL server context.
     */
    public boolean isServer() {
        return getRole() == Role.SERVER;
    }

    /**
     * @return true if this is a SASL client context.
     */
    public boolean isClient() {
        return getRole() == Role.SERVER;
    }

    /**
     * @return true if SASL authentication has completed
     */
    public boolean isDone() {
        return done;
    }

    //----- Handle AMQP Header input -----------------------------------------//

    public abstract void handleAMQPHeader(TransportHandlerContext context, AMQPHeader header);

    //----- Entry point for Sasl Performative processing ---------------------//

    @Override
    public void handleMechanisms(SaslMechanisms saslMechanisms, Binary payload, TransportHandlerContext context) {
        // TODO
    }

    @Override
    public void handleInit(SaslInit saslInit, Binary payload, TransportHandlerContext context) {
        // TODO
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, Binary payload, TransportHandlerContext context) {
        // TODO
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, Binary payload, TransportHandlerContext context) {
        // TODO
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, Binary payload, TransportHandlerContext context) {
        // TODO
    }
}
