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
import org.apache.qpid.proton4j.amqp.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader.HeaderHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.proton4j.engine.impl.ProtonAttachments;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.sasl.SaslContext;
import org.apache.qpid.proton4j.engine.sasl.SaslOutcome;

/**
 * The State engine for a SASL exchange.
 */
abstract class ProtonSaslContext implements SaslContext {

    protected final ProtonSaslHandler saslHandler;

    private ProtonAttachments attachments;

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

    @Override
    public ProtonAttachments getAttachments() {
        return attachments == null ? attachments = new ProtonAttachments() : attachments;
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
        return getRole() == Role.CLIENT;
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
     * @return true if SASL authentication has completed regardless of outcome
     */
    @Override
    public boolean isDone() {
        return done;
    }

    ProtonSaslContext done(SaslOutcome outcome) {
        this.done = true;
        this.outcome = outcome;
        this.state = outcome == SaslOutcome.SASL_OK ? SaslState.AUTHENTICATED : SaslState.AUTHENTICATION_FAILED;

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

    //----- Internal events for the specific contexts to respond to

    abstract ProtonSaslContext handleContextInitialization(ProtonEngine engine);

    //----- Read and Write contexts that will see all inbound and outbound activity

    abstract HeaderHandler<EngineHandlerContext> headerReadContext();

    abstract HeaderHandler<EngineHandlerContext> headerWriteContext();

    abstract SaslPerformativeHandler<EngineHandlerContext> saslReadContext();

    abstract SaslPerformativeHandler<EngineHandlerContext> saslWriteContext();

}
