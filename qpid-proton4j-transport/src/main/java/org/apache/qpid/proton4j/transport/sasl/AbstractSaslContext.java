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
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslOutcome;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslState;

/**
 * The State engine for a Sasl exchange.
 */
public abstract class AbstractSaslContext {

    enum Role { CLIENT, SERVER };

    protected SaslHandler saslHandler;

    protected SaslOutcome outcome = SaslOutcome.PN_SASL_NONE;
    protected SaslState state = SaslState.PN_SASL_IDLE;
    protected String hostname;

    protected Symbol[] serverMechanisms;
    protected Symbol clientMechanism;

    protected boolean done;
    protected Symbol chosenMechanism;

    protected ProtonBuffer pending;

    protected boolean headerWritten;
    protected Binary challengeResponse;
    protected boolean initReceived;
    protected boolean mechanismsSent;
    protected boolean initSent;

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
}
