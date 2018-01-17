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
import org.apache.qpid.proton4j.transport.SaslContext.SaslOutcome;
import org.apache.qpid.proton4j.transport.SaslContext.SaslState;

/**
 * The State engine for a Sasl exchange.
 *
 * TODO - Should we have a client and server context ?
 *
 */
public class ProtonSaslContext {

    enum Role { CLIENT, SERVER };

    private ProtonSaslHandler saslHandler;

    private SaslOutcome outcome = SaslOutcome.PN_SASL_NONE;
    private SaslState state = SaslState.PN_SASL_IDLE;
    private String hostname;

    private Symbol[] serverMechanisms;
    private Symbol clientMechanism;

    private boolean done;
    private Symbol chosenMechanism;

    private Role role;

    private ProtonBuffer pending;

    private boolean headerWritten;
    private Binary challengeResponse;
    private boolean initReceived;
    private boolean mechanismsSent;
    private boolean initSent;

    public ProtonSaslContext(ProtonSaslHandler handler, Role role) {
        this.saslHandler = handler;
        this.role = role;
    }

    public boolean isDone() {
        return done;
    }

    /**
     * Called from a client to indicate the chosen SASL mechanism for the exchange.
     *
     * @param mechanism
     *      The SASL mechanism chosen from the server provided set.
     */
    public void setClientMechanism(Symbol mechanism) {
        this.clientMechanism = mechanism;
    }
}
