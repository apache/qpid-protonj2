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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslStates;

public class SaslClientContext extends AbstractSaslContext {

    private final SaslClientListener listener;

    public SaslClientContext(SaslHandler handler, SaslClientListener listener) {
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
        this.chosenMechanism = Symbol.valueOf(mechanism);
    }

    //----- SASL Frame event handlers-----------------------------------------//

    @Override
    public void handleHeaderFrame(TransportHandlerContext context, HeaderFrame header) {
        if (!header.getBody().isSaslHeader()) {
            // TODO - Error on server not supporting SASL
            context.fireFailed(new IllegalStateException(
                "Remote does not support SASL authentication."));
        }
    }

    @Override
    public void handleMechanisms(SaslMechanisms saslMechanisms, TransportHandlerContext context) {
        serverMechanisms = saslMechanisms.getSaslServerMechanisms();

        listener.onSaslMechanisms(this, getServerMechanisms());

        // TODO - How is the listener driving output, send methods ?
        //        We probably want to support asynchronous triggering
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, TransportHandlerContext context) {
        if (saslChallenge.getChallenge() != null) {
            // TODO - How to present the response data, perhaps pass as arg to listener
            //        instead of storing pending bytes.
            //setPending(saslChallenge.getChallenge().asByteBuffer());
        }

        listener.onSaslChallenge(this);

        // TODO - How is the listener driving output, send methods ?
        //        We probably want to support asynchronous triggering
        // listener.onSaslChallenge(this, saslChallenge.getChallenge());
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, TransportHandlerContext context) {
        for (SaslOutcomes outcome : SaslOutcomes.values()) {
            // TODO - How to present the response data, perhaps pass as arg to listener
            //        instead of storing pending bytes.
            //if (saslOutcome.getAdditionalData() != null) {
            //    setPending(saslOutcome.getAdditionalData().asByteBuffer());
            //}

            if (outcome.getCode() == saslOutcome.getCode().ordinal()) {
                this.outcome = outcome;
                if (state != SaslStates.PN_SASL_IDLE) {
                    state = classifyStateFromOutcome(outcome);
                }
                break;
            }
        }

        done = true;

        listener.onSaslOutcome(this);

        // TODO - Also pass the Outcome Enum and possible additional data
        // listener.onSaslOutcome(this, outcome, saslOutcome.getAdditionalData());
    }
}
