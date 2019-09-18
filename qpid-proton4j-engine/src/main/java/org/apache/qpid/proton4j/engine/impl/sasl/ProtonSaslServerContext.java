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
import org.apache.qpid.proton4j.amqp.security.SaslCode;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.sasl.SaslServerContext;
import org.apache.qpid.proton4j.engine.sasl.SaslServerListener;

final class ProtonSaslServerContext extends ProtonSaslContext implements SaslServerContext {

    // TODO - Update SASL State in driver

    private SaslServerListener server = new ProtonDefaultSaslServerListener();

    // Work state trackers
    private boolean headerWritten;
    private boolean headerReceived;
    private boolean mechanismsSent;
    private boolean mechanismChosen;
    private boolean responseRequired;

    ProtonSaslServerContext(ProtonSaslHandler handler) {
        super(handler);
    }

    @Override
    public Role getRole() {
        return Role.SERVER;
    }

    @Override
    public SaslServerContext setListener(SaslServerListener listener) {
        Objects.requireNonNull(listener, "Cannot configure a null SaslServerListnener");
        this.server = listener;
        return this;
    }

    @Override
    public SaslServerListener getListener() {
        return server;
    }

    @Override
    public SaslServerContext sendMechanisms(Symbol[] mechanisms) {
        Objects.requireNonNull(mechanisms);

        if (!mechanismsSent) {
            saslHandler.context().fireWrite(new SaslMechanisms().setSaslServerMechanisms(mechanisms));
            mechanisms = Arrays.copyOf(mechanisms, mechanisms.length);
            mechanismsSent = true;
        } else {
            throw new IllegalStateException("SASL Mechanisms already sent to client");
        }
        return this;
    }

    @Override
    public SaslServerContext sendChallenge(Binary challenge) {
        if (headerWritten && mechanismsSent && !responseRequired) {
            saslHandler.context().fireWrite(new SaslChallenge().setChallenge(challenge));
            responseRequired = true;
        } else {
            throw new IllegalStateException("SASL Challenge sent when state does not allow it");
        }
        return this;
    }

    @Override
    public SaslServerContext sendOutcome(org.apache.qpid.proton4j.engine.sasl.SaslOutcome outcome, Binary additional) {
        if (headerWritten && mechanismsSent && !responseRequired) {
            SaslOutcome saslOutcome = new SaslOutcome();

            saslOutcome.setCode(SaslCode.values()[outcome.ordinal()]);
            saslOutcome.setAdditionalData(additional);

            saslHandler.context().fireWrite(saslOutcome);

            done(outcome);
        } else {
            throw new IllegalStateException("SASL Challenge sent when state does not allow it");
        }
        return this;
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

        server.handleSaslHeader(this, header);
    }

    @Override
    public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
        if (mechanismChosen) {
            context.fireFailed(new IllegalStateException("SASL Handler received second SASL Init"));
            return;
        }

        hostname = saslInit.getHostname();
        chosenMechanism = saslInit.getMechanism();
        mechanismChosen = true;

        server.handleSaslInit(this, chosenMechanism, saslInit.getInitialResponse());
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
        if (responseRequired) {
            server.handleSaslResponse(this, saslResponse.getResponse());
        } else {
            context.fireFailed(new IllegalStateException("SASL Response received when none was expected"));
        }
    }

    //----- Internal methods and super overrides

    @Override
    void handleEngineStarting(ProtonEngine engine) {
        getListener().initialize(this);
    }

    //----- Default SASL Server listener that fails any negotiations

    public static class ProtonDefaultSaslServerListener implements SaslServerListener {

        @Override
        public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
            // TODO Auto-generated method stub
        }

        @Override
        public void handleSaslInit(SaslServerContext context, Symbol mechanism, Binary initResponse) {
            // TODO Auto-generated method stub
        }

        @Override
        public void handleSaslResponse(SaslServerContext context, Binary response) {
            // TODO Auto-generated method stub
        }
    }
}
