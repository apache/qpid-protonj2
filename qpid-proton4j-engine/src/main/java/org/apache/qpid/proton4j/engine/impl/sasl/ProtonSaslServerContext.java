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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

final class ProtonSaslServerContext extends ProtonSaslContext implements SaslServerContext {

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

        saslStartedHandler.accept(header);
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

        initHandler.accept(chosenMechanism, saslInit.getInitialResponse());
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
        if (responseRequired) {
            responseHandler.accept(saslResponse.getResponse());
        } else {
            context.fireFailed(new IllegalStateException("SASL Response received when none was expected"));
        }
    }

    //----- Registration of SASL server event handlers

    // TODO - Listener interface feels more intuitive and useful for SASL than the handler paradigm

    private Consumer<SaslServerContext> initializationHandler; // TODO - Change to engine started handler ?

    // TODO - Defaults that will respond but eventually fail the SASL exchange.

    private Consumer<AMQPHeader> saslStartedHandler;
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
    public void saslStartedHandler(Consumer<AMQPHeader> handler) {
        Objects.requireNonNull(handler);
        this.saslStartedHandler = handler;
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

    //----- Internal methods and super overrides

    @Override
    void handleEngineStarting(ProtonEngine engine) {
        initializationHandler.accept(this);
    }
}
