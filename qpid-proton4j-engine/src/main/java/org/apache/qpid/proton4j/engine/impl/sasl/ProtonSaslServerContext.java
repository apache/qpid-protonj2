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

import javax.security.sasl.SaslException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader.HeaderHandler;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.sasl.SaslServerContext;
import org.apache.qpid.proton4j.engine.sasl.SaslServerListener;

final class ProtonSaslServerContext extends ProtonSaslContext implements SaslServerContext {

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

    //----- SASL negotiations API

    @Override
    public SaslServerContext sendMechanisms(Symbol[] mechanisms) {
        Objects.requireNonNull(mechanisms);
        saslHandler.engine().pipeline().fireWrite(new SaslMechanisms().setSaslServerMechanisms(mechanisms));
        return this;
    }

    @Override
    public SaslServerContext sendChallenge(ProtonBuffer challenge) {
        Objects.requireNonNull(challenge);
        saslHandler.engine().pipeline().fireWrite(new SaslChallenge().setChallenge(challenge));
        return this;
    }

    @Override
    public SaslServerContext sendOutcome(org.apache.qpid.proton4j.engine.sasl.SaslOutcome outcome, ProtonBuffer additional) {
        Objects.requireNonNull(outcome);
        saslHandler.engine().pipeline().fireWrite(new SaslOutcome().setCode(outcome.saslCode()).setAdditionalData(additional));
        return this;
    }

    @Override
    public SaslServerContext saslFailure(SaslException failure) {
        if (!isDone()) {
            done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_PERM);
            saslHandler.engine().engineFailed(failure);
        }
        return this;
    }

    //----- SASL Handler API sink for all reads and writes

    @Override
    ProtonSaslServerContext handleContextInitialization(ProtonEngine engine) {
        getListener().initialize(this);
        return this;
    }

    @Override
    HeaderHandler<EngineHandlerContext> headerReadContext() {
        return this.headerReadContext;
    }

    @Override
    HeaderHandler<EngineHandlerContext> headerWriteContext() {
        return this.headerWriteContext;
    }

    @Override
    SaslPerformativeHandler<EngineHandlerContext> saslReadContext() {
        return this.saslReadContext;
    }

    @Override
    SaslPerformativeHandler<EngineHandlerContext> saslWriteContext() {
        return this.saslWriteContext;
    }

    //----- Read and Write contexts for SASL and Header types

    private final HeaderReadContext headerReadContext = new HeaderReadContext();
    private final HeaderWriteContext headerWriteContext = new HeaderWriteContext();
    private final SaslReadContext saslReadContext = new SaslReadContext();
    private final SaslWriteContext saslWriteContext = new SaslWriteContext();

    private final class HeaderReadContext implements HeaderHandler<EngineHandlerContext> {

        @Override
        public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
            // Raw AMQP Header shouldn't arrive before the SASL negotiations are done.
            context.fireWrite(AMQPHeader.getSASLHeader());
            throw new ProtocolViolationException("Unexpected AMQP Header before SASL Authentication completed.");
        }

        @Override
        public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
            if (headerReceived) {
                throw new ProtocolViolationException("Unexpected second SASL Header read before SASL Authentication completed.");
            } else {
                headerReceived = true;
            }

            if (!headerWritten) {
                context.fireWrite(header);
                headerWritten = true;
                state = SaslState.AUTHENTICATING;
            }

            server.handleSaslHeader(ProtonSaslServerContext.this, header);
        }
    }

    private final class HeaderWriteContext implements HeaderHandler<EngineHandlerContext> {

        @Override
        public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected AMQP Header write before SASL Authentication completed.");
        }

        @Override
        public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
            if (headerWritten) {
                throw new ProtocolViolationException("Unexpected SASL write following a previous header send.");
            }
            headerWritten = true;
            context.fireWrite(AMQPHeader.getSASLHeader());
        }
    }

    private final class SaslReadContext implements SaslPerformativeHandler<EngineHandlerContext> {

        @Override
        public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Mechanisms Frame received at SASL Server.");
        }

        @Override
        public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
            if (mechanismChosen) {
                throw new ProtocolViolationException("SASL Handler received second SASL Init");
            }

            hostname = saslInit.getHostname();
            chosenMechanism = saslInit.getMechanism();
            mechanismChosen = true;

            server.handleSaslInit(ProtonSaslServerContext.this, chosenMechanism, saslInit.getInitialResponse());
        }

        @Override
        public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Challenege Frame received at SASL Server.");
        }

        @Override
        public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
            if (responseRequired) {
                server.handleSaslResponse(ProtonSaslServerContext.this, saslResponse.getResponse());
            } else {
                throw new ProtocolViolationException("SASL Response received when none was expected");
            }
        }

        @Override
        public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Outcome Frame received at SASL Server.");
        }
    }

    private final class SaslWriteContext implements SaslPerformativeHandler<EngineHandlerContext> {

        @Override
        public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
            if (!mechanismsSent) {
                context.fireWrite(saslMechanisms);
                serverMechanisms = Arrays.copyOf(saslMechanisms.getSaslServerMechanisms(), saslMechanisms.getSaslServerMechanisms().length);
                mechanismsSent = true;
            } else {
                throw new ProtocolViolationException("SASL Mechanisms already sent to client");
            }
        }

        @Override
        public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Init Frame write attempted on SASL Server.");
        }

        @Override
        public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
            if (headerWritten && mechanismsSent && !responseRequired) {
                context.fireWrite(saslChallenge);
                responseRequired = true;
            } else {
                throw new ProtocolViolationException("SASL Challenge sent when state does not allow it");
            }
        }

        @Override
        public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Response Frame write attempted on SASL Server.");
        }

        @Override
        public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
            if (headerWritten && mechanismsSent && !responseRequired) {
                done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.valueOf(saslOutcome.getCode().getValue().byteValue()));
                context.fireWrite(saslOutcome);
                // Request that the SASL handler be removed from the chain now that we are done with the SASL
                // exchange, the engine driver will remain in place holding the state for later examination.
                context.engine().pipeline().remove(saslHandler);
            } else {
                throw new ProtocolViolationException("SASL Outcome sent when state does not allow it");
            }
        }
    }

    //----- Default SASL Server listener that fails any negotiations

    public static class ProtonDefaultSaslServerListener implements SaslServerListener {

        private static final Symbol[] PLAIN = { Symbol.valueOf("PLAIN") };

        @Override
        public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
            context.sendMechanisms(PLAIN);
        }

        @Override
        public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
            context.sendOutcome(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_AUTH, null);
        }

        @Override
        public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
            throw new ProtocolViolationException("SASL Response arrived when no challenge was issued or supported.");
        }
    }
}
