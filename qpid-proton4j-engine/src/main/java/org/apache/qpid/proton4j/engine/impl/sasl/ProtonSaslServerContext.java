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
import org.apache.qpid.proton4j.amqp.security.SaslCode;
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

    // TODO - Consider routing these sends through the engine proper such that the engine could
    //        track outbound work for a future timeout of SASL feature, and also ensures that all
    //        handlers see the outbound work item.
    //
    //          engine.pipeline.fireWrite(X);

    @Override
    public SaslServerContext sendMechanisms(Symbol[] mechanisms) {
        Objects.requireNonNull(mechanisms);
        saslWriteContext().handleMechanisms(new SaslMechanisms().setSaslServerMechanisms(mechanisms), saslHandler.context());
        return this;
    }

    @Override
    public SaslServerContext sendChallenge(ProtonBuffer challenge) {
        Objects.requireNonNull(challenge);
        saslWriteContext().handleChallenge(new SaslChallenge().setChallenge(challenge), saslHandler.context());
        return this;
    }

    @Override
    public SaslServerContext sendOutcome(org.apache.qpid.proton4j.engine.sasl.SaslOutcome outcome, ProtonBuffer additional) {
        Objects.requireNonNull(outcome);

        SaslOutcome saslOutcome = new SaslOutcome();
        saslOutcome.setCode(SaslCode.values()[outcome.ordinal()]);
        saslOutcome.setAdditionalData(additional);

        saslWriteContext().handleOutcome(saslOutcome, saslHandler.context());
        return this;
    }

    @Override
    public SaslServerContext saslFailure(SaslException failure) {
        done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_PERM);
        saslHandler.engine().pipeline().fireFailed(failure);
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
            context.fireFailed(new IllegalStateException(
                "Unexpected AMQP Header before SASL Authentication completed."));
        }

        @Override
        public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
            if (headerReceived) {
                context.fireFailed(new IllegalStateException(
                    "Unexpected second SASL Header read before SASL Authentication completed."));
            } else {
                headerReceived = true;
            }

            if (!headerWritten) {
                context.fireWrite(header);
                headerWritten = true;
                state = SaslState.AUTHENTICATING;
            }

            try {
                server.handleSaslHeader(ProtonSaslServerContext.this, header);
            } catch (Throwable error) {
                context.fireFailed(error);
            }
        }
    }

    private final class HeaderWriteContext implements HeaderHandler<EngineHandlerContext> {

        @Override
        public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
            context.fireFailed(new IllegalStateException(
                "Unexpected AMQP Header write before SASL Authentication completed."));
        }

        @Override
        public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
            if (headerWritten) {
                context.fireFailed(new IllegalStateException(
                    "Unexpected SASL write following a previous header send."));
            }
            headerWritten = true;
            context.fireWrite(AMQPHeader.getSASLHeader());
        }
    }

    private final class SaslReadContext implements SaslPerformativeHandler<EngineHandlerContext> {

        @Override
        public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
            context.fireFailed(new IllegalStateException(
                "Unexpected SASL Mechanisms Frame received at SASL Server."));
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

            try {
                server.handleSaslInit(ProtonSaslServerContext.this, chosenMechanism, saslInit.getInitialResponse());
            } catch (Throwable error) {
                context.fireFailed(error);
            }
        }

        @Override
        public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
            context.fireFailed(new IllegalStateException(
                "Unexpected SASL Challenege Frame received at SASL Server."));
        }

        @Override
        public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
            if (responseRequired) {
                try {
                    server.handleSaslResponse(ProtonSaslServerContext.this, saslResponse.getResponse());
                } catch (Throwable error) {
                    context.fireFailed(error);
                }
            } else {
                context.fireFailed(new IllegalStateException("SASL Response received when none was expected"));
            }
        }

        @Override
        public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
            context.fireFailed(new IllegalStateException(
                "Unexpected SASL Outcome Frame received at SASL Server."));
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
                context.fireFailed(new IllegalStateException("SASL Mechanisms already sent to client"));
            }
        }

        @Override
        public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
            context.fireFailed(new IllegalStateException(
                "Unexpected SASL Init Frame write attempted on SASL Server."));
        }

        @Override
        public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
            if (headerWritten && mechanismsSent && !responseRequired) {
                context.fireWrite(saslChallenge);
                responseRequired = true;
            } else {
                context.fireFailed(new IllegalStateException("SASL Challenge sent when state does not allow it"));
            }
        }

        @Override
        public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
            context.fireFailed(new IllegalStateException(
                "Unexpected SASL Response Frame write attempted on SASL Server."));
        }

        @Override
        public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
            if (headerWritten && mechanismsSent && !responseRequired) {
                done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.valueOf(saslOutcome.getCode().getValue().byteValue()));
                context.fireWrite(saslOutcome);
            } else {
                context.fireFailed(new IllegalStateException("SASL Outcome sent when state does not allow it"));
            }
        }
    }

    //----- Default SASL Server listener that fails any negotiations

    // TODO - Default behavior when server not configured ?
    //        Present a Mechanism like PLAIN that always fails the authentication ?
    //        Default to insecure and just offer ANONYMOUS ?

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
            // TODO Failure of engine, response not expected ?
        }
    }
}
