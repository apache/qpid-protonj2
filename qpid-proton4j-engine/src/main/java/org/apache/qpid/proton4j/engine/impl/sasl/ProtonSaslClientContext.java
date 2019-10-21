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
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.exceptions.SaslAuthenticationException;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientListener;

final class ProtonSaslClientContext extends ProtonSaslContext implements SaslClientContext {

    private SaslClientListener client = new ProtonDefaultSaslClientListener();

    // Work state trackers
    private boolean headerWritten;
    private boolean headerReceived;
    private boolean mechanismsReceived;
    private boolean mechanismChosen;
    private boolean responseRequired;

    private AMQPHeader pausedAMQPHeader;

    public ProtonSaslClientContext(ProtonSaslHandler handler) {
        super(handler);
    }

    @Override
    public Role getRole() {
        return Role.CLIENT;
    }

    @Override
    public SaslClientContext setListener(SaslClientListener listener) {
        Objects.requireNonNull(listener, "Cannot configure a null SaslClientListener");
        this.client = listener;
        return this;
    }

    @Override
    public SaslClientListener getListener() {
        return client;
    }

    //----- SASL negotiations API

    @Override
    public SaslClientContext sendSASLHeader() throws IllegalStateException, EngineStateException {
        saslHandler.engine().pipeline().fireWrite(AMQPHeader.getSASLHeader());
        return this;
    }

    @Override
    public SaslClientContext sendChosenMechanism(Symbol mechanism, String hostname, ProtonBuffer initialResponse) throws IllegalStateException, EngineStateException {
        Objects.requireNonNull(mechanism, "Client must choose a mechanism");
        SaslInit saslInit = new SaslInit().setHostname(hostname)
                                          .setMechanism(mechanism)
                                          .setInitialResponse(initialResponse);
        saslHandler.engine().pipeline().fireWrite(saslInit);
        return this;
    }

    @Override
    public SaslClientContext sendResponse(ProtonBuffer response) throws IllegalStateException, EngineStateException {
        Objects.requireNonNull(response);
        saslHandler.engine().pipeline().fireWrite(new SaslResponse().setResponse(response));
        return this;
    }

    @Override
    public SaslClientContext saslFailure(SaslException failure) {
        if (!isDone()) {
            done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_PERM);
            saslHandler.engine().engineFailed(failure);
        }

        return this;
    }

    //----- SASL Handler API sink for all reads and writes

    @Override
    ProtonSaslClientContext handleContextInitialization(ProtonEngine engine) {
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
            state = SaslState.AUTHENTICATION_FAILED;
            context.fireWrite(AMQPHeader.getSASLHeader());
            throw new ProtocolViolationException("Remote does not support SASL authentication.");
        }

        @Override
        public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
            if (!headerReceived) {
                headerReceived = true;
                state = SaslState.AUTHENTICATING;
                if (!headerWritten) {
                    context.fireWrite(AMQPHeader.getSASLHeader());
                    headerWritten = true;
                }
            } else {
                throw new ProtocolViolationException("Remote server sent illegal additional SASL headers.");
            }
        }
    }

    private final class HeaderWriteContext implements HeaderHandler<EngineHandlerContext> {

        @Override
        public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
            // Hold until outcome is known, if success then forward along to start negotiation.
            // Send a SASL header instead so that SASL negotiations can commence with the remote.
            pausedAMQPHeader = header;
            handleSASLHeader(AMQPHeader.getSASLHeader(), context);
        }

        @Override
        public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
            if (!headerWritten) {
                headerWritten = true;
                context.fireWrite(AMQPHeader.getSASLHeader());
            } else {
                throw new ProtocolViolationException("SASL Header already sent to the remote SASL server");
            }
        }
    }

    private final class SaslReadContext implements SaslPerformativeHandler<EngineHandlerContext> {

        @Override
        public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
            if (!mechanismsReceived) {
                serverMechanisms = saslMechanisms.getSaslServerMechanisms();
                client.handleSaslMechanisms(ProtonSaslClientContext.this, getServerMechanisms());
            } else {
                throw new ProtocolViolationException("Remote sent illegal additional SASL Mechanisms frame.");
            }
        }

        @Override
        public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Init Frame received at SASL Client.");
        }

        @Override
        public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
            if (mechanismsReceived) {
                client.handleSaslChallenge(ProtonSaslClientContext.this, saslChallenge.getChallenge());
            } else {
                throw new ProtocolViolationException("Remote sent unexpected SASL Challenge frame.");
            }
        }

        @Override
        public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Response Frame received at SASL Client.");
        }

        @Override
        public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
            done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.values()[saslOutcome.getCode().ordinal()]);

            SaslException saslFailure = null;
            if (!saslOutcome.getCode().equals(SaslCode.OK)) {
                saslFailure = new SaslAuthenticationException(saslOutcome.getCode(), "SASL Authentication Failed");
            }

            try {
                client.handleSaslOutcome(ProtonSaslClientContext.this, getSaslOutcome(), saslOutcome.getAdditionalData());
            } catch (Throwable error) {
                if (saslFailure == null) {
                    saslFailure = new SaslException("Client threw unknown error while processing the outcome", error);
                }
            }

            if (saslFailure == null) {
                if (pausedAMQPHeader != null) {
                    context.fireWrite(pausedAMQPHeader);
                }
            } else {
                context.engine().engineFailed(saslFailure);
            }
        }
    }

    private final class SaslWriteContext implements SaslPerformativeHandler<EngineHandlerContext> {

        @Override
        public void handleMechanisms(SaslMechanisms saslMechanisms, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Mechanisms Frame written from SASL Client.");
        }

        @Override
        public void handleInit(SaslInit saslInit, EngineHandlerContext context) {
            if (!mechanismChosen) {
                chosenMechanism = saslInit.getMechanism();
                hostname = saslInit.getHostname();
                mechanismChosen = true;
                context.fireWrite(saslInit);
            } else {
                throw new ProtocolViolationException("SASL Init already sent to the remote SASL server");
            }
        }

        @Override
        public void handleChallenge(SaslChallenge saslChallenge, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Challenge Frame written from SASL Client.");
        }

        @Override
        public void handleResponse(SaslResponse saslResponse, EngineHandlerContext context) {
            if (responseRequired) {
                responseRequired = false;
                context.fireWrite(saslResponse);
            } else {
                throw new ProtocolViolationException("SASL Response is not currently expected by remote server");
            }
        }

        @Override
        public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
            throw new ProtocolViolationException("Unexpected SASL Outcome Frame written from SASL Client.");
        }
    }

    //----- Default SASL Client listener fails the exchange

    private static class ProtonDefaultSaslClientListener implements SaslClientListener {

        private final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");

        @Override
        public void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms) {
           if (mechanisms != null && Arrays.binarySearch(mechanisms, ANONYMOUS) > 0) {
               context.sendChosenMechanism(ANONYMOUS, null, ProtonByteBufferAllocator.DEFAULT.allocate(0, 0));
           } else {
               ProtonSaslContext sasl = (ProtonSaslContext) context;
               sasl.done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_SYS);
               context.saslFailure(new SaslAuthenticationException(SaslCode.SYS));
           }
       }

        @Override
        public void handleSaslChallenge(SaslClientContext context, ProtonBuffer challenge) {
            ProtonSaslContext sasl = (ProtonSaslContext) context;
            sasl.done(org.apache.qpid.proton4j.engine.sasl.SaslOutcome.SASL_SYS);
            context.saslFailure(new SaslAuthenticationException(SaslCode.SYS));
        }

        @Override
        public void handleSaslOutcome(SaslClientContext context, org.apache.qpid.proton4j.engine.sasl.SaslOutcome outcome, ProtonBuffer additional) {
            ProtonSaslContext sasl = (ProtonSaslContext) context;
            sasl.done(outcome);
            // TODO - Finish default handler.
        }
    }
}
