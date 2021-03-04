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
package org.apache.qpid.protonj2.engine.impl.sasl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.EngineState;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.impl.ProtonEngine;
import org.apache.qpid.protonj2.engine.impl.ProtonEngineNoOpSaslDriver;

/**
 * Base class used for common portions of the SASL processing pipeline.
 */
public final class ProtonSaslHandler implements EngineHandler {

    private EngineHandlerContext context;
    private ProtonEngineSaslDriver driver;
    private ProtonEngine engine;
    private ProtonSaslContext saslContext;

    public boolean isDone() {
        return saslContext != null ? saslContext.isDone() : false;
    }

    @Override
    public void handlerAdded(EngineHandlerContext context) {
        this.engine = (ProtonEngine) context.engine();
        this.driver = new ProtonEngineSaslDriver(engine, this);
        this.context = context;

        engine.registerSaslDriver(driver);
    }

    @Override
    public void handlerRemoved(EngineHandlerContext context) {
        this.driver = null;
        this.saslContext = null;
        this.engine = null;
        this.context = null;

        // If the engine wasn't started then it is okay to remove this handler otherwise
        // we would only be removed from the pipeline on completion of SASL negotiations
        // and the driver must remain to convey the outcome.
        if (context.engine().state() == EngineState.IDLE) {
            ((ProtonEngine) context.engine()).registerSaslDriver(ProtonEngineNoOpSaslDriver.INSTANCE);
        }
    }

    @Override
    public void engineStarting(EngineHandlerContext context) {
        driver.handleEngineStarting(engine);
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderEnvelope header) {
        if (isDone()) {
            context.fireRead(header);
        } else {
            // Default to server if application has not configured one way or the other.
            saslContext = driver.context();
            if (saslContext == null) {
                saslContext = driver.server();
            }

            header.invoke(saslContext.headerReadContext(), context);
        }
    }

    @Override
    public void handleRead(EngineHandlerContext context, SASLEnvelope frame) {
        if (isDone()) {
            throw new ProtocolViolationException("Unexpected SASL Frame: SASL processing has already completed");
        } else {
            frame.invoke(safeGetSaslContext().saslReadContext(), context);
        }
    }

    @Override
    public void handleRead(EngineHandlerContext context, IncomingAMQPEnvelope frame) {
        if (isDone()) {
            context.fireRead(frame);
        } else {
            throw new ProtocolViolationException("Unexpected AMQP Frame: SASL processing not yet completed");
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, HeaderEnvelope frame) {
        if (isDone()) {
            context.fireWrite(frame);
        } else {
            // Default to client if application has not configured one way or the other.
            saslContext = driver.context();
            if (saslContext == null) {
                saslContext = driver.client();
            }

            // Delegate write to the SASL Context in use to allow for state updates.
            frame.invoke(saslContext.headerWriteContext(), context);
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, OutgoingAMQPEnvelope frame) {
        if (isDone()) {
            context.fireWrite(frame);
        } else {
            throw new ProtocolViolationException("Unexpected AMQP Performative: SASL processing not yet completed");
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SASLEnvelope frame) {
        if (isDone()) {
            throw new ProtocolViolationException("Unexpected SASL Performative: SASL processing has yet completed");
        } else {
            // Delegate to the SASL Context to allow state tracking to be maintained.
            frame.invoke(safeGetSaslContext().saslWriteContext(), context);
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireWrite(buffer);
    }

    //----- Internal implementation API and helper methods

    ProtonEngine engine() {
        return engine;
    }

    EngineHandlerContext context() {
        return context;
    }

    private ProtonSaslContext safeGetSaslContext() {
        if (saslContext != null) {
            return saslContext;
        }

        throw new IllegalStateException("Cannot process incoming SASL performative, driver not yet initialized");
    }
}
