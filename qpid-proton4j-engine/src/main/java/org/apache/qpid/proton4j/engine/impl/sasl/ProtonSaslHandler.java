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

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.impl.ProtonEngineNoOpSaslDriver;

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
        this.engine = (ProtonEngine) context.getEngine();
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

        engine.registerSaslDriver(ProtonEngineNoOpSaslDriver.INSTANCE);
    }

    @Override
    public void engineStarting(EngineHandlerContext context) {
        driver.handleEngineStarting(engine);
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
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
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        if (isDone()) {
            throw new ProtocolViolationException("Unexpected SASL Frame: SASL processing has already completed");
        } else {
            frame.invoke(safeGetSaslContext().saslReadContext(), context);
        }
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        if (isDone()) {
            context.fireRead(frame);
        } else {
            throw new ProtocolViolationException("Unexpected AMQP Frame: SASL processing not yet completed");
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        if (isDone()) {
            context.fireWrite(header);
        } else {
            // Default to client if application has not configured one way or the other.
            saslContext = driver.context();
            if (saslContext == null) {
                saslContext = driver.client();
            }

            // Delegate write to the SASL Context in use to allow for state updates.
            header.invoke(saslContext.headerWriteContext(), context);
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        if (isDone()) {
            context.fireWrite(performative, channel, payload, payloadToLarge);
        } else {
            throw new ProtocolViolationException("Unexpected AMQP Performative: SASL processing not yet completed");
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        if (isDone()) {
            throw new ProtocolViolationException("Unexpected SASL Performative: SASL processing has yet completed");
        } else {
            // Delegate to the SASL Context to allow state tracking to be maintained.
            performative.invoke(safeGetSaslContext().saslWriteContext(), context);
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
