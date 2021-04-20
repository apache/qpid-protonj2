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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.AMQPHeader.HeaderHandler;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.End;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Open;
import org.apache.qpid.protonj2.types.transport.Performative.PerformativeHandler;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Transport Handler that forwards the incoming Performatives to the associated Connection
 * as well as any error encountered during the Transport processing.
 */
public class ProtonPerformativeHandler implements EngineHandler, HeaderHandler<EngineHandlerContext>, PerformativeHandler<EngineHandlerContext> {

    private ProtonEngine engine;
    private ProtonConnection connection;
    private ProtonEngineConfiguration configuration;

    //----- Handle transport events

    @Override
    public void handlerAdded(EngineHandlerContext context) {
        engine = (ProtonEngine) context.engine();
        connection = engine.connection();
        configuration = engine.configuration();

        ((ProtonEngineHandlerContext) context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderEnvelope header) {
        header.invoke(this, context);
    }

    @Override
    public void handleRead(EngineHandlerContext context, IncomingAMQPEnvelope envelope) {
        try {
            envelope.invoke(this, context);
        } finally {
            envelope.release();
        }
    }

    @Override
    public void engineFailed(EngineHandlerContext context, EngineFailedException failure) {
        // In case external source injects failure we grab it and propagate after the
        // appropriate changes to our engine state.
        if (!engine.isFailed()) {
            engine.engineFailed(failure.getCause());
        }
    }

    //----- Deal with the incoming AMQP performatives

    // Here we can spy on incoming performatives and update engine state relative to
    // those prior to sending along notifications to other handlers or to the connection.
    //
    // We currently can't spy on outbound performatives but we could in future by splitting these
    // into inner classes for inbound and outbound and handle the write to invoke the outbound
    // handlers.

    @Override
    public void handleAMQPHeader(AMQPHeader header, EngineHandlerContext context) {
        // Recompute max frame size now based on engine max frame size in case sasl was enabled.
        configuration.recomputeEffectiveFrameSizeLimits();

        // Let the Connection know we have a header so it can emit any pending work.
        header.invoke(connection, engine);
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
        // Respond with Raw AMQP Header and then fail the engine.
        context.fireWrite(HeaderEnvelope.AMQP_HEADER_ENVELOPE);

        throw new ProtocolViolationException("Received SASL Header but no SASL support configured");
    }

    @Override
    public void handleOpen(Open open, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        if (channel != 0) {
            throw new ProtocolViolationException("Open not sent on channel zero");
        }

        connection.handleOpen(open, payload, channel, engine);

        // Recompute max frame size now based on what remote told us.
        configuration.recomputeEffectiveFrameSizeLimits();
    }

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleBegin(begin, payload, channel, engine);
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleAttach(attach, payload, channel, engine);
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleFlow(flow, payload, channel, engine);
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleTransfer(transfer, payload, channel, engine);
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleDisposition(disposition, payload, channel, engine);
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleDetach(detach, payload, channel, engine);
    }

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleEnd(end, payload, channel, engine);
    }

    @Override
    public void handleClose(Close close, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        connection.handleClose(close, payload, channel, engine);
    }
}
