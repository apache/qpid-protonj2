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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonExceptionSupport;

/**
 * Transport Handler that forwards the incoming Performatives to the associated Connection
 * as well as any error encountered during the Transport processing.
 */
public class ProtonPerformativeHandler implements EngineHandler, AMQPHeader.HeaderHandler<EngineHandlerContext>, Performative.PerformativeHandler<EngineHandlerContext> {

    private ProtonEngine engine;
    private ProtonConnection connection;
    private ProtonEngineConfiguration configuration;

    private boolean headerReceived;
    private boolean headerSent;

    //----- Handle transport events

    @Override
    public void handlerAdded(EngineHandlerContext context) throws Exception {
        engine = (ProtonEngine) context.getEngine();
        configuration = engine.configuration();
        configuration = engine.configuration();
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {

    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        // TODO - If closed or failed we should reject incoming frames

        try {
            frame.invoke(this, context);
        } finally {
            frame.release();
        }
    }

    @Override
    public void transportEncodingError(EngineHandlerContext context, Throwable e) {
        // TODO signal error to the connection, try and differentiate between fatal and non-fatal conditions ?
    }

    @Override
    public void transportDecodingError(EngineHandlerContext context, Throwable e) {
        // TODO signal error to the connection, try and differentiate between fatal and non-fatal conditions ?
    }

    @Override
    public void transportFailed(EngineHandlerContext context, Throwable e) {
        engine.engineFailed(ProtonExceptionSupport.create(e));
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
        if (headerReceived) {
            // TODO signal failure to the engine and adjust state
        }

        headerReceived = true;
        if (!headerSent) {
            context.fireWrite(AMQPHeader.getAMQPHeader());
        }

        // Recompute max frame size now based on engine max frame size in case sasl was enabled.
        configuration.recomputeEffectiveFrameSizeLimits();

        // Let the Connection know we have a header so it can emit any pending work.
        header.invoke(connection, engine);
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, EngineHandlerContext context) {
        if (!headerSent) {
            headerReceived = true;
            headerSent = true;
            context.fireWrite(AMQPHeader.getAMQPHeader());
        }

        // TODO signal failure as we don't handle SASL at this level.
    }

    @Override
    public void handleOpen(Open open, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        if (channel != 0) {
            transportFailed(context, new ProtocolViolationException("Open not sent on channel zero"));
        }

        // TODO - This isn't storing the truth of what remote said, so configuration reports
        //        our trimmed view when asked externally.
        configuration.setRemoteMaxFrameSize(
            (int) Math.min(open.getMaxFrameSize().longValue(), Integer.MAX_VALUE));

        // Recompute max frame size now based on what remote told us.
        configuration.recomputeEffectiveFrameSizeLimits();

        // TODO - Define the error from these methods, IOException other ?
        connection.handleOpen(open, payload, channel, engine);
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
