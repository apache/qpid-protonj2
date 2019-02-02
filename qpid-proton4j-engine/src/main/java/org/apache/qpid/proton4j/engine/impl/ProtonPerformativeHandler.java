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
import org.apache.qpid.proton4j.engine.EngineHandlerAdapter;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;

/**
 * Transport Handler that forwards the incoming Performatives to the associated Connection
 * as well as any error encountered during the Transport processing.
 */
public class ProtonPerformativeHandler extends EngineHandlerAdapter implements Performative.PerformativeHandler<EngineHandlerContext> {

    private static final int MIN_MAX_AMQP_FRAME_SIZE = 512;

    private final ProtonConnection connection;
    private final ProtonEngine engine;
    private final ProtonEngineConfiguration configuration;

    private int maxFrameSizeLimit = MIN_MAX_AMQP_FRAME_SIZE;

    private boolean headerReceived;

    public ProtonPerformativeHandler(ProtonEngine engine, ProtonConnection connection) {
        this.connection = connection;
        this.engine = engine;
        this.configuration = engine.getConfiguration();
    }

    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSizeLimit = Math.max(MIN_MAX_AMQP_FRAME_SIZE, maxFrameSize);
    }

    public int getMaxFrameSize() {
        return maxFrameSizeLimit;
    }

    //----- Handle transport events

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        if (headerReceived) {
            // TODO signal failure to the engine and adjust state
        }

        headerReceived = true;
        // TODO Convey to the engine that we should check local Connection state and fire
        //      the open if locally opened already.
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        // TODO - If closed or failed we should reject incoming frames

        try {
            frame.getBody().invoke(this, frame.getPayload(), frame.getChannel(), context);
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
        // TODO signal error to the connection and move transport state to failed.
    }

    //----- Deal with the incoming AMQP performatives

    // Here we can spy on incoming performatives and update engine state relative to
    // those prior to sending along notifications to other handlers or to the connection.

    @Override
    public void handleOpen(Open open, ProtonBuffer payload, int channel, EngineHandlerContext context) {
        if (channel != 0) {
            transportFailed(context, new ProtocolViolationException("Open not sent on channel zero"));
        }

        if (open.getMaxFrameSize() != null) {
            configuration.setRemoteMaxFrameSize(open.getMaxFrameSize().intValue());
        }

        // Recompute max frame size now based on what remote told us.
        configuration.recomputeEffectiveFrameSizeLimits(engine);

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
