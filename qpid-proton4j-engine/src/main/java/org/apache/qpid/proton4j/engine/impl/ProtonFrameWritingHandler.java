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

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.engine.EngineHandlerAdapter;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.ProtocolFrame;

/**
 * Handler that encodes performatives into properly formed frames for IO
 */
public class ProtonFrameWritingHandler extends EngineHandlerAdapter {

    private static final InboudPerformativeHandler inboundSpy = new InboudPerformativeHandler();
    private static final OutboudPerformativeHandler outboundSpy = new OutboudPerformativeHandler();

    private Encoder saslEncoder = CodecFactory.getSaslEncoder();
    private Encoder encoder = CodecFactory.getEncoder();

    private EncoderState saslEncoderState;
    private EncoderState encoderState;

    private long localMaxFrameSize = -1;
    private long remoteMaxFrameSize = -1;
    private long outgoingMaxFrameSize;  // TODO

    private ProtonEngine engine;

    public Encoder getEndoer() {
        return encoder;
    }

    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    public Encoder getSaslEndoer() {
        return saslEncoder;
    }

    public void setSaslEncoder(Encoder encoder) {
        this.saslEncoder = encoder;
    }

    @Override
    public void handlerAdded(EngineHandlerContext context) throws Exception {
        saslEncoderState = getSaslEndoer().newEncoderState();
        encoderState = getEndoer().newEncoderState();
        engine = (ProtonEngine) context.getEngine();
    }

    @Override
    public void handlerRemoved(EngineHandlerContext context) throws Exception {
        saslEncoderState = null;
        encoderState = null;
        encoder = null;
        engine = null;
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        // Spy on incoming frames to gather remote configuration
        // TODO - Or Engine always has a Connection and we ask it ?
        frame.getBody().invoke(inboundSpy, frame.getPayload(), this);

        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        context.fireWrite(header.getBuffer());
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        try {
            performative.invoke(outboundSpy, payload, this);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        try {
            // TODO
        } finally {
            saslEncoderState.reset();
        }
    }

    //----- Internal Frame Writer implementation

    private void setLocalMaxFrameSize(long localMaxFrameSize) {
        this.localMaxFrameSize = localMaxFrameSize;
        computeMaxOutgoingFrameSize(localMaxFrameSize, remoteMaxFrameSize);
    }

    private void setRemoteMaxFrameSize(long remoteMaxFrameSize) {
        this.remoteMaxFrameSize = remoteMaxFrameSize;
        computeMaxOutgoingFrameSize(localMaxFrameSize, remoteMaxFrameSize);
    }

    private static void computeMaxOutgoingFrameSize(long localMaxFrameSize, long remoteMaxFrameSize) {
        // TODO - decide on defaults when neither sets a max etc
    }

    private static class OutboudPerformativeHandler implements Performative.PerformativeHandler<ProtonFrameWritingHandler> {

        @Override
        public void handleOpen(Open open, ProtonBuffer payload, ProtonFrameWritingHandler context) {
            if (open.getMaxFrameSize() != null) {
                context.setLocalMaxFrameSize(open.getMaxFrameSize().longValue());
            }
        }
    }

    private static class InboudPerformativeHandler implements Performative.PerformativeHandler<ProtonFrameWritingHandler> {

        @Override
        public void handleOpen(Open open, ProtonBuffer payload, ProtonFrameWritingHandler context) {
            if (open.getMaxFrameSize() != null) {
                context.setRemoteMaxFrameSize(open.getMaxFrameSize().longValue());
            }
        }
    }
}
