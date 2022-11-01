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
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.PerformativeEncoder;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.FrameEncodingException;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * Handler that encodes performatives into properly formed frames for IO
 */
public class ProtonFrameEncodingHandler implements EngineHandler {

    /**
     * Frame type indicator for AMQP protocol frames.
     */
    public static final byte AMQP_FRAME_TYPE = (byte) 0;

    /**
     * Frame type indicator for SASL protocol frames.
     */
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    private static final int AMQP_PERFORMATIVE_PAD = 256;
    private static final int FRAME_HEADER_SIZE = 8;
    private static final int FRAME_DOFF_SIZE = 2;

    private static final int FRAME_START_BYTE = 0;
    private static final int FRAME_DOFF_BYTE = 4;

    private static final int FRAME_HEADER_PREFIX = FRAME_DOFF_SIZE << 24 | AMQP_FRAME_TYPE << 15;

    private static final byte[] SASL_FRAME_HEADER = new byte[] { 0, 0, 0, 0, FRAME_DOFF_SIZE, SASL_FRAME_TYPE, 0, 0 };

    private static final ProtonBuffer EMPTY_BUFFER = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[0]);

    private final Encoder saslEncoder = CodecFactory.getSaslEncoder();
    private final EncoderState saslEncoderState = saslEncoder.newEncoderState();
    private final Encoder amqpEncoder = CodecFactory.getEncoder();

    private PerformativeEncoder encoder;
    private ProtonEngine engine;
    private ProtonEngineConfiguration configuration;

    @Override
    public void handlerAdded(EngineHandlerContext context) {
        engine = (ProtonEngine) context.engine();
        configuration = engine.configuration();

        ((ProtonEngineHandlerContext) context).interestMask(ProtonEngineHandlerContext.HANDLER_WRITES);
    }

    @Override
    public void engineStarting(EngineHandlerContext context) {
        encoder = new PerformativeEncoder(amqpEncoder);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, HeaderEnvelope envelope) {
        context.fireWrite(envelope.getBody().getBuffer(), null);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SASLEnvelope envelope) {
        ProtonBuffer output = configuration.getBufferAllocator().outputBuffer(AMQP_PERFORMATIVE_PAD, (int) configuration.getOutboundMaxFrameSize());

        output.setWriteIndex(FRAME_HEADER_SIZE);
        output.setBytes(FRAME_START_BYTE, SASL_FRAME_HEADER);

        try {
            saslEncoder.writeObject(output, saslEncoderState, envelope.getBody());
        } catch (EncodeException ex) {
            throw new FrameEncodingException(ex);
        } finally {
            saslEncoderState.reset();
        }

        context.fireWrite(output.setInt(FRAME_START_BYTE, output.getReadableBytes()), null);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, OutgoingAMQPEnvelope envelope) {
        final ProtonBuffer payload = envelope.getPayload() == null ? EMPTY_BUFFER : envelope.getPayload();
        final int maxFrameSize = (int) configuration.getOutboundMaxFrameSize();
        final int outputBufferSize = Math.min(maxFrameSize, AMQP_PERFORMATIVE_PAD + payload.getReadableBytes());
        final ProtonBuffer output = configuration.getBufferAllocator().outputBuffer(outputBufferSize, maxFrameSize);

        writePerformative(output, encoder, envelope.getChannel(), envelope.getBody());

        if (payload.getReadableBytes() > output.getMaxWritableBytes()) {
            envelope.handlePayloadToLarge();

            writePerformative(output, encoder, envelope.getChannel(), envelope.getBody());

            output.writeBytes(payload, output.getMaxWritableBytes());
        } else {
            output.writeBytes(payload);
        }

        // Now fill in the frame header with the specified information
        output.setInt(FRAME_START_BYTE, output.getReadableBytes());
        output.setInt(FRAME_DOFF_BYTE, FRAME_HEADER_PREFIX | envelope.getChannel());

        context.fireWrite(output, envelope::handleOutgoingFrameWriteComplete);
    }

    private static void writePerformative(ProtonBuffer target, PerformativeEncoder encoder, int channel, Performative performative) {
        target.setWriteIndex(FRAME_HEADER_SIZE);

        try {
            performative.invoke(encoder, target, channel, encoder.getEncoder());
        } catch (EncodeException ex) {
            throw new FrameEncodingException(ex);
        }
    }
}
