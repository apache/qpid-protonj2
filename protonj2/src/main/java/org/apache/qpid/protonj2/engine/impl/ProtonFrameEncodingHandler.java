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
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderFrame;
import org.apache.qpid.protonj2.engine.OutgoingProtocolFrame;
import org.apache.qpid.protonj2.engine.SaslFrame;
import org.apache.qpid.protonj2.engine.exceptions.FrameEncodingException;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * Handler that encodes performatives into properly formed frames for IO
 */
public class ProtonFrameEncodingHandler implements EngineHandler {

    public static final byte AMQP_FRAME_TYPE = (byte) 0;
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    private static final int AMQP_PERFORMATIVE_PAD = 256;
    private static final int FRAME_HEADER_SIZE = 8;
    private static final int FRAME_DOFF_SIZE = 2;

    private static final int FRAME_START_BYTE = 0;
    private static final int FRAME_DOFF_BYTE = 4;
    private static final int FRAME_TYPE_BYTE = 5;
    private static final int FRAME_CHANNEL_BYTE = 6;

    private static final byte[] SASL_FRAME_HEADER = new byte[] { 0, 0, 0, 0, FRAME_DOFF_SIZE, SASL_FRAME_TYPE, 0, 0 };

    private static final ProtonBuffer EMPTY_BUFFER = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[0]);

    private final Encoder saslEncoder = CodecFactory.getSaslEncoder();
    private final EncoderState saslEncoderState = saslEncoder.newEncoderState();
    private final Encoder amqpEncoder = CodecFactory.getEncoder();
    private final EncoderState amqpEncoderState = amqpEncoder.newEncoderState();

    private ProtonEngine engine;
    private ProtonEngineConfiguration configuration;

    @Override
    public void handlerAdded(EngineHandlerContext context) {
        engine = (ProtonEngine) context.engine();
        configuration = engine.configuration();
    }

    @Override
    public void handleWrite(EngineHandlerContext context, HeaderFrame frame) {
        context.fireWrite(frame.getBody().getBuffer());
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslFrame frame) {
        ProtonBuffer output = configuration.getBufferAllocator().outputBuffer(AMQP_PERFORMATIVE_PAD, configuration.getOutboundMaxFrameSize());

        output.setWriteIndex(FRAME_HEADER_SIZE);
        output.setBytes(FRAME_START_BYTE, SASL_FRAME_HEADER);

        try {
            saslEncoder.writeObject(output, saslEncoderState, frame.getBody());
        } catch (EncodeException ex) {
            throw new FrameEncodingException(ex);
        } finally {
            saslEncoderState.reset();
        }

        context.fireWrite(output.setInt(FRAME_START_BYTE, output.getReadableBytes()));
    }

    @Override
    public void handleWrite(EngineHandlerContext context, OutgoingProtocolFrame frame) {
        try {
            // TODO: Ensure no numeric overflows in size calculations

            final ProtonBuffer payload = frame.getPayload() == null ? EMPTY_BUFFER : frame.getPayload();
            final int maxFrameSize = configuration.getOutboundMaxFrameSize();
            final int outputBufferSize = Math.min(maxFrameSize, AMQP_PERFORMATIVE_PAD + payload.getReadableBytes());
            final ProtonBuffer output = configuration.getBufferAllocator().outputBuffer(outputBufferSize, maxFrameSize);

            writePerformative(output, amqpEncoder, amqpEncoderState, frame.getBody());

            if (payload.getReadableBytes() > output.getMaxWritableBytes()) {
                frame.handlePayloadToLarge();

                writePerformative(output, amqpEncoder, amqpEncoderState, frame.getBody());

                output.writeBytes(payload, output.getMaxWritableBytes());
            } else {
                output.writeBytes(payload);
            }

            // Now fill in the frame header with the specified information
            output.setInt(FRAME_START_BYTE, output.getReadableBytes());
            output.setByte(FRAME_DOFF_BYTE, FRAME_DOFF_SIZE);
            output.setByte(FRAME_TYPE_BYTE, AMQP_FRAME_TYPE);
            output.setShort(FRAME_CHANNEL_BYTE, (short) frame.getChannel());

            context.fireWrite(output);
        } finally {
            frame.release();
        }
    }

    private static void writePerformative(ProtonBuffer target, Encoder encoder, EncoderState state, Performative performative) {
        target.setWriteIndex(FRAME_HEADER_SIZE);

        try {
            encoder.writeObject(target, state, performative);
        } catch (EncodeException ex) {
            throw new FrameEncodingException(ex);
        } finally {
            state.reset();
        }
    }
}
