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
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;

/**
 * Handler that encodes performatives into properly formed frames for IO
 *
 * TODO - Maybe rename to frame encoding handler
 */
public class ProtonFrameEncodingHandler implements EngineHandler {

    public static final byte AMQP_FRAME_TYPE = (byte) 0;
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    private static final int AMQP_PERFORMATIVE_PAD = 256;
    private static final int FRAME_HEADER_SIZE = 8;

    private static final int FRAME_START_BYTE = 0;
    private static final int FRAME_DOFF_BYTE = 4;
    private static final int FRAME_DOFF_SIZE = 2;
    private static final int FRAME_TYPE_BYTE = 5;
    private static final int FRAME_CHANNEL_BYTE = 6;

    private final Encoder saslEncoder = CodecFactory.getSaslEncoder();
    private final EncoderState saslEncoderState = saslEncoder.newEncoderState();
    private final Encoder amqpEncoder = CodecFactory.getEncoder();
    private final EncoderState amqpEncoderState = amqpEncoder.newEncoderState();

    private ProtonEngine engine;
    private ProtonEngineConfiguration configuration;

    @Override
    public void handlerAdded(EngineHandlerContext context) throws Exception {
        engine = (ProtonEngine) context.getEngine();
        configuration = engine.configuration();
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        context.fireWrite(header.getBuffer());
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        context.fireWrite(writeFrame(amqpEncoder, amqpEncoderState, performative, payload, AMQP_FRAME_TYPE, channel, configuration.getOutboundMaxFrameSize(), payloadToLarge));
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        context.fireWrite(writeFrame(saslEncoder, saslEncoderState, performative, null, SASL_FRAME_TYPE, (short) 0, configuration.getOutboundMaxFrameSize(), null));
    }

    private ProtonBuffer writeFrame(Encoder encoder, EncoderState encoderState, Object performative, ProtonBuffer payload, byte frameType, short channel, int maxFrameSize, Runnable onPayloadTooLarge) {
        int outputBufferSize = AMQP_PERFORMATIVE_PAD + (payload != null ? payload.getReadableBytes() : 0);

        ProtonBuffer output = configuration.getBufferAllocator().outputBuffer(AMQP_PERFORMATIVE_PAD + outputBufferSize);

        final int performativeSize = writePerformative(encoder, encoderState, performative, payload, maxFrameSize, output, onPayloadTooLarge);
        final int capacity = maxFrameSize > 0 ? maxFrameSize - performativeSize : Integer.MAX_VALUE;
        final int payloadSize = Math.min(payload == null ? 0 : payload.getReadableBytes(), capacity);

        if (payloadSize > 0) {
            output.writeBytes(payload, payloadSize);
        }

        endFrame(output, frameType, channel);

        return output;
    }

    private int writePerformative(Encoder encoder, EncoderState encoderState, Object performative, ProtonBuffer payload, int maxFrameSize, ProtonBuffer output, Runnable onPayloadTooLarge) {
        output.setWriteIndex(FRAME_HEADER_SIZE);

        if (performative != null) {
            try {
                encoder.writeObject(output, encoderState, performative);
            } finally {
                encoderState.reset();
            }
        }

        int performativeSize = output.getReadIndex();

        if (onPayloadTooLarge != null && maxFrameSize > 0 && payload != null && (payload.getReadableBytes() + performativeSize) > maxFrameSize) {
            // Next iteration will re-encode the frame body again with updates from the <payload-to-large>
            // handler and then we can move onto the body portion.
            onPayloadTooLarge.run();
            performativeSize = writePerformative(encoder, encoderState, performative, payload, maxFrameSize, output, null);
        }

        return performativeSize;
    }

    private static void endFrame(ProtonBuffer output, byte frameType, int channel) {
        output.setInt(FRAME_START_BYTE, output.getReadableBytes());
        output.setByte(FRAME_DOFF_BYTE, FRAME_DOFF_SIZE);
        output.setByte(FRAME_TYPE_BYTE, frameType);
        output.setShort(FRAME_CHANNEL_BYTE, (short) channel);
    }
}
