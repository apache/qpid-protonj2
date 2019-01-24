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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.engine.Frame;

/**
 * Utility class that writes AMQP and SASL frames into ProtonBuffer instances.
 */
class ProtonFrameWriter {

    private static final int AMQP_PERFORMATIVE_PAD = 256;
    private static final int FRAME_HEADER_SIZE = 8;

    private static final int FRAME_START_BYTE = 0;
    private static final int FRAME_DOFF_BYTE = 4;
    private static final int FRAME_DOFF_SIZE = 2;
    private static final int FRAME_TYPE_BYTE = 5;
    private static final int FRAME_CHANNEL_BYTE = 6;

    private final Encoder encoder;
    private final EncoderState encoderState;

    public ProtonFrameWriter(Encoder encoder, boolean sasl) {
        this.encoder = encoder;
        this.encoderState = encoder.newEncoderState();
    }

    public ProtonBuffer writeFrame(Frame<?> frame, int maxFrameSize, Runnable onPayloadTooLarge) {
        // TODO - We could simplify some handling logic if a Frame<?> always returned a payload value with the
        //        default being an read-only EMPTY_BUFFER constant that has zero size.

        int outputBufferSize = AMQP_PERFORMATIVE_PAD + (frame.getPayload() != null ? frame.getPayload().getReadableBytes() : 0);

        // TODO - Need to get access to the engine configuration for buffer allocator.
        ProtonBuffer output = ProtonByteBufferAllocator.DEFAULT.outputBuffer(AMQP_PERFORMATIVE_PAD + outputBufferSize);

        final int performativeSize = writePerformative(frame, maxFrameSize, output, onPayloadTooLarge);
        final int capacity = maxFrameSize > 0 ? maxFrameSize - performativeSize : Integer.MAX_VALUE;
        final int payloadSize = Math.min(frame.getPayload() == null ? 0 : frame.getPayload().getReadableBytes(), capacity);

        if (payloadSize > 0) {
            output.writeBytes(frame.getPayload(), payloadSize);
        }

        endFrame(output, frame.getType(), frame.getChannel());

        return null;
    }

    private int writePerformative(Frame<?> frame, int maxFrameSize, ProtonBuffer output, Runnable onPayloadTooLarge) {
        output.setWriteIndex(FRAME_HEADER_SIZE);

        if (frame.getBody() != null) {
            encoder.writeObject(output, encoderState, frame.getBody());
        }

        int performativeSize = output.getReadIndex();

        if (onPayloadTooLarge != null && maxFrameSize > 0 && frame.getPayload() != null && (frame.getPayload().getReadableBytes() + performativeSize) > maxFrameSize) {
            // Next iteration will re-encode the frame body again with updates from the <payload-to-large>
            // handler and then we can move onto the body portion.
            onPayloadTooLarge.run();
            performativeSize = writePerformative(frame, maxFrameSize, output, null);
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
