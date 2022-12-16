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
package org.apache.qpid.protonj2.test.driver;

import org.apache.qpid.protonj2.test.driver.codec.Codec;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;

/**
 * Encodes AMQP performatives into frames for transmission
 */
public class FrameEncoder {

    public static final byte AMQP_FRAME_TYPE = (byte) 0;
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    private static final int AMQP_PERFORMATIVE_PAD = 512;
    private static final int FRAME_HEADER_SIZE = 8;

    private static final int FRAME_START_BYTE = 0;
    private static final int FRAME_DOFF_BYTE = 4;
    private static final int FRAME_DOFF_SIZE = 2;
    private static final int FRAME_TYPE_BYTE = 5;
    private static final int FRAME_CHANNEL_BYTE = 6;

    private final AMQPTestDriver driver;

    private final Codec codec = Codec.Factory.create();

    public FrameEncoder(AMQPTestDriver driver) {
        this.driver = driver;
    }

    public Buffer handleWrite(DescribedType performative, int channel, Buffer payload, Runnable payloadToLarge) {
        return writeFrame(performative, payload, AMQP_FRAME_TYPE, channel, driver.getOutboundMaxFrameSize(), payloadToLarge);
    }

    public Buffer handleWrite(DescribedType performative, int channel) {
        return writeFrame(performative, null, SASL_FRAME_TYPE, (short) 0, driver.getOutboundMaxFrameSize(), null);
    }

    private Buffer writeFrame(DescribedType performative, Buffer payload, byte frameType, int channel, int maxFrameSize, Runnable onPayloadTooLarge) {
        final int outputBufferSize = AMQP_PERFORMATIVE_PAD + (payload != null ? payload.readableBytes() : 0);
        final Buffer output = BufferAllocator.onHeapUnpooled().allocate(outputBufferSize);

        final int performativeSize = writePerformative(performative, payload, maxFrameSize, output, onPayloadTooLarge);
        final int capacity = maxFrameSize > 0 ? maxFrameSize - performativeSize : Integer.MAX_VALUE;
        final int payloadSize = Math.min(payload == null ? 0 : payload.readableBytes(), capacity);

        if (payloadSize > 0) {
            payload.copyInto(payload.readerOffset(), output, output.writerOffset(), payloadSize);
            payload.skipReadableBytes(payloadSize);
            output.skipWritableBytes(payloadSize);
        }

        endFrame(output, frameType, channel);

        return output;
    }

    private int writePerformative(DescribedType performative, Buffer payload, int maxFrameSize, Buffer output, Runnable onPayloadTooLarge) {
        output.writerOffset(FRAME_HEADER_SIZE);

        long encodedSize = 0;
        final int startIndex = output.writerOffset();

        if (performative != null) {
            try {
                codec.putDescribedType(performative);
                encodedSize = codec.encode(output);
            } finally {
                codec.clear();
            }
        }

        int performativeSize = output.writerOffset() - startIndex;

        if (performativeSize != encodedSize) {
            throw new IllegalStateException(String.format(
                "Unable to encode performative %s of %d bytes into provided proton buffer, only wrote %d bytes",
                performative, performativeSize, encodedSize));
        }

        if (onPayloadTooLarge != null && maxFrameSize > 0 && payload != null && (payload.readableBytes() + performativeSize) > maxFrameSize) {
            // Next iteration will re-encode the frame body again with updates from the <payload-to-large>
            // handler and then we can move onto the body portion.
            onPayloadTooLarge.run();
            performativeSize = writePerformative(performative, payload, maxFrameSize, output, null);
        }

        return performativeSize;
    }

    private static void endFrame(Buffer output, byte frameType, int channel) {
        output.setInt(FRAME_START_BYTE, output.readableBytes());
        output.setByte(FRAME_DOFF_BYTE, (byte) FRAME_DOFF_SIZE);
        output.setByte(FRAME_TYPE_BYTE, frameType);
        output.setShort(FRAME_CHANNEL_BYTE, (short) channel);
    }
}
