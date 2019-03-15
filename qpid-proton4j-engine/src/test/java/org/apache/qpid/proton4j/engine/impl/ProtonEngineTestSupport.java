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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.junit.After;

/**
 * Base class for Proton Engine and its components.
 */
public abstract class ProtonEngineTestSupport {

    protected ArrayList<ProtonBuffer> engineWrites = new ArrayList<>();

    protected final Decoder decoder = CodecFactory.getDefaultDecoder();
    protected final DecoderState decoderState = decoder.newDecoderState();

    protected final Encoder encoder = CodecFactory.getDefaultEncoder();
    protected final EncoderState encoderState = encoder.newEncoderState();

    protected Exception failure;

    @After
    public void tearDown() {
        engineWrites.clear();
        decoderState.reset();
        encoderState.reset();

        failure = null;
    }

    protected ProtonBuffer wrapInFrame(Object input, int channel) {
        final int FRAME_START_BYTE = 0;
        final int FRAME_DOFF_BYTE = 4;
        final int FRAME_DOFF_SIZE = 2;
        final int FRAME_TYPE_BYTE = 5;
        final int FRAME_CHANNEL_BYTE = 6;

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(512);

        buffer.writeLong(0); // Reserve header space

        try {
            encoder.writeObject(buffer, encoderState, input);
        } finally {
            encoderState.reset();
        }

        buffer.setInt(FRAME_START_BYTE, buffer.getReadableBytes());
        buffer.setByte(FRAME_DOFF_BYTE, FRAME_DOFF_SIZE);
        buffer.setByte(FRAME_TYPE_BYTE, 0);
        buffer.setShort(FRAME_CHANNEL_BYTE, channel);

        return buffer;
    }

    @SuppressWarnings({ "unchecked", "unused" })
    protected <E> E unwrapFrame(ProtonBuffer buffer, Class<E> typeClass) throws IOException {
        int frameSize = buffer.readInt();
        int dataOffset = (buffer.readByte() << 2) & 0x3FF;
        int type = buffer.readByte() & 0xFF;
        short channel = buffer.readShort();
        if (dataOffset != 8) {
            buffer.setReadIndex(buffer.getReadIndex() + dataOffset - 8);
        }

        final int frameBodySize = frameSize - dataOffset;

        ProtonBuffer payload = null;
        Object val = null;

        if (frameBodySize > 0) {
            try {
                val = decoder.readObject(buffer, decoderState);
            } finally {
                decoderState.reset();
            }
        } else {
            val = null;
        }

        return (E) val;
    }
}
