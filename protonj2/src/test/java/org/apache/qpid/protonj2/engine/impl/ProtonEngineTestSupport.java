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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for Proton Engine and its components.
 */
public abstract class ProtonEngineTestSupport {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonEngineTestSupport.class);

    protected ArrayList<ProtonBuffer> engineWrites = new ArrayList<>();

    protected final Decoder decoder = CodecFactory.getDefaultDecoder();
    protected final DecoderState decoderState = decoder.newDecoderState();

    protected final Encoder encoder = CodecFactory.getDefaultEncoder();
    protected final EncoderState encoderState = encoder.newEncoderState();

    protected Throwable failure;
    protected String testName;

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        testName = testInfo.getDisplayName();
        LOG.info("========== start " + testInfo.getDisplayName() + " ==========");
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) {
        engineWrites.clear();
        decoderState.reset();
        encoderState.reset();

        failure = null;

        LOG.info("========== tearDown " + testInfo.getDisplayName() + " ==========");
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

    protected static ProtonBuffer createContentBuffer(int length) {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return ProtonByteBufferAllocator.DEFAULT.wrap(payload).setIndex(0, length);
    }

    protected ProtonTestConnector createTestPeer(Engine engine) {
        ProtonTestConnector peer = new ProtonTestConnector(buffer -> {
            engine.accept(ProtonByteBufferAllocator.DEFAULT.wrap(buffer));
        });
        engine.outputConsumer(buffer -> {
            peer.accept(buffer.toByteBuffer());
        });

        return peer;
    }

    protected String getTestName() {
        return getClass().getSimpleName() + "." + testName;
    }

    protected byte[] createEncodedMessage(Section<Object> body) {
        Encoder encoder = CodecFactory.getEncoder();
        ProtonBuffer buffer = new ProtonByteBufferAllocator().allocate();
        encoder.writeObject(buffer, encoder.newEncoderState(), body);
        byte[] result = new byte[buffer.getReadableBytes()];
        buffer.readBytes(result);
        return result;
    }

    protected byte[] createEncodedMessage(Section<?>... body) {
        Encoder encoder = CodecFactory.getEncoder();
        ProtonBuffer buffer = new ProtonByteBufferAllocator().allocate();
        for (Section<?> section : body) {
            encoder.writeObject(buffer, encoder.newEncoderState(), section);
        }
        byte[] result = new byte[buffer.getReadableBytes()];
        buffer.readBytes(result);
        return result;
    }

    protected byte[] createEncodedMessage(Data... body) {
        Encoder encoder = CodecFactory.getEncoder();
        ProtonBuffer buffer = new ProtonByteBufferAllocator().allocate();
        for (Data data : body) {
            encoder.writeObject(buffer, encoder.newEncoderState(), data);
        }
        byte[] result = new byte[buffer.getReadableBytes()];
        buffer.readBytes(result);
        return result;
    }
}
