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

import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.EmptyFrame;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.ProtocolFramePool;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonExceptionSupport;

/**
 * Handler used to parse incoming frame data input into the engine
 */
public class ProtonFrameDecodingHandler implements EngineHandler, SaslPerformative.SaslPerformativeHandler<EngineHandlerContext> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonFrameDecodingHandler.class);

    public static final byte AMQP_FRAME_TYPE = (byte) 0;
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    public static final int FRAME_SIZE_BTYES = 4;

    private final ProtocolFramePool framePool = ProtocolFramePool.DEFAULT;

    private Decoder decoder;
    private DecoderState decoderState;
    private FrameParserStage stage = new HeaderParsingStage();
    private ProtonEngine engine;
    private ProtonEngineConfiguration configuration;

    // Parser stages used during the parsing process
    private final FrameSizeParsingStage frameSizeParser = new FrameSizeParsingStage();
    private final FrameBufferingStage frameBufferingStage = new FrameBufferingStage();
    private final FrameParserStage frameBodyParsingStage = new FrameBodyParsingStage();

    //----- Handler method implementations

    @Override
    public void handlerAdded(EngineHandlerContext context) throws Exception {
        engine = (ProtonEngine) context.getEngine();
        configuration = engine.configuration();
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        try {
            // Parses in-incoming data and emit one complete frame before returning, caller should
            // ensure that the input buffer is drained into the engine or stop if the engine
            // has changed to a non-writable state.
            stage.parse(context, buffer);
        } catch (IOException ex) {
            transitionToErrorStage(ex).fireError(context);
        } catch (Throwable throwable) {
            transitionToErrorStage(ProtonExceptionSupport.create(throwable)).fireError(context);
        }
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        frame.getBody().invoke(this, context);
        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        performative.invoke(this, context);
        context.fireWrite(performative);
    }

    //----- Sasl Performative Handler to check for change to non-SASL state

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, EngineHandlerContext context) {
        // When we have read or written a SASL Outcome the next value to be read
        // should be an AMQP Header to begin the next phase of the connection.
        this.stage = new HeaderParsingStage();
    }

    //---- Methods to transition between stages

    private FrameParserStage transitionToFrameSizeParsingStage() {
        return stage = frameSizeParser.reset(0);
    }

    private FrameParserStage transitionToFrameBufferingStage(int frameSize) {
        return stage = frameBufferingStage.reset(frameSize);
    }

    private FrameParserStage initializeFrameBodyParsingStage(int frameSize) {
        return stage = frameBodyParsingStage.reset(frameSize);
    }

    private ParsingErrorStage transitionToErrorStage(IOException error) {
        if (!(stage instanceof ParsingErrorStage)) {
            LOG.trace("Frame decoder encounted error: ", error);
            stage = new ParsingErrorStage(error);
        }

        return (ParsingErrorStage) stage;
    }

    //----- Frame Parsing Stage definition

    private interface FrameParserStage {

        /**
         * Parse the incoming data and provide events to the parent Transport
         * based on the contents of that data.
         *
         * @param context
         *      The TransportHandlerContext that applies to the current event
         * @param input
         *      The ProtonBuffer containing new data to be parsed.
         *
         * @throws IOException if an error occurs while parsing incoming data.
         */
        void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException;

        /**
         * Reset the stage to its defaults for a new cycle of parsing.
         *
         * @param frameSize
         *      The frameSize to use for this part of the parsing operation
         *
         * @return a reference to this parsing stage for chaining.
         */
        FrameParserStage reset(int frameSize);

    }

    //---- Built in FrameParserStages

    private class HeaderParsingStage implements FrameParserStage {

        private final byte[] headerBytes = new byte[AMQPHeader.HEADER_SIZE_BYTES];

        private int headerByte;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer incoming) throws IOException {
            while (incoming.isReadable() && headerByte <= AMQPHeader.HEADER_SIZE_BYTES) {
                headerBytes[headerByte++] = incoming.readByte();
            }

            // Construct a new Header from the read bytes which will validate the contents
            AMQPHeader header = new AMQPHeader(headerBytes);

            // Transition to parsing the frames if any pipelined into this buffer.
            transitionToFrameSizeParsingStage();

            // This probably isn't right as this fires to next not current.
            if (header.isSaslHeader()) {
                decoder = CodecFactory.getSaslDecoder();
                decoderState = decoder.newDecoderState();
                context.fireRead(HeaderFrame.SASL_HEADER_FRAME);
            } else {
                decoder = CodecFactory.getDecoder();
                decoderState = decoder.newDecoderState();
                context.fireRead(HeaderFrame.AMQP_HEADER_FRAME);
            }
        }

        @Override
        public HeaderParsingStage reset(int frameSize) {
            headerByte = 0;
            return this;
        }
    }

    private class FrameSizeParsingStage implements FrameParserStage {

        private int frameSize;
        private int multiplier = FRAME_SIZE_BTYES;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
            while (input.isReadable()) {
                frameSize |= ((input.readByte() & 0xFF) << --multiplier * Byte.SIZE);
                if (multiplier == 0) {
                    break;
                }
            }

            if (multiplier == 0) {
                validateFrameSize();

                // Normalize the frame size to the reminder portion
                frameSize -= FRAME_SIZE_BTYES;

                if (input.getReadableBytes() < frameSize) {
                    transitionToFrameBufferingStage(frameSize);
                } else {
                    initializeFrameBodyParsingStage(frameSize);
                }

                stage.parse(context, input);
            }
        }

        private void validateFrameSize() throws IOException {
            if (frameSize < 8) {
               throw new ProtonException(String.format(
                    "specified frame size %d smaller than minimum frame header size 8", frameSize));
            }

            if (frameSize > configuration.getInboundMaxFrameSize()) {
                throw new ProtonException(String.format(
                    "specified frame size %d larger than maximum frame size %d", frameSize, configuration.getInboundMaxFrameSize()));
            }
        }

        @Override
        public FrameSizeParsingStage reset(int frameSize) {
            multiplier = FRAME_SIZE_BTYES;
            this.frameSize = frameSize;
            return this;
        }
    }

    private class FrameBufferingStage implements FrameParserStage {

        private ProtonBuffer buffer;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
            if (input.getReadableBytes() < buffer.getWritableBytes()) {
                buffer.writeBytes(input);
            } else {
                buffer.writeBytes(input, buffer.getWritableBytes());

                // Now we can consume the buffer frame body.
                initializeFrameBodyParsingStage(buffer.getReadableBytes());
                try {
                    stage.parse(context, buffer);
                } finally {
                    buffer = null;
                }
            }
        }

        @Override
        public FrameBufferingStage reset(int frameSize) {
            buffer = ProtonByteBufferAllocator.DEFAULT.allocate(frameSize, frameSize);
            return this;
        }
    }

    private class FrameBodyParsingStage implements FrameParserStage {

        private int frameSize;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
            int dataOffset = (input.readByte() << 2) & 0x3FF;

            if (dataOffset < 8) {
                throw new ProtonException(String.format(
                    "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8));
            }
            if (dataOffset > frameSize) {
                throw new ProtonException(String.format(
                    "specified frame data offset %d larger than the frame size %d", dataOffset, frameSize));
            }

            int type = input.readByte() & 0xFF;
            short channel = input.readShort();

            // note that this skips over the extended header if it's present
            if (dataOffset != 8) {
                input.setReadIndex(input.getReadIndex() + dataOffset - 8);
            }

            final int frameBodySize = frameSize - (dataOffset - FRAME_SIZE_BTYES);

            ProtonBuffer payload = null;
            Object val = null;

            if (frameBodySize > 0) {
                val = decoder.readObject(input, decoderState);

                // Slice to the known Frame body size and use that as the buffer for any payload once
                // the actual Performative has been decoded.  The implies that the data comprising the
                // performative will be held as long as the payload buffer is kept.
                if (input.isReadable()) {
                    payload = input.slice(input.getReadIndex(), frameBodySize);
                    input.skipBytes(payload.getReadableBytes());
                }
            } else {
                val = new EmptyFrame();
            }

            if (type == AMQP_FRAME_TYPE) {
                Performative performative = (Performative) val;
                ProtocolFrame frame = framePool.take(performative, channel, frameSize, payload);
                transitionToFrameSizeParsingStage();
                context.fireRead(frame);
            } else if (type == SASL_FRAME_TYPE) {
                SaslPerformative performative = (SaslPerformative) val;
                SaslFrame saslFrame = new SaslFrame(performative, frameSize, payload);
                transitionToFrameSizeParsingStage();
                context.fireRead(saslFrame);
            } else {
                throw new ProtonException(String.format("unknown frame type: %d", type));
            }
        }

        @Override
        public FrameBodyParsingStage reset(int frameSize) {
            this.frameSize = frameSize;
            return this;
        }
    }

    /*
     * If parsing fails the parser enters the failed state and remains there always throwing the given exception
     * if additional parsing is requested.
     */
    private class ParsingErrorStage implements FrameParserStage {

        private final IOException parsingError;

        public ParsingErrorStage(IOException parsingError) {
            this.parsingError = parsingError;
        }

        public void fireError(EngineHandlerContext context) {
            context.fireDecodingError(parsingError);
        }

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
            throw parsingError;
        }

        @Override
        public ParsingErrorStage reset(int frameSize) {
            return this;
        }
    }
}
