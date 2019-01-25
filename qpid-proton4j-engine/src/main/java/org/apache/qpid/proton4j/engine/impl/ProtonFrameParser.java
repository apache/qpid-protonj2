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

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
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

/**
 * Parse and return a single Frame on each call unless insufficient data exists in the provided buffer in
 * which case the parser will begin buffering data until a full frame has been received.
 */
public class ProtonFrameParser {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonFrameParser.class);

    private static final int AMQP_HEADER_BYTES = 8;

    public static final byte AMQP_FRAME_TYPE = (byte) 0;
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    private final ProtocolFramePool framePool = ProtocolFramePool.DEFAULT;
    private final EngineHandler handler;
    private final Decoder decoder;
    private final DecoderState decoderState;

    private FrameParserStage stage;
    private int frameSizeLimit;

    // Parser stages used during the parsing process
    private final FrameSizeParsingStage frameSizeParser = new FrameSizeParsingStage();
    private final FrameBufferingStage frameBufferingStage = new FrameBufferingStage();
    private final FrameParserStage frameBodyParsingStage;

    private ProtonFrameParser(EngineHandler handler, Decoder decoder, int frameSizeLimit, AMQPHeader expectedHeader, boolean sasl) {
        this.handler = handler;
        this.decoder = decoder;
        this.decoderState = decoder.newDecoderState();
        this.frameSizeLimit = frameSizeLimit;
        this.stage = new HeaderParsingStage(expectedHeader);

        if (sasl) {
            frameBodyParsingStage = new SaslFrameBodyParsingStage();
        } else {
            frameBodyParsingStage = new AMQPFrameBodyParsingStage();
        }
    }

    //----- Factory methods for SASL or non-SASL parser

    /**
     * Create a SASL based Frame parser that will accept only SASL frames and reject frames of any other type.
     *
     * @param handler
     *      The transport handler that will be signaled when a frame is parsed.
     * @param decoder
     *      The Decoder instance that will be used to decode SASL performatives.
     * @param frameSizeLimit
     *      The maximum allow frame size limit before the parser should throw an error.
     *
     * @return a new SASL frame parser that is linked to the provided {@link EngineHandler}.
     */
    public static ProtonFrameParser createSaslParser(EngineHandler handler, Decoder decoder, int frameSizeLimit) {
        return new ProtonFrameParser(handler, decoder, frameSizeLimit, AMQPHeader.getSASLHeader(), true);
    }

    /**
     * Create a AMQP based Frame parser that will accept only AMQP protocol frames and reject frames of any other type.
     *
     * @param handler
     *      The transport handler that will be signaled when a frame is parsed.
     * @param decoder
     *      The Decoder instance that will be used to decode AMQP performatives.
     * @param frameSizeLimit
     *      The maximum allow frame size limit before the parser should throw an error.
     *
     * @return a new AMQP frame parser that is linked to the provided {@link EngineHandler}.
     */
    public static ProtonFrameParser createNonSaslParser(EngineHandler handler, Decoder decoder, int frameSizeLimit) {
        return new ProtonFrameParser(handler, decoder, frameSizeLimit, AMQPHeader.getRawAMQPHeader(), false);
    }

    //----- Parser API

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
    public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
        try {
            stage.parse(context, input);
        } catch (IOException ex) {
            transitionToErrorStage(ex).fireError();
        }
    }

    public int getMaxFrameSize() {
        return frameSizeLimit;
    }

    public void setMaxFrameSize(int frameSizeLimit) {
        this.frameSizeLimit = frameSizeLimit;
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

    private ParsingErrorStage transitionToErrorStage(IOException error) throws IOException {
        if (!(stage instanceof ParsingErrorStage)) {
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
         * @param parser
         *      The parser that initiated this parse call.
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

        private final AMQPHeader header;
        private final HeaderFrame headerFrame;

        private int headerByte;

        public HeaderParsingStage(AMQPHeader header) {
            this.header = header;
            this.headerFrame = new HeaderFrame(header);
        }

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer incoming) throws IOException {
            while (incoming.isReadable() && headerByte <= AMQP_HEADER_BYTES) {
                byte c = incoming.readByte();

                if (c != header.getByteAt(headerByte)) {
                    throw new ProtonException(String.format(
                        "AMQP header mismatch value %x, expecting %x. In header byte: %d", c, header.getByteAt(headerByte), headerByte));
                }

                headerByte++;
            }

            // Transition to parsing the frames if any pipelined into this buffer.
            transitionToFrameSizeParsingStage();

            // This probably isn't right as this fires to next not current.
            handler.handleRead(context, headerFrame);
        }

        @Override
        public HeaderParsingStage reset(int frameSize) {
            headerByte = 0;
            return this;
        }
    }

    private class FrameSizeParsingStage implements FrameParserStage {

        private static final int FRAME_SIZE_BTYES = 4;

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

            if (frameSize > getMaxFrameSize()) {
                throw new ProtonException(String.format(
                    "specified frame size %d larger than maximum frame size %d", frameSize, frameSizeLimit));
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

    private class AMQPFrameBodyParsingStage implements FrameParserStage {

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

            if (type != AMQP_FRAME_TYPE) {
                throw new ProtonException(String.format("unknown frame type: %d", type));
            }

            // note that this skips over the extended header if it's present
            if (dataOffset != 8) {
                input.setReadIndex(input.getReadIndex() + dataOffset - 8);
            }

            final int frameBodySize = frameSize - dataOffset;

            ProtonBuffer payload = null;
            Object val = null;

            if (frameBodySize > 0) {
                // Slice to the known Frame body size and use that as the buffer for any payload once
                // the actual Performative has been decoded.  The implies that the data comprising the
                // performative will be held as long as the payload buffer is kept.
                input = input.slice(input.getReadIndex(), frameBodySize);

                val = decoder.readObject(input, decoderState);

                if (input.isReadable()) {
                    payload = input;
                }
            } else {
                val = new EmptyFrame();
            }

            if (val instanceof Performative) {
                Performative performative = (Performative) val;
                LOG.trace("IN: CH[{}] : {} [{}]", channel, performative, payload);
                ProtocolFrame frame = framePool.take(performative, channel, payload);
                // This probably isn't right as this fires to next not current.
                transitionToFrameSizeParsingStage();
                handler.handleRead(context, frame);
                // TODO - Error specification of this method is unclear right now
            } else {
                throw new ProtonException("Frameparser encountered a "
                        + (val == null? "null" : val.getClass())
                        + " which is not a " + Performative.class);
            }
        }

        @Override
        public AMQPFrameBodyParsingStage reset(int frameSize) {
            this.frameSize = frameSize;
            return this;
        }
    }

    private class SaslFrameBodyParsingStage implements FrameParserStage {

        private int frameSize;

        @Override
        public void parse(EngineHandlerContext context,ProtonBuffer input) throws IOException {
            int dataOffset = (input.readByte() << 2) & 0x3FF;

            if (dataOffset < 8) {
                throw new ProtonException(String.format(
                    "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8));
            } else if (dataOffset > frameSize) {
                throw new ProtonException(String.format(
                    "specified frame data offset %d larger than the frame size %d", dataOffset, frameSize));
            }

            int type = input.readByte() & 0xFF;
            // SASL frame has no type-specific content in the frame
            // header, so we skip next two bytes
            input.readByte();
            input.readByte();

            if (type != SASL_FRAME_TYPE) {
                // The SASL handling code should not pass use data beyond the last SASL frame so we throw here
                // to indicate that the pipeline of frames is incorrect.
                throw new ProtonException(String.format("unknown frame type: %d", type));
            }

            if (dataOffset != 8) {
                input.setReadIndex(input.getReadIndex() + dataOffset - 8);
            }

            Object val = decoder.readObject(input, decoderState);

            final ProtonBuffer payload;

            if (input.isReadable()) {
                int payloadSize = input.getReadableBytes();
                payload = ProtonByteBufferAllocator.DEFAULT.allocate(payloadSize, payloadSize);
                input.readBytes(payload);
            } else {
                payload = null;
            }

            if (val instanceof SaslPerformative) {
                SaslPerformative performative = (SaslPerformative) val;
                LOG.trace("IN: {} [{}]", performative, payload);
                SaslFrame saslFrame = new SaslFrame(performative, payload);
                // This probably isn't right as this fires to next not current.
                transitionToFrameSizeParsingStage();
                handler.handleRead(context, saslFrame);
                // TODO - Error specification of above fire call is unclear
            } else {
                throw new ProtonException(String.format(
                    "Unexpected frame type encountered." + " Found a %s which does not implement %s",
                    val == null ? "null" : val.getClass(), SaslPerformative.class));
            }
        }

        @Override
        public SaslFrameBodyParsingStage reset(int frameSize) {
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

        public void fireError() throws IOException {
            throw parsingError;
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
