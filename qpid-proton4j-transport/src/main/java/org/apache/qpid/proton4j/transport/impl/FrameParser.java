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
package org.apache.qpid.proton4j.transport.impl;

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
import org.apache.qpid.proton4j.transport.EmptyFrame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.ProtocolFramePool;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.exceptions.TransportException;

/**
 * Parse and return a single Frame on each call unless insufficient data exists in the provided buffer in
 * which case the parser will begin buffering data until a full frame has been received.
 */
public class FrameParser {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(FrameParser.class);

    private static final int AMQP_HEADER_BYTES = 8;

    private final ProtocolFramePool framePool = ProtocolFramePool.DEFAULT;
    private final TransportHandler handler;
    private final Decoder decoder;
    private final DecoderState decoderState;

    private FrameParserStage stage;
    private int frameSizeLimit;

    // Parser stages used during the parsing process
    private final FrameSizeParsingStage frameSizeParser = new FrameSizeParsingStage();
    private final FrameBufferingStage frameBufferingStage = new FrameBufferingStage();
    private final FrameParserStage frameBodyParsingStage;

    private FrameParser(TransportHandler handler, Decoder decoder, int frameSizeLimit, AMQPHeader expectedHeader, boolean sasl) {
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
     * @return a new SASL frame parser that is linked to the provided {@link TransportHandler}.
     */
    public static FrameParser createSaslParser(TransportHandler handler, Decoder decoder, int frameSizeLimit) {
        return new FrameParser(handler, decoder, frameSizeLimit, AMQPHeader.getSASLHeader(), true);
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
     * @return a new AMQP frame parser that is linked to the provided {@link TransportHandler}.
     */
    public static FrameParser createNonSaslParser(TransportHandler handler, Decoder decoder, int frameSizeLimit) {
        return new FrameParser(handler, decoder, frameSizeLimit, AMQPHeader.getRawAMQPHeader(), false);
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
    public void parse(TransportHandlerContext context, ProtonBuffer input) throws IOException {
        try {
            stage.parse(context, this, input);
        } catch (IOException ex) {
            transitionToErrorStage(ex);
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

    private FrameParserStage transitionToErrorStage(IOException error) throws IOException {
        stage = new ParsingErrorStage(error);
        throw error;
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
        void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer input) throws IOException;

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
        public void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer incoming) throws IOException {
            while (incoming.isReadable() && headerByte <= AMQP_HEADER_BYTES) {
                byte c = incoming.readByte();

                if (c != header.getByteAt(headerByte)) {
                    transitionToErrorStage(new TransportException(String.format(
                        "AMQP header mismatch value %x, expecting %x. In header byte: %d", c, header.getByteAt(headerByte), headerByte)));
                }

                headerByte++;
            }

            try {
                // Transition to parsing the frames if any pipelined into this buffer.
                parser.transitionToFrameSizeParsingStage();

                // This probably isn't right as this fires to next not current.
                handler.handleRead(context, headerFrame);
            } catch (Throwable e) {
                // TODO - Error mechanics here are not quite clear what does this throw?
                parser.transitionToErrorStage(new IOException(e));
            }
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
        public void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer input) throws IOException {
            while (input.isReadable()) {
                frameSize += ((input.readByte() & 0xFF) << --multiplier * Byte.SIZE);
                if (multiplier == 0) {
                    break;
                }
            }

            if (multiplier == 0) {
                validateFrameSize(parser);

                // Normalize the frame size to the reminder portion
                frameSize -= FRAME_SIZE_BTYES;

                if (input.getReadableBytes() < frameSize) {
                    transitionToFrameBufferingStage(frameSize);
                } else {
                    initializeFrameBodyParsingStage(frameSize);
                }

                stage.parse(context, parser, input);
            }
        }

        private void validateFrameSize(FrameParser parser) throws IOException {
            if (frameSize < 8) {
               transitionToErrorStage(new TransportException(String.format(
                    "specified frame size %d smaller than minimum frame header size 8", frameSize)));
            }

            if (frameSize > parser.getMaxFrameSize()) {
                transitionToErrorStage(new TransportException(String.format(
                    "specified frame size %d larger than maximum frame size %d", frameSize, frameSizeLimit)));
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
        public void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer input) throws IOException {
            if (input.getReadableBytes() < buffer.getWritableBytes()) {
                buffer.writeBytes(input);
            } else {
                buffer.writeBytes(input, buffer.getWritableBytes());

                // Now we can consume the buffer frame body.
                stage = initializeFrameBodyParsingStage(buffer.getReadableBytes());
                try {
                    stage.parse(context, parser, buffer);
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
        public void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer input) throws IOException {
            int dataOffset = (input.readByte() << 2) & 0x3FF;

            if (dataOffset < 8) {
                transitionToErrorStage(new TransportException(String.format(
                    "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8)));
            }
            if (dataOffset > frameSize) {
                transitionToErrorStage(new TransportException(String.format(
                    "specified frame data offset %d larger than the frame size %d", dataOffset, frameSize)));
            }

            int type = input.readByte() & 0xFF;
            short channel = input.readShort();

            if (type != 0) {
                transitionToErrorStage(new TransportException(String.format("unknown frame type: %d", type)));
            }

            // note that this skips over the extended header if it's present
            if (dataOffset != 8) {
                input.setReadIndex(input.getReadIndex() + dataOffset - 8);
            }

            final int frameBodySize = frameSize - dataOffset;

            try {
                ProtonBuffer payload = null;
                Object val = null;

                if (frameBodySize > 0) {
                    val = decoder.readObject(input, decoderState);

                    if (input.isReadable()) {
                        int payloadSize = input.getReadableBytes();
                        payload = ProtonByteBufferAllocator.DEFAULT.allocate(payloadSize, payloadSize);
                        input.readBytes(payload);
                    } else {
                        payload = null;
                    }
                } else {
                    val = new EmptyFrame();
                }

                if (val instanceof Performative) {
                    Performative frameBody = (Performative) val;
                    LOG.trace("IN: {} CH[{}] : {} [{}]", channel, frameBody, payload);
                    ProtocolFrame frame = framePool.take(frameBody, channel, payload);
                    // This probably isn't right as this fires to next not current.
                    handler.handleRead(context, frame);
                    // TODO - Error specification of this method is unclear right now
                } else {
                    throw new TransportException("Frameparser encountered a "
                            + (val == null? "null" : val.getClass())
                            + " which is not a " + Performative.class);
                }

                stage = transitionToFrameSizeParsingStage();
            } catch (IOException ex) {
                stage = transitionToErrorStage(ex);
            }
        }

        @Override
        public AMQPFrameBodyParsingStage reset(int frameSize) {
            this.frameSize = frameSize;
            return this;
        }
    }

    private class SaslFrameBodyParsingStage implements FrameParserStage {

        public static final byte SASL_FRAME_TYPE = (byte) 1;

        private int frameSize;

        @Override
        public void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer input) throws IOException {
            int dataOffset = (input.readByte() << 2) & 0x3FF;

            if (dataOffset < 8) {
                transitionToErrorStage(new TransportException(String.format(
                    "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8)));
            } else if (dataOffset > frameSize) {
                transitionToErrorStage(new TransportException(String.format(
                    "specified frame data offset %d larger than the frame size %d", dataOffset, frameSize)));
            }

            int type = input.readByte() & 0xFF;
            // SASL frame has no type-specific content in the frame
            // header, so we skip next two bytes
            input.readByte();
            input.readByte();

            if (type != SASL_FRAME_TYPE) {
                // The SASL handling code should not pass use data beyond the last SASL frame so we throw here
                // to indicate that the pipeline of frames is incorrect.
                transitionToErrorStage(new TransportException(String.format("unknown frame type: %d", type)));
            }

            if (dataOffset != 8) {
                input.setReadIndex(input.getReadIndex() + dataOffset - 8);
            }

            try {
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
                    SaslFrame saslFrame = new SaslFrame(performative, payload);
                    // This probably isn't right as this fires to next not current.
                    transitionToFrameSizeParsingStage();
                    handler.handleRead(context, saslFrame);
                    // TODO - Error specification of above fire call is unclear
                } else {
                    transitionToErrorStage(new TransportException(String.format(
                        "Unexpected frame type encountered." + " Found a %s which does not implement %s",
                        val == null ? "null" : val.getClass(), SaslPerformative.class)));
                }
            } catch (IOException ex) {
                // TODO - handle above or allow dup handling ?
                transitionToErrorStage(new TransportException(ex));
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

        @Override
        public void parse(TransportHandlerContext context, FrameParser parser, ProtonBuffer input) throws IOException {
            throw parsingError;
        }

        @Override
        public ParsingErrorStage reset(int frameSize) {
            return this;
        }
    }
}
