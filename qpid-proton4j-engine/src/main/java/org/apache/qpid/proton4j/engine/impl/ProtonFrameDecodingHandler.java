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
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.MalformedAMQPHeaderException;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonIOException;

/**
 * Handler used to parse incoming frame data input into the engine
 */
public class ProtonFrameDecodingHandler implements EngineHandler, SaslPerformative.SaslPerformativeHandler<EngineHandlerContext> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonFrameDecodingHandler.class);

    public static final byte AMQP_FRAME_TYPE = (byte) 0;
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    public static final int FRAME_SIZE_BYTES = 4;

    private final ProtocolFramePool framePool = ProtocolFramePool.DEFAULT;

    private Decoder decoder;
    private DecoderState decoderState;
    private FrameParserStage stage = new HeaderParsingStage();
    private ProtonEngine engine;
    private ProtonEngineConfiguration configuration;

    // Parser stages used during the parsing process
    private final FrameSizeParsingStage frameSizeParser = new FrameSizeParsingStage();
    private final FrameBufferingStage frameBufferingStage = new FrameBufferingStage();
    private final FrameBodyParsingStage frameBodyParsingStage = new FrameBodyParsingStage();

    //----- Handler method implementations

    @Override
    public void handlerAdded(EngineHandlerContext context) {
        engine = (ProtonEngine) context.engine();
        configuration = engine.configuration();
    }

    @Override
    public void engineFailed(EngineHandlerContext context, EngineFailedException failure) {
        transitionToErrorStage(failure);
        context.fireFailed(failure);
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        try {
            // Parses in-incoming data and emit events for complete frames before returning, caller
            // should ensure that the input buffer is drained into the engine or stop if the engine
            // has changed to a non-writable state.
            while (buffer.isReadable() && engine.isWritable()) {
                stage.parse(context, buffer);
            }
        } catch (IOException ex) {
            transitionToErrorStage(new ProtonIOException(ex)).fireError(context);
        } catch (ProtonException pex) {
            transitionToErrorStage(pex).fireError(context);
        } catch (Exception error) {
            transitionToErrorStage(new ProtonException(error)).fireError(context);
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

    private FrameParserStage transitionToFrameBufferingStage(int length) {
        return stage = frameBufferingStage.reset(length);
    }

    private FrameParserStage initializeFrameBodyParsingStage(int length) {
        return stage = frameBodyParsingStage.reset(length);
    }

    private ParsingErrorStage transitionToErrorStage(ProtonException error) {
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
         * @param length
         *      The length to use for this part of the parsing operation
         *
         * @return a reference to this parsing stage for chaining.
         */
        FrameParserStage reset(int length);

    }

    //---- Built in FrameParserStages

    private class HeaderParsingStage implements FrameParserStage {

        private final byte[] headerBytes = new byte[AMQPHeader.HEADER_SIZE_BYTES];

        private int headerByte;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer incoming) throws IOException {
            while (incoming.isReadable() && headerByte < AMQPHeader.HEADER_SIZE_BYTES) {
                byte nextByte = incoming.readByte();
                try {
                    AMQPHeader.validateByte(headerByte, nextByte);
                } catch (IllegalArgumentException iae) {
                    throw new MalformedAMQPHeaderException(
                        String.format("Error on validation of header byte %d with value of %d", headerByte, nextByte), iae);
                }
                headerBytes[headerByte++] = nextByte;
            }

            if (headerByte == AMQPHeader.HEADER_SIZE_BYTES) {
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
        }

        @Override
        public HeaderParsingStage reset(int frameSize) {
            headerByte = 0;
            return this;
        }
    }

    private class FrameSizeParsingStage implements FrameParserStage {

        private int frameSize;
        private int multiplier = FRAME_SIZE_BYTES;

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

                int length = frameSize - FRAME_SIZE_BYTES;

                if (input.getReadableBytes() < length) {
                    transitionToFrameBufferingStage(length);
                } else {
                    initializeFrameBodyParsingStage(length);
                }

                stage.parse(context, input);
            }
        }

        private void validateFrameSize() throws IOException {
            if (frameSize < 8) {
               throw new ProtocolViolationException(String.format(
                    "specified frame size %d smaller than minimum frame header size 8", frameSize));
            }

            if (frameSize > configuration.getInboundMaxFrameSize()) {
                throw new ProtocolViolationException(String.format(
                    "specified frame size %d larger than maximum frame size %d", frameSize, configuration.getInboundMaxFrameSize()));
            }
        }

        @Override
        public FrameSizeParsingStage reset(int frameSize) {
            multiplier = FRAME_SIZE_BYTES;
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
        public FrameBufferingStage reset(int length) {
            buffer = ProtonByteBufferAllocator.DEFAULT.allocate(length, length);
            return this;
        }
    }

    private class FrameBodyParsingStage implements FrameParserStage {

        private int length;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
            int dataOffset = (input.readByte() << 2) & 0x3FF;
            int frameSize = length + FRAME_SIZE_BYTES;

            validateDataOffset(dataOffset, frameSize);

            int type = input.readByte() & 0xFF;
            short channel = input.readShort();

            // Skip over the extended header if present (i.e offset > 8)
            if (dataOffset != 8) {
                input.setReadIndex(input.getReadIndex() + dataOffset - 8);
            }

            final int frameBodySize = frameSize - dataOffset;

            ProtonBuffer payload = null;
            Object val = null;

            if (frameBodySize > 0) {
                int startReadIndex = input.getReadIndex();
                val = decoder.readObject(input, decoderState);

                // Copy the payload portion of the incoming bytes for now as the incoming may be
                // from a wrapped pooled buffer and for now we have no way of retaining or otherwise
                // ensuring that the buffer remains ours.  Since we might want to store received
                // data at a client level and decode later we could end up losing the data to reuse
                // if it was pooled.
                if (input.isReadable()) {
                    int payloadSize = frameBodySize - (input.getReadIndex() - startReadIndex);
                    // Check that the remaining bytes aren't part of another frame.
                    if (payloadSize > 0) {
                        payload = configuration.getBufferAllocator().allocate(payloadSize, payloadSize);
                        payload.writeBytes(input, payloadSize);
                    }
                }
            } else {
                transitionToFrameSizeParsingStage();
                context.fireRead(EmptyFrame.INSTANCE);
                return;
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
                // Ensure we process transition from SASL to AMQP header state
                handleRead(context, saslFrame);
            } else {
                throw new ProtocolViolationException(String.format("unknown frame type: %d", type));
            }
        }

        private void validateDataOffset(int dataOffset, int frameSize) throws ProtonException {
            if (dataOffset < 8) {
                throw new ProtocolViolationException(String.format(
                    "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8));
            }

            if (dataOffset > frameSize) {
                throw new ProtocolViolationException(String.format(
                    "specified frame data offset %d larger than the frame size %d", dataOffset, frameSize));
            }
        }

        @Override
        public FrameBodyParsingStage reset(int length) {
            this.length = length;
            return this;
        }
    }

    /*
     * If parsing fails the parser enters the failed state and remains there always throwing the given exception
     * if additional parsing is requested.
     */
    private class ParsingErrorStage implements FrameParserStage {

        private final ProtonException parsingError;

        public ParsingErrorStage(ProtonException parsingError) {
            this.parsingError = parsingError;
        }

        public void fireError(EngineHandlerContext context) {
            throw parsingError;
        }

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) throws IOException {
            throw new IOException(parsingError);
        }

        @Override
        public ParsingErrorStage reset(int length) {
            return this;
        }
    }
}
