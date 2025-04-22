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
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.engine.AMQPPerformativeEnvelopePool;
import org.apache.qpid.protonj2.engine.EmptyEnvelope;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.FrameDecodingException;
import org.apache.qpid.protonj2.engine.exceptions.MalformedAMQPHeaderException;
import org.apache.qpid.protonj2.engine.exceptions.ProtonException;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.security.SaslOutcome;
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * Handler used to parse incoming frame data input into the engine
 */
public class ProtonFrameDecodingHandler implements EngineHandler, SaslPerformative.SaslPerformativeHandler<EngineHandlerContext> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonFrameDecodingHandler.class);

    /**
     * Frame type indicator for AMQP protocol frames.
     */
    public static final byte AMQP_FRAME_TYPE = (byte) 0;

    /**
     * Frame type indicator for SASL protocol frames.
     */
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    /**
     * The specified encoding size for the frame size value of each encoded frame.
     */
    public static final int FRAME_SIZE_BYTES = 4;

    /**
     * Size of the initial portion of an encoded from before any extended header portion.
     */
    private static final int FRAME_PREAMBLE_BYTES = 8;

    private final AMQPPerformativeEnvelopePool<IncomingAMQPEnvelope> framePool = AMQPPerformativeEnvelopePool.incomingEnvelopePool();

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
        } catch (FrameDecodingException frameEx) {
            transitionToErrorStage(frameEx).fireError(context);
        } catch (ProtonException pex) {
            transitionToErrorStage(pex).fireError(context);
        } catch (DecodeException ex) {
            transitionToErrorStage(new FrameDecodingException(ex.getMessage(), ex)).fireError(context);
        } catch (Exception error) {
            transitionToErrorStage(new ProtonException(error.getMessage(), error)).fireError(context);
        }
    }

    @Override
    public void handleRead(EngineHandlerContext context, SASLEnvelope envelope) {
        envelope.getBody().invoke(this, context);
        context.fireRead(envelope);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SASLEnvelope envelope) {
        envelope.invoke(this, context);
        context.fireWrite(envelope);
    }

    //----- SASL Performative Handler to check for change to non-SASL state

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
            LOG.trace("Frame decoder encountered error: ", error);
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
         */
        void parse(EngineHandlerContext context, ProtonBuffer input);

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

    private final class HeaderParsingStage implements FrameParserStage {

        private final byte[] headerBytes = new byte[AMQPHeader.HEADER_SIZE_BYTES];

        private int headerByte;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer incoming) {
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
                AMQPHeader.validate(headerBytes);

                // Trim the header from the inbound buffer to avoid those bytes being available
                // if another frame is contained within this buffer instance.
                if (incoming.isReadable()) {
                    incoming.readSplit(0).close();
                }

                // Transition to parsing the frames if any pipelined into this buffer.
                transitionToFrameSizeParsingStage();

                if (headerBytes[AMQPHeader.PROTOCOL_ID_INDEX] == AMQPHeader.SASL_PROTOCOL_ID) {
                    decoder = CodecFactory.getSaslDecoder();
                    decoderState = decoder.newDecoderState();
                    context.fireRead(HeaderEnvelope.SASL_HEADER_ENVELOPE);
                } else {
                    decoder = CodecFactory.getDecoder();
                    decoderState = decoder.newDecoderState();
                    // Once we've read an AMQP header we no longer care if any SASL work
                    // occurs as that would be erroneous behavior which this handler doesn't
                    // deal with.
                    ((ProtonEngineHandlerContext) context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
                    context.fireRead(HeaderEnvelope.AMQP_HEADER_ENVELOPE);
                }
            }
        }

        @Override
        public HeaderParsingStage reset(int frameSize) {
            headerByte = 0;
            return this;
        }
    }

    private final class FrameSizeParsingStage implements FrameParserStage {

        private int frameSize;
        private int multiplier = FRAME_SIZE_BYTES;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) {
            if (multiplier == FRAME_SIZE_BYTES && input.getReadableBytes() >= Integer.BYTES) {
                frameSize = input.readInt();
                multiplier = 0;
            } else {
                readFrameSizeInChunks(input);
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

        private void readFrameSizeInChunks(ProtonBuffer input) {
            while (input.isReadable()) {
                frameSize |= ((input.readByte() & 0xFF) << --multiplier * Byte.SIZE);
                if (multiplier == 0) {
                    break;
                }
            }
        }

        private void validateFrameSize() throws FrameDecodingException {
            if (Integer.compareUnsigned(frameSize, 8) < 0) {
               throw new FrameDecodingException(String.format(
                    "specified frame size %d smaller than minimum frame header size 8", frameSize));
            }

            if (Integer.toUnsignedLong(frameSize) > configuration.getInboundMaxFrameSize()) {
                throw new FrameDecodingException(String.format(
                    "specified frame size %s larger than maximum frame size %d",
                    Integer.toUnsignedString(frameSize), configuration.getInboundMaxFrameSize()));
            }
        }

        @Override
        public FrameSizeParsingStage reset(int frameSize) {
            multiplier = FRAME_SIZE_BYTES;
            this.frameSize = frameSize;
            return this;
        }
    }

    private final class FrameBufferingStage implements FrameParserStage {

        private ProtonCompositeBuffer buffer;
        private int frameBytesRemaining;
        private int frameSize;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) {
            if (input.getReadableBytes() > frameBytesRemaining) {
                buffer.append(input.readSplit(frameBytesRemaining));
            } else {
                buffer.append(input);
            }

            frameBytesRemaining = frameSize - buffer.getReadableBytes();

            if (frameBytesRemaining == 0) {
                // Now we can consume the buffer frame body.
                initializeFrameBodyParsingStage(buffer.getReadableBytes());
                try (ProtonBuffer buffered = buffer) {
                    stage.parse(context, buffer);
                } finally {
                    buffer = null;
                }
            }
        }

        @Override
        public FrameBufferingStage reset(int length) {
            buffer = configuration.getBufferAllocator().composite().convertToReadOnly();
            frameBytesRemaining = length;
            frameSize = length;

            return this;
        }
    }

    private final class FrameBodyParsingStage implements FrameParserStage {

        private int length;

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) {
            final int dataOffset = (input.readByte() << 2) & 0x3FF;
            final int frameSize = length + FRAME_SIZE_BYTES;
            final int frameBodySize = frameSize - dataOffset;

            validateDataOffset(dataOffset, frameSize);

            final int type = input.readByte() & 0xFF;
            final short channel = input.readShort();

            // Skip over the extended header if present (i.e offset > FRAME_PREAMBLE_SIZE)
            if (dataOffset != FRAME_PREAMBLE_BYTES) {
                input.advanceReadOffset(dataOffset - FRAME_PREAMBLE_BYTES);
            }

            if (frameBodySize > 0) {
                switch (type) {
                    case AMQP_FRAME_TYPE:
                        handleAMQPPerformative(context, channel, frameBodySize, input);
                        break;
                    case SASL_FRAME_TYPE:
                        handleSASLPerformative(context, input);
                        break;
                    default:
                        throw new FrameDecodingException(String.format("unknown frame type: %d", type));
                }
            } else {
                handleEmptyFrame(context);
            }
        }

        private void handleEmptyFrame(EngineHandlerContext context) {
            transitionToFrameSizeParsingStage();
            context.fireRead(EmptyEnvelope.INSTANCE);
        }

        private void handleAMQPPerformative(EngineHandlerContext context, short channel, int frameBodySize, ProtonBuffer input) {
            final int startReadIndex = input.getReadOffset();
            final Performative performative = decoder.readObject(input, decoderState, Performative.class);

            // Copy the payload portion of the incoming bytes for now as the incoming may be from a
            // wrapped pooled buffer and for now we have no way of retaining or otherwise ensuring
            // that the buffer remains ours. Since we might want to store received data at a client
            // level and decode later we could end up losing the data to reuse if it was pooled.
            if (input.isReadable()) {
                final ProtonBuffer payload;
                final int payloadSize = frameBodySize - (input.getReadOffset() - startReadIndex);

                if (payloadSize > 0) {
                    // The payload buffer is now only a read-only view of the bytes from the input that comprise it.
                    payload = input.copy(input.getReadOffset(), payloadSize, true);
                    input.advanceReadOffset(payloadSize);
                } else {
                    payload = null;
                }

                transitionToFrameSizeParsingStage();
                try {
                    context.fireRead(framePool.take(performative, channel, payload));
                } catch (Exception ex) {
                    if (payload != null) {
                        payload.close();
                    }
                }
            } else {
                transitionToFrameSizeParsingStage();
                context.fireRead(framePool.take(performative, channel, null));
            }
        }

        private void handleSASLPerformative(EngineHandlerContext context, ProtonBuffer input) {
            final SaslPerformative performative = (SaslPerformative) decoder.readObject(input, decoderState);;

            transitionToFrameSizeParsingStage();
            // Ensure we process transition from SASL to AMQP header state
            handleRead(context, new SASLEnvelope(performative));
        }

        private void validateDataOffset(int dataOffset, int frameSize) throws FrameDecodingException {
            if (dataOffset < 8) {
                throw new FrameDecodingException(String.format(
                    "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8));
            }

            if (dataOffset > frameSize) {
                throw new FrameDecodingException(String.format(
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
    private static class ParsingErrorStage implements FrameParserStage {

        private final ProtonException parsingError;

        public ParsingErrorStage(ProtonException parsingError) {
            this.parsingError = parsingError;
        }

        public void fireError(EngineHandlerContext context) {
            throw parsingError;
        }

        @Override
        public void parse(EngineHandlerContext context, ProtonBuffer input) {
            throw new FrameDecodingException(parsingError);
        }

        @Override
        public ParsingErrorStage reset(int length) {
            return this;
        }
    }
}
