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
package org.apache.qpid.proton4j.transport.sasl;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.transport.FrameParser;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.exceptions.TransportException;

/**
 * Parser of SASL Frames from the incoming data stream
 */
public class SaslFrameParser implements FrameParser {

    enum State {
        HEADER0,
        HEADER1,
        HEADER2,
        HEADER3,
        HEADER4,
        HEADER5,
        HEADER6,
        HEADER7,
        SIZE_0,
        SIZE_1,
        SIZE_2,
        SIZE_3,
        PRE_PARSE,
        BUFFERING,
        PARSING,
        ERROR
    }

    public static final byte SASL_FRAME_TYPE = (byte) 1;

    private State state = State.SIZE_0;
    private int size;

    private SaslHandler sasl;
    private ProtonBuffer buffer;
    private int frameSizeLimit;
    private Decoder decoder;
    private DecoderState decoderState;
    private AMQPHeader header = AMQPHeader.getSASLHeader();
    private HeaderFrame headerFrame = new HeaderFrame(header);

    public SaslFrameParser(Decoder decoder) {
        this.decoder = decoder;
        this.decoderState = decoder.newDecoderState();
    }

    @Override
    public void reset() {
        header = AMQPHeader.getSASLHeader();
        state = State.SIZE_0;
        buffer = null;
    }

    @Override
    public void parse(TransportHandlerContext context, ProtonBuffer incoming) throws IOException {
        TransportException parsingError = null;
        int size = this.size;
        State parsingState = this.state;
        ProtonBuffer input = incoming;

        while (input.isReadable() && parsingState != State.ERROR && !sasl.isDone()) {
            switch (parsingState) {
                case HEADER0:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER1;
                    } else {
                        break;
                    }
                case HEADER1:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(1)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER2;
                    } else {
                        break;
                    }
                case HEADER2:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER3;
                    } else {
                        break;
                    }
                case HEADER3:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER4;
                    } else {
                        break;
                    }
                case HEADER4:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            header = AMQPHeader.getRawAMQPHeader();
                            if (c != header.getByteAt(state.ordinal())) {
                                parsingError = new TransportException(String.format(
                                    "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                                parsingState = State.ERROR;
                                break;
                            }
                        }
                        parsingState = State.HEADER5;
                    } else {
                        break;
                    }
                case HEADER5:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER6;
                    } else {
                        break;
                    }
                case HEADER6:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER7;
                    } else {
                        break;
                    }
                case HEADER7:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(state.ordinal())) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(state.ordinal()), parsingState));
                            parsingState = State.ERROR;
                            break;
                        } else {
                            sasl.handleHeaderFrame(context, headerFrame);
                            parsingState = State.SIZE_0;
                        }
                    } else {
                        break;
                    }
                case SIZE_0:
                    if (!input.isReadable()) {
                        break;
                    }

                    if (input.getReadableBytes() >= 4) {
                        size = input.readInt();
                        parsingState = State.PRE_PARSE;
                        break;
                    } else {
                        size = (input.readByte() << 24) & 0xFF000000;
                        if (!input.isReadable()) {
                            parsingState = State.SIZE_1;
                            break;
                        }
                    }
                case SIZE_1:
                    size |= (input.readByte() << 16) & 0xFF0000;
                    if (!input.isReadable()) {
                        parsingState = State.SIZE_2;
                        break;
                    }
                case SIZE_2:
                    size |= (input.readByte() << 8) & 0xFF00;
                    if (!input.isReadable()) {
                        parsingState = State.SIZE_3;
                        break;
                    }
                case SIZE_3:
                    size |= input.readByte() & 0xFF;
                    parsingState = State.PRE_PARSE;
                case PRE_PARSE:
                    if (size < 8) {
                        parsingError = new TransportException(String.format(
                            "specified frame size %d smaller than minimum SASL frame header size 8", size));
                        parsingState = State.ERROR;
                        break;
                    }

                    if (size > frameSizeLimit) {
                        parsingError = new TransportException(String.format(
                            "specified frame size %d larger than maximum SASL frame size %d", size, frameSizeLimit));
                        parsingState = State.ERROR;
                        break;
                    }

                    if (input.getReadableBytes() < size - 4) {
                        buffer = ProtonByteBufferAllocator.DEFAULT.allocate(size - 4, size - 4);
                        buffer.writeBytes(input);
                        parsingState = State.BUFFERING;
                        break;
                    }
                case BUFFERING:
                    if (buffer != null) {
                        if (input.getReadableBytes() < buffer.getWritableBytes()) {
                            buffer.writeBytes(input);
                            break;
                        } else {
                            buffer.writeBytes(input, buffer.getWritableBytes());
                            parsingState = State.PARSING;
                            input = buffer;
                        }
                    }
                case PARSING:

                    int dataOffset = (input.readByte() << 2) & 0x3FF;

                    if (dataOffset < 8) {
                        parsingError = new TransportException(String.format(
                            "specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8));
                        parsingState = State.ERROR;
                        break;
                    } else if (dataOffset > size) {
                        parsingError = new TransportException(String.format(
                            "specified frame data offset %d larger than the frame size %d", dataOffset, size));
                        parsingState = State.ERROR;
                        break;
                    }

                    // type

                    int type = input.readByte() & 0xFF;
                    // SASL frame has no type-specific content in the frame
                    // header, so we skip next two bytes
                    input.readByte();
                    input.readByte();

                    if (type != SASL_FRAME_TYPE) {
                        parsingError = new TransportException(String.format("unknown frame type: %d", type));
                        parsingState = State.ERROR;
                        break;
                    }

                    if (dataOffset != 8) {
                        input.setReadIndex(input.getReadIndex() + dataOffset - 8);
                    }

                    try {
                        // TODO - Using a SASL only configured decoder we would not
                        //        get an object unless it was a Sasl performative but
                        //        instead would get an exception.
                        Object val = decoder.readObject(input, decoderState);

                        final Binary payload;

                        if (input.isReadable()) {
                            byte[] payloadBytes = new byte[input.getReadableBytes()];
                            input.readBytes(payloadBytes);
                            payload = new Binary(payloadBytes);
                        } else {
                            payload = null;
                        }

                        if (val instanceof SaslPerformative) {
                            SaslPerformative performative = (SaslPerformative) val;
                            SaslFrame saslFrame = new SaslFrame(performative, payload);
                            sasl.handleSaslFrame(context, saslFrame);
                            input = incoming;
                            parsingState = State.SIZE_0;
                        } else {
                            parsingState = State.ERROR;
                            parsingError = new TransportException(String.format(
                                "Unexpected frame type encountered." + " Found a %s which does not implement %s",
                                val == null ? "null" : val.getClass(), SaslPerformative.class));
                        }
                    } catch (IOException ex) {
                        parsingState = State.ERROR;
                        parsingError = new TransportException(ex);
                    }

                    break;
                case ERROR:
                    break;
            }
        }

        this.state = parsingState;
        this.size = size;

        if (this.state == State.ERROR) {
            if (parsingError != null) {
                throw parsingError;
            } else {
                throw new TransportException("Unable to parse, probably because of a previous error");
            }
        }
    }
}
