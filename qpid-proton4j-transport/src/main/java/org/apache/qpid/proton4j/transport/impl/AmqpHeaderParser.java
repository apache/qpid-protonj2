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

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.FrameParser;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.exceptions.TransportException;

/**
 * Used to parse incoming AMQP Headers from a data stream.
 */
public class AmqpHeaderParser implements FrameParser {

    private enum State {
        HEADER0,
        HEADER1,
        HEADER2,
        HEADER3,
        HEADER4,
        HEADER5,
        HEADER6,
        HEADER7,
        DONE,
        ERROR
    }

    private State parsingState = State.HEADER0;

    private AMQPHeader header = AMQPHeader.getRawAMQPHeader();
    private final HeaderFrame headerFrame = new HeaderFrame(header);

    @Override
    public void reset() {
        parsingState = State.HEADER0;
    }

    @Override
    public void parse(TransportHandlerContext context, ProtonBuffer incoming) throws IOException {
        TransportException parsingError = null;

        while (incoming.isReadable() && parsingState != State.ERROR && parsingState != State.ERROR) {
            switch (parsingState) {
                case HEADER0:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(0)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(0), parsingState));
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
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(1), parsingState));
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
                        if (c != header.getByteAt(2)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(2), parsingState));
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
                        if (c != header.getByteAt(3)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(3), parsingState));
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
                        if (c != header.getByteAt(4)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(4), parsingState));
                            parsingState = State.ERROR;
                            break;
                        }
                        parsingState = State.HEADER5;
                    } else {
                        break;
                    }
                case HEADER5:
                    if (incoming.isReadable()) {
                        byte c = incoming.readByte();
                        if (c != header.getByteAt(5)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(5), parsingState));
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
                        if (c != header.getByteAt(6)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(6), parsingState));
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
                        if (c != header.getByteAt(7)) {
                            parsingError = new TransportException(String.format(
                                "AMQP header mismatch value %x, expecting %x. In state: %s", c, header.getByteAt(7), parsingState));
                            parsingState = State.ERROR;
                            break;
                        } else {
                            parsingState = State.DONE;
                        }
                    } else {
                        break;
                    }
                default:
                    parsingError = new TransportException("AMQP Header parse in invalid state.");
                    parsingState = State.ERROR;
                    break;
            }
        }

        if (parsingState == State.DONE) {
            context.fireHeaderFrame(headerFrame);
        } else if (parsingState == State.ERROR) {
            throw parsingError;
        }
    }
}
