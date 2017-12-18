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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.FrameParser;
import org.apache.qpid.proton4j.transport.exceptions.TransportException;

/**
 * Used to parse incoming AMQP Headers from a data stream.
 */
public class AmqpHeaderParser implements FrameParser {

    public static final byte[] HEADER = new byte[]
        { 'A', 'M', 'Q', 'P', 0, 1, 0, 0 };

    public static final byte[] SASL_HEADER = new byte[]
        { 'A', 'M', 'Q', 'P', 3, 1, 0, 0 };

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

    private ProtonTransport transport;

    public AmqpHeaderParser(final ProtonTransport transport) {
        this.transport = transport;
    }

    @Override
    public void reset() {
        parsingState = State.HEADER0;
    }

    @Override
    public void parse(ProtonBuffer incoming) throws IOException {

        // TODO - Buffer incoming Header if smaller than 8 bytes, otherwise parse and consume

        TransportException parsingError = null;

        switch (parsingState) {
            case HEADER0:
                if (incoming.isReadable()) {
                    byte c = incoming.readByte();
                    if (c != HEADER[0]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[0], parsingState));
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
                    if (c != HEADER[1]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[1], parsingState));
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
                    if (c != HEADER[2]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[2], parsingState));
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
                    if (c != HEADER[3]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[3], parsingState));
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
                    if (c != HEADER[4]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[4], parsingState));
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
                    if (c != HEADER[5]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[5], parsingState));
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
                    if (c != HEADER[6]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[6], parsingState));
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
                    if (c != HEADER[7]) {
                        parsingError = new TransportException(String.format(
                            "AMQP header mismatch value %x, expecting %x. In state: %s", c, HEADER[7], parsingState));
                        parsingState = State.ERROR;
                        break;
                    }
                } else {
                    break;
                }
            default:
                parsingError = new TransportException("AMQP Header parse in invalid state.");
                parsingState = State.ERROR;
                break;
        }

        if (parsingError != null) {
            // TODO - Signal Transport of error
        } else {
            // TODO - Check complete and signal transport if so
        }
    }
}
