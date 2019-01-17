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
package org.apache.qpid.proton4j.transport.handlers;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.transport.TransportHandlerAdapter;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Handler that encodes performatives into properly formed frames for IO
 */
public class FrameWritingHandler extends TransportHandlerAdapter {

    private Encoder saslEncoder = CodecFactory.getSaslEncoder();
    private Encoder encoder = CodecFactory.getEncoder();

    private EncoderState saslEncoderState;
    private EncoderState encoderState;

    public Encoder getEndoer() {
        return encoder;
    }

    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    public Encoder getSaslEndoer() {
        return saslEncoder;
    }

    public void setSaslEncoder(Encoder encoder) {
        this.saslEncoder = encoder;
    }

    @Override
    public void handlerAdded(TransportHandlerContext context) throws Exception {
        saslEncoderState = getSaslEndoer().newEncoderState();
        encoderState = getEndoer().newEncoderState();
    }

    @Override
    public void handlerRemoved(TransportHandlerContext context) throws Exception {
        saslEncoderState = null;
        encoder = null;
    }

    @Override
    public void handleWrite(TransportHandlerContext context, AMQPHeader header) {
        context.fireWrite(header.getBuffer());
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        try {
            // TODO - An outbound frame type will contain all we need to encode
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleWrite(TransportHandlerContext context, SaslPerformative performative) {
        try {
            // TODO - An outbound frame type will contain all we need to encode
        } finally {
            saslEncoderState.reset();
        }
    }
}
