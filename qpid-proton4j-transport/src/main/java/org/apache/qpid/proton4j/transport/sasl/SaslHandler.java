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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Base class used for common portions of the SASL processing pipeline.
 */
public class SaslHandler implements TransportHandler {

    private Decoder saslDecoder = CodecFactory.getSaslDecoder();
    private Encoder saslEncoder = CodecFactory.getSaslEncoder();

    private final SaslFrameParser frameParser;

    private SaslContext saslContext;

    /*
     * The Handler must be create from the client or server methods to configure
     * the state correctly.
     */
    private SaslHandler() {
        frameParser = new SaslFrameParser(saslDecoder);
    }

    public Encoder getSaslEndoer() {
        return saslEncoder;
    }

    public void setSaslEncoder(Encoder encoder) {
        this.saslEncoder = encoder;
    }

    public Decoder getSaslDecoder() {
        return saslDecoder;
    }

    public void setSaslDecoder(Decoder decoder) {
        this.saslDecoder = decoder;
    }

    public boolean isDone() {
        return saslContext.isDone();
    }

    public static SaslHandler client(SaslClientListener listener) {
        SaslHandler handler = new SaslHandler();
        SaslClientContext context = new SaslClientContext(handler, listener);
        handler.saslContext = context;

        return handler;
    }

    public static SaslHandler server(SaslServerListener listener) {
        SaslHandler handler = new SaslHandler();
        SaslServerContext context = new SaslServerContext(handler, listener);
        handler.saslContext = context;

        return handler;
    }

    //----- TransportHandler implementation ----------------------------------//

    @Override
    public void handleRead(TransportHandlerContext context, ProtonBuffer buffer) {
        if (isDone()) {
            context.fireRead(buffer);
        } else {
            try {
                frameParser.parse(context, buffer);
            } catch (IOException e) {
                // TODO - A more well defined exception API might allow for only
                //        one error event method ?
                context.fireDecodingError(e);
            }
        }
    }

    @Override
    public void handleHeaderFrame(TransportHandlerContext context, HeaderFrame header) {
        if (isDone()) {
            context.fireHeaderFrame(header);
        }

        saslContext.handleHeaderFrame(context, header);
    }

    @Override
    public void handleSaslFrame(TransportHandlerContext context, SaslFrame frame) {
        if (isDone()) {
            // TODO specific error for this case.
            context.fireFailed(new IllegalStateException(
                "Unexpected SASL Frame: SASL processing has already completed"));
        }

        frame.getBody().invoke(saslContext, context);
    }

    @Override
    public void handleProtocolFrame(TransportHandlerContext context, ProtocolFrame frame) {
        if (isDone()) {
            context.fireProtocolFrame(frame);
        } else {
            // TODO - Pipelined Connect, hold frame for later
        }
    }

    @Override
    public void transportEncodingError(TransportHandlerContext context, Throwable e) {
    }

    @Override
    public void transportDecodingError(TransportHandlerContext context, Throwable e) {
    }

    @Override
    public void transportFailed(TransportHandlerContext context, Throwable e) {
    }

    @Override
    public void transportReadable(TransportHandlerContext context) {
    }

    @Override
    public void transportWritable(TransportHandlerContext context) {
    }

    @Override
    public void write(Frame<?> frame) {
    }

    @Override
    public void flush() {
    }
}
