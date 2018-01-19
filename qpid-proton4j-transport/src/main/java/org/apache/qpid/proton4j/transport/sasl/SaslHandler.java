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

import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.Encoder;

/**
 * Base class used for common portions of the SASL processing pipeline.
 */
public class SaslHandler {

    private Decoder saslDecoder = CodecFactory.getSaslDecoder();
    private Encoder saslEncoder = CodecFactory.getSaslEncoder();

    private boolean done;

    private AbstractSaslContext context;
    private SaslFrameParser frameParser;

    private SaslHandler() {
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
        return done;
    }

    public static SaslHandler client(SaslClientListener listener) {
        SaslHandler handler = new SaslHandler();
        SaslClientContext context = new SaslClientContext(handler, listener);
        handler.context = context;

        return handler;
    }

    public static SaslHandler server(SaslServerListener listener) {
        SaslHandler handler = new SaslHandler();
        SaslServerContext context = new SaslServerContext(handler, listener);
        handler.context = context;

        return handler;
    }
}
