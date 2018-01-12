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
import org.apache.qpid.proton4j.transport.SaslHandler;
import org.apache.qpid.proton4j.transport.SaslListener;

/**
 * Base class used for common portions of the SASL processing pipeline.
 */
public class ProtonSaslHandler implements SaslHandler {

    private Decoder saslDecoder = CodecFactory.getSaslDecoder();
    private Encoder saslEncoder = CodecFactory.getSaslEncoder();

    private SaslListener saslListener;
    private boolean done;

    @Override
    public void setSaslListener(SaslListener saslListener) {
        this.saslListener = saslListener;
    }

    @Override
    public SaslListener getSaslListener() {
        return saslListener;
    }

    @Override
    public Encoder getSaslEndoer() {
        return saslEncoder;
    }

    @Override
    public void setSaslEncoder(Encoder encoder) {
        this.saslEncoder = encoder;
    }

    @Override
    public Decoder getSaslDecoder() {
        return saslDecoder;
    }

    @Override
    public void setSaslDecoder(Decoder decoder) {
        this.saslDecoder = decoder;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public void client() {
    }

    @Override
    public void server() {
    }
}
