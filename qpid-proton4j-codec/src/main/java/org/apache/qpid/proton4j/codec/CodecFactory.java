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
package org.apache.qpid.proton4j.codec;

import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;

/**
 * Factory Class used to create new instances of AMQP type
 * Encoder and Decoder instances registered in the factory.
 */
public final class CodecFactory {

    private static Encoder amqpTypeEncoder;
    private static Encoder saslTypeEncoder;
    private static Decoder amqpTypeDecoder;
    private static Decoder saslTypeDecoder;

    private CodecFactory() {
    }

    public static void setEncoder(Encoder encoder) {
        amqpTypeEncoder = encoder;
    }

    public static void setSaslEncoder(Encoder encoder) {
        saslTypeEncoder = encoder;
    }

    public static void setDecoder(Decoder decoder) {
        amqpTypeDecoder = decoder;
    }

    public static void setSaslDecoder(Decoder decoder) {
        saslTypeDecoder = decoder;
    }

    public static Encoder getEncoder() throws InstantiationException, IllegalAccessException {
        if (amqpTypeEncoder == null) {
            return getDefaultEncoder();
        }

        return amqpTypeEncoder;
    }

    public static Decoder getDecoder() throws InstantiationException, IllegalAccessException {
        if (amqpTypeDecoder == null) {
            return getDefaultDecoder();
        }

        return amqpTypeDecoder;
    }

    public static Encoder getSaslEncoder() throws InstantiationException, IllegalAccessException {
        if (saslTypeEncoder == null) {
            return getDefaultSaslEncoder();
        }

        return saslTypeEncoder;
    }

    public static Decoder getSaslDecoder() throws InstantiationException, IllegalAccessException {
        if (saslTypeDecoder == null) {
            return getDefaultSaslDecoder();
        }

        return saslTypeDecoder;
    }

    public static Encoder getDefaultEncoder() {
        return ProtonEncoderFactory.create();
    }

    public static Decoder getDefaultDecoder() {
        return ProtonDecoderFactory.create();
    }

    public static Encoder getDefaultSaslEncoder() {
        return ProtonEncoderFactory.createSasl();
    }

    public static Decoder getDefaultSaslDecoder() {
        return ProtonDecoderFactory.createSasl();
    }
}
