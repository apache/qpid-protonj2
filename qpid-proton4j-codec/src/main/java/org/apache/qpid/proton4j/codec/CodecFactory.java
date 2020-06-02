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

    /**
     * Sets an {@link Encoder} instance that will be returned from all calls to the
     * {@link CodecFactory#getEncoder()}.  If no {@link Encoder} is configured then the
     * calls to get an Encoder instance will return the default Encoder from the library.
     *
     * @param encoder
     *      The encoder to return from all calls to the {@link CodecFactory#getEncoder()} method/
     */
    public static void setEncoder(Encoder encoder) {
        amqpTypeEncoder = encoder;
    }

    /**
     * Sets an {@link Decoder} instance that will be returned from all calls to the
     * {@link CodecFactory#getDecoder()}.  If no {@link Decoder} is configured then the
     * calls to get an Decoder instance will return the default Decoder from the library.
     *
     * @param decoder
     *      The decoder to return from all calls to the {@link CodecFactory#getDecoder()} method/
     */
    public static void setDecoder(Decoder decoder) {
        amqpTypeDecoder = decoder;
    }

    /**
     * Sets an {@link Encoder} instance that will be returned from all calls to the
     * {@link CodecFactory#getSaslEncoder()}.  If no {@link Encoder} is configured then the
     * calls to get an Encoder instance will return the default Encoder from the library.
     * The Encoder configured should only accept encodes of the SASL AMQP types.
     *
     * @param encoder
     *      The encoder to return from all calls to the {@link CodecFactory#getSaslEncoder()} method/
     */
    public static void setSaslEncoder(Encoder encoder) {
        saslTypeEncoder = encoder;
    }

    /**
     * Sets an {@link Decoder} instance that will be returned from all calls to the
     * {@link CodecFactory#getSaslDecoder()}.  If no {@link Decoder} is configured then the
     * calls to get an Decoder instance will return the default Decoder from the library.
     * The Decoder configured should only decode the SASL AMQP types.
     *
     * @param decoder
     *      The decoder to return from all calls to the {@link CodecFactory#getSaslDecoder()} method/
     */
    public static void setSaslDecoder(Decoder decoder) {
        saslTypeDecoder = decoder;
    }

    public static Encoder getEncoder() {
        if (amqpTypeEncoder == null) {
            amqpTypeEncoder = getDefaultEncoder();
        }

        return amqpTypeEncoder;
    }

    public static Decoder getDecoder() {
        if (amqpTypeDecoder == null) {
            amqpTypeDecoder = getDefaultDecoder();
        }

        return amqpTypeDecoder;
    }

    public static Encoder getSaslEncoder() {
        if (saslTypeEncoder == null) {
            saslTypeEncoder = getDefaultSaslEncoder();
        }

        return saslTypeEncoder;
    }

    public static Decoder getSaslDecoder() {
        if (saslTypeDecoder == null) {
            saslTypeDecoder = getDefaultSaslDecoder();
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
