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

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;

/**
 * Factory Class used to create new instances of AMQP type
 * Encoder and Decoder instances registered in the factory.
 */
public final class CodecFactory {

    private static final String BUILTIN_ENTRY = "builtin";

    private static final Map<String, Class<Encoder>> encoderRegistry = new HashMap<>();
    private static final Map<String, Class<Decoder>> decoderRegistry = new HashMap<>();

    private CodecFactory() {
    }

    public static Encoder getEncoder(String encoderName) throws InstantiationException, IllegalAccessException {
        if (BUILTIN_ENTRY.equalsIgnoreCase(encoderName)) {
            return getDefaultEncoder();
        }

        if (encoderRegistry.containsKey(encoderName)) {
            return encoderRegistry.get(encoderName).newInstance();
        }

        throw new NoSuchElementException("No encoder registered with name: " + encoderName);
    }

    public static Decoder getDecoder(String decoderName) throws InstantiationException, IllegalAccessException {
        if (BUILTIN_ENTRY.equalsIgnoreCase(decoderName)) {
            return getDefaultDecoder();
        }

        if (decoderRegistry.containsKey(decoderName)) {
            return decoderRegistry.get(decoderName).newInstance();
        }

        throw new NoSuchElementException("No decoder registered with name: " + decoderName);
    }

    public static Encoder getDefaultEncoder() {
        return ProtonEncoderFactory.create();
    }

    public static Decoder getDefaultDecoder() {
        return ProtonDecoderFactory.create();
    }
}
