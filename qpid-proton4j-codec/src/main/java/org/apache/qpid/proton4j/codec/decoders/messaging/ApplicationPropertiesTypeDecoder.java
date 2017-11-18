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
package org.apache.qpid.proton4j.codec.decoders.messaging;

import java.io.IOException;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.MapTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.NullTypeDecoder;

/**
 * Decoder of AMQP ApplicationProperties types from a byte stream
 */
public class ApplicationPropertiesTypeDecoder implements DescribedTypeDecoder<ApplicationProperties> {

    @Override
    public Class<ApplicationProperties> getTypeClass() {
        return ApplicationProperties.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return ApplicationProperties.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ApplicationProperties.DESCRIPTOR_SYMBOL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ApplicationProperties readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(buffer, state);
            return new ApplicationProperties(null);
        }

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected Map type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        Map<String, Object> map = mapDecoder.readValue(buffer, state);
        return new ApplicationProperties(map);
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            return;
        }

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        decoder.skipValue(buffer, state);
    }
}
