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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.MapTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.NullTypeDecoder;

/**
 * Decoder of AMQP Footer type values from a byte stream.
 */
public class FooterTypeDecoder implements DescribedTypeDecoder<Footer> {

    @Override
    public Class<Footer> getTypeClass() {
        return Footer.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Footer.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Footer.DESCRIPTOR_SYMBOL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Footer readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(buffer, state);
            return new Footer(null);
        }

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected Map type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        return new Footer(mapDecoder.readValue(buffer, state));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Footer[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        Footer[] result = new Footer[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(buffer, state);
                result[i] = new Footer(null);
            }
            return result;
        }

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected Map type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        for (int i = 0; i < count; ++i) {
            decoder.readValue(buffer, state);
            result[i] = new Footer(mapDecoder.readValue(buffer, state));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected Map type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        decoder.skipValue(buffer, state);
    }
}
