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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.MapTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.NullTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Footer;

/**
 * Decoder of AMQP Footer type values from a byte stream.
 */
public final class FooterTypeDecoder extends AbstractDescribedTypeDecoder<Footer> {

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
    public Footer readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(buffer, state);
            return new Footer(null);
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        return new Footer(mapDecoder.readValue(buffer, state));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Footer[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        Footer[] result = new Footer[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(buffer, state);
                result[i] = new Footer(null);
            }
            return result;
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        for (int i = 0; i < count; ++i) {
            result[i] = new Footer(mapDecoder.readValue(buffer, state));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof NullTypeDecoder)) {
            checkIsExpectedType(MapTypeDecoder.class, decoder);
            decoder.skipValue(buffer, state);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Footer readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(stream, state);
            return new Footer(null);
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        return new Footer(mapDecoder.readValue(stream, state));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Footer[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        Footer[] result = new Footer[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(stream, state);
                result[i] = new Footer(null);
            }
            return result;
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        for (int i = 0; i < count; ++i) {
            decoder.readValue(stream, state);
            result[i] = new Footer(mapDecoder.readValue(stream, state));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (!(decoder instanceof NullTypeDecoder)) {
            checkIsExpectedType(MapTypeDecoder.class, decoder);
            decoder.skipValue(stream, state);
        }
    }
}
