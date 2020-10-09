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
import java.util.LinkedHashMap;
import java.util.Map;

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
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;

/**
 * Decoder of AMQP Message Annotations type values from a byte stream.
 */
public final class MessageAnnotationsTypeDecoder extends AbstractDescribedTypeDecoder<MessageAnnotations> {

    @Override
    public Class<MessageAnnotations> getTypeClass() {
        return MessageAnnotations.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return MessageAnnotations.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return MessageAnnotations.DESCRIPTOR_SYMBOL;
    }

    @Override
    public MessageAnnotations readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(buffer, state);
            return new MessageAnnotations(null);
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        return new MessageAnnotations(readMap(buffer, state, (MapTypeDecoder) decoder));
    }

    @Override
    public MessageAnnotations[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        MessageAnnotations[] result = new MessageAnnotations[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(buffer, state);
                result[i] = new MessageAnnotations(null);
            }
            return result;
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        for (int i = 0; i < count; ++i) {
            result[i] = new MessageAnnotations(readMap(buffer, state, mapDecoder));
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

    private Map<Symbol, Object> readMap(ProtonBuffer buffer, DecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        int size = mapDecoder.readSize(buffer);
        int count = mapDecoder.readCount(buffer);

        if (count > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        // Count include both key and value so we must include that in the loop
        Map<Symbol, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Symbol key = state.getDecoder().readSymbol(buffer, state);
            Object value = state.getDecoder().readObject(buffer, state);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public MessageAnnotations readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(stream, state);
            return new MessageAnnotations(null);
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        return new MessageAnnotations(readMap(stream, state, (MapTypeDecoder) decoder));
    }

    @Override
    public MessageAnnotations[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        MessageAnnotations[] result = new MessageAnnotations[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(stream, state);
                result[i] = new MessageAnnotations(null);
            }
            return result;
        }

        checkIsExpectedType(MapTypeDecoder.class, decoder);

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        for (int i = 0; i < count; ++i) {
            result[i] = new MessageAnnotations(readMap(stream, state, mapDecoder));
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

    private Map<Symbol, Object> readMap(InputStream stream, StreamDecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        int size = mapDecoder.readSize(stream);
        int count = mapDecoder.readCount(stream);

        // Count include both key and value so we must include that in the loop
        Map<Symbol, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Symbol key = state.getDecoder().readSymbol(stream, state);
            Object value = state.getDecoder().readObject(stream, state);

            map.put(key, value);
        }

        return map;
    }
}
