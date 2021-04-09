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
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;

/**
 * Decoder of AMQP Delivery Annotations type values from a byte stream.
 */
public final class DeliveryAnnotationsTypeDecoder extends AbstractDescribedTypeDecoder<DeliveryAnnotations> {

    @Override
    public Class<DeliveryAnnotations> getTypeClass() {
        return DeliveryAnnotations.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DeliveryAnnotations.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DeliveryAnnotations.DESCRIPTOR_SYMBOL;
    }

    @Override
    public DeliveryAnnotations readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(buffer, state);
            return new DeliveryAnnotations(null);
        }

        return new DeliveryAnnotations(readMap(buffer, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
    }

    @Override
    public DeliveryAnnotations[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final DeliveryAnnotations[] result = new DeliveryAnnotations[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(buffer, state);
                result[i] = new DeliveryAnnotations(null);
            }
            return result;
        }

        for (int i = 0; i < count; ++i) {
            result[i] = new DeliveryAnnotations(readMap(buffer, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof NullTypeDecoder)) {
            checkIsExpectedType(MapTypeDecoder.class, decoder);
            decoder.skipValue(buffer, state);
        }
    }

    private Map<Symbol, Object> readMap(ProtonBuffer buffer, DecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        final int size = mapDecoder.readSize(buffer);
        final int count = mapDecoder.readCount(buffer);

        if (count > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        // Count include both key and value so we must include that in the loop
        final Map<Symbol, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Symbol key = state.getDecoder().readSymbol(buffer, state);
            Object value = state.getDecoder().readObject(buffer, state);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public DeliveryAnnotations readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (decoder instanceof NullTypeDecoder) {
            decoder.readValue(stream, state);
            return new DeliveryAnnotations(null);
        }

        return new DeliveryAnnotations(readMap(stream, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
    }

    @Override
    public DeliveryAnnotations[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final DeliveryAnnotations[] result = new DeliveryAnnotations[count];

        if (decoder instanceof NullTypeDecoder) {
            for (int i = 0; i < count; ++i) {
                decoder.readValue(stream, state);
                result[i] = new DeliveryAnnotations(null);
            }
            return result;
        }

        for (int i = 0; i < count; ++i) {
            result[i] = new DeliveryAnnotations(readMap(stream, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (!(decoder instanceof NullTypeDecoder)) {
            checkIsExpectedType(MapTypeDecoder.class, decoder);
            decoder.skipValue(stream, state);
        }
    }

    private Map<Symbol, Object> readMap(InputStream stream, StreamDecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        final int size = mapDecoder.readSize(stream);
        final int count = mapDecoder.readCount(stream);

        // Count include both key and value so we must include that in the loop
        final Map<Symbol, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Symbol key = state.getDecoder().readSymbol(stream, state);
            Object value = state.getDecoder().readObject(stream, state);

            map.put(key, value);
        }

        return map;
    }
}
