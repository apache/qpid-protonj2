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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedMapTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonScanningContext;
import org.apache.qpid.protonj2.codec.decoders.ScanningContext;
import org.apache.qpid.protonj2.codec.decoders.StreamScanningContext;
import org.apache.qpid.protonj2.codec.decoders.primitives.MapTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;

/**
 * Decoder of AMQP Message Annotations type values from a byte stream.
 */
public final class MessageAnnotationsTypeDecoder extends AbstractDescribedMapTypeDecoder<MessageAnnotations> {

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
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder.isNull()) {
            return new MessageAnnotations(null);
        }

        return new MessageAnnotations(readMap(buffer, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
    }

    @Override
    public MessageAnnotations[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final MessageAnnotations[] result = new MessageAnnotations[count];

        if (decoder.isNull()) {
            for (int i = 0; i < count; ++i) {
                result[i] = new MessageAnnotations(null);
            }
            return result;
        }

        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder);

        for (int i = 0; i < count; ++i) {
            result[i] = new MessageAnnotations(readMap(buffer, state, mapDecoder));
        }

        return result;
    }

    private Map<Symbol, Object> readMap(ProtonBuffer buffer, DecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        final int size = mapDecoder.readSize(buffer, state);
        final int count = mapDecoder.readCount(buffer, state);

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
    public MessageAnnotations readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (decoder.isNull()) {
            return new MessageAnnotations(null);
        }

        return new MessageAnnotations(readMap(stream, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
    }

    @Override
    public MessageAnnotations[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final MessageAnnotations[] result = new MessageAnnotations[count];

        if (decoder.isNull()) {
            for (int i = 0; i < count; ++i) {
                result[i] = new MessageAnnotations(null);
            }
            return result;
        }

        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder);

        for (int i = 0; i < count; ++i) {
            result[i] = new MessageAnnotations(readMap(stream, state, mapDecoder));
        }

        return result;
    }

    private Map<Symbol, Object> readMap(InputStream stream, StreamDecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        final int size = mapDecoder.readSize(stream, state);
        final int count = mapDecoder.readCount(stream, state);

        // Count include both key and value so we must include that in the loop
        final Map<Symbol, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Symbol key = state.getDecoder().readSymbol(stream, state);
            Object value = state.getDecoder().readObject(stream, state);

            map.put(key, value);
        }

        return map;
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of Symbol keys.
     */
    public static ScanningContext<Symbol> createScanContext(Symbol...keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of Symbol keys.
     */
    public static ScanningContext<Symbol> createScanContext(Collection<Symbol> keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link StreamScanningContext} for the collection of Symbol keys.
     */
    public static StreamScanningContext<Symbol> createStreamScanContext(Symbol...keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link StreamScanningContext} for the collection of Symbol keys.
     */
    public static StreamScanningContext<Symbol> createStreamScanContext(Collection<Symbol> keys) {
        return ProtonScanningContext.createSymbolScanContext(keys);
    }

    /**
     * Scans through the encoded {@link MessageAnnotations} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded MessageAnnotations have been
     * read and that decoding of the next object can begin if the provided buffer remains readable.
     *
     * @param buffer
     * 		The buffer to scan for specific key / value mappings
     * @param state
     * 		The decoder state used during the scanning
     * @param context
     * 		A matching context primed with the match data needed to match encoded keys.
     * @param matchConsumer
     * 		A {@link BiConsumer} that will be notified of each matching key / value pair.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    public void scanAnnotations(ProtonBuffer buffer, DecoderState state, ScanningContext<Symbol> context, BiConsumer<Symbol, Object> matchConsumer) throws DecodeException {
        scanMapEntries(buffer, state, context, matchConsumer);
    }

    /**
     * Scans through the encoded {@link MessageAnnotations} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded MessageAnnotations have been
     * read and that decoding of the next object can begin if the provided stream remains readable.
     *
     * @param stream
     * 		The {@link InputStream} to scan for specific key / value mappings
     * @param state
     * 		The decoder state used during the scanning
     * @param context
     * 		A matching context primed with the match data needed to match encoded keys.
     * @param matchConsumer
     * 		A {@link BiConsumer} that will be notified of each matching key / value pair.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    public void scanAnnotations(InputStream stream, StreamDecoderState state, StreamScanningContext<Symbol> context, BiConsumer<Symbol, Object> matchConsumer) throws DecodeException {
        scanMapEntries(stream, state, context, matchConsumer);
    }
}
