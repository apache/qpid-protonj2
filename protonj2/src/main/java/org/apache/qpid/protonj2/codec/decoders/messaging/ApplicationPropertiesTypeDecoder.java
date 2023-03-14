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
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
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
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;

/**
 * Decoder of AMQP ApplicationProperties types from a byte stream
 */
public final class ApplicationPropertiesTypeDecoder extends AbstractDescribedMapTypeDecoder<ApplicationProperties> {

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

    @Override
    public ApplicationProperties readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder.isNull()) {
            return new ApplicationProperties(null);
        }

        return new ApplicationProperties(readMap(buffer, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
    }

    @Override
    public ApplicationProperties[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final ApplicationProperties[] result = new ApplicationProperties[count];

        if (decoder.isNull()) {
            for (int i = 0; i < count; ++i) {
                result[i] = new ApplicationProperties(null);
            }
            return result;
        }

        for (int i = 0; i < count; ++i) {
            result[i] = new ApplicationProperties(readMap(buffer, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
        }

        return result;
    }

    private Map<String, Object> readMap(ProtonBuffer buffer, DecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        final int size = mapDecoder.readSize(buffer, state);
        final int count = mapDecoder.readCount(buffer, state);

        if (count > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        final Decoder decoder = state.getDecoder();

        // Count include both key and value so we must include that in the loop
        final Map<String, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            map.put(decoder.readString(buffer, state), decoder.readObject(buffer, state));
        }

        return map;
    }

    @Override
    public ApplicationProperties readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (decoder.isNull()) {
            return new ApplicationProperties(null);
        }

        return new ApplicationProperties(readMap(stream, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
    }

    @Override
    public ApplicationProperties[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final ApplicationProperties[] result = new ApplicationProperties[count];

        if (decoder.isNull()) {
            for (int i = 0; i < count; ++i) {
                result[i] = new ApplicationProperties(null);
            }
            return result;
        }

        for (int i = 0; i < count; ++i) {
            result[i] = new ApplicationProperties(readMap(stream, state, checkIsExpectedTypeAndCast(MapTypeDecoder.class, decoder)));
        }

        return result;
    }

    private Map<String, Object> readMap(InputStream stream, StreamDecoderState state, MapTypeDecoder mapDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        final int size = mapDecoder.readSize(stream, state);
        final int count = mapDecoder.readCount(stream, state);

        final StreamDecoder decoder = state.getDecoder();

        // Count include both key and value so we must include that in the loop
        final Map<String, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            map.put(decoder.readString(stream, state), decoder.readObject(stream, state));
        }

        return map;
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static ScanningContext<String> createScanContext(String...keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static ScanningContext<String> createScanContext(Collection<String> keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static StreamScanningContext<String> createStreamScanContext(String...keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param keys
     * 		The {@link String} keys that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link ScanningContext} for the collection of string keys.
     */
    public static StreamScanningContext<String> createStreamScanContext(Collection<String> keys) {
        return ProtonScanningContext.createStringScanContext(keys);
    }

    /**
     * Scans through the encoded {@link ApplicationProperties} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded ApplicationProperties have been
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
    public void scanProperties(ProtonBuffer buffer, DecoderState state, ScanningContext<String> context, BiConsumer<String, Object> matchConsumer) throws DecodeException {
        scanMapEntries(buffer, state, context, matchConsumer);
    }

    /**
     * Scans through the encoded {@link ApplicationProperties} map looking for keys that match with
     * the provided {@link ScanningContext}.  When a match is found the provided match consumer
     * is called with the matched key and the decoded value mapped to that key. When the method
     * returns the caller can assume that all bytes of the encoded ApplicationProperties have been
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
    public void scanProperties(InputStream stream, StreamDecoderState state, StreamScanningContext<String> context, BiConsumer<String, Object> matchConsumer) throws DecodeException {
        scanMapEntries(stream, state, context, matchConsumer);
    }
}
