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
package org.apache.qpid.protonj2.codec.decoders.primitives;

import java.io.InputStream;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ScanningContext;
import org.apache.qpid.protonj2.codec.decoders.StreamScanningContext;

/**
 * Base interface for all AMQP Map type value decoders.
 */
@SuppressWarnings("rawtypes")
public interface MapTypeDecoder extends PrimitiveTypeDecoder<Map> {

    @Override
    default Class<Map> getTypeClass() {
        return Map.class;
    }

    /**
     * Reads the count of entries in the encoded Map.
     * <p>
     * This value is the total count of all key values pairs, and should
     * always be an even number as Map types cannot be unbalanced.
     *
     * @param buffer
     *      The buffer containing the encoded Map type.
     * @param state
     * 		The {@link DecoderState} used during this decode.
     *
     * @return the number of elements that we encoded from the original Map.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    int readCount(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads the count of entries in the encoded Map.
     * <p>
     * This value is the total count of all key values pairs, and should
     * always be an even number as Map types cannot be unbalanced.
     *
     * @param stream
     *      The InputStream containing the encoded Map type.
     * @param state
     * 		The {@link StreamDecoderState} used during this decode.
     *
     * @return the number of elements that we encoded from the original Map.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    int readCount(InputStream stream, StreamDecoderState state) throws DecodeException;

    /**
     * Scan the encoded {@link Map} keys matching on predetermined key value encodings to quickly
     * find mappings that are of interest and then only decoding the value portion of the matched
     * key / value pair. This allows for quick checks of incoming {@link Map} types without the
     * performance penalty of a full decode. After the method returns the contexts of the encoded
     * Map in the provided buffer will have been consumed and the next type can be decoded.
     * <p>
     * Each matching key / value mapping triggers a call to the provided {@link BiConsumer} with
     * the key that triggered the match (generally a cached non-decoded value) and the decoded
     * value mapped to that key. The caller should use the consumer to trigger actions based on
     * the matches found in the mappings which avoid full decodings of large maps when only a
     * limited set of values is desired.
     *
     * @param <KeyType>
     * 		The key type is used when calling the match consumer
     * @param buffer
     *      The buffer containing the encoded Map type.
     * @param state
     * 		The {@link DecoderState} used during this decode.
     * @param context
     * 		The previously created and configured {@link ScanningContext}
     * @param matchConsumer
     * 		The consumer that will be notified when a matching key is found.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    <KeyType> void scanKeys(ProtonBuffer buffer, DecoderState state, ScanningContext<KeyType> context, BiConsumer<KeyType, Object> matchConsumer) throws DecodeException;

    /**
     * Scan the encoded {@link Map} keys matching on predetermined key value encodings to quickly
     * find mappings that are of interest and then only decoding the value portion of the matched
     * key / value pair. This allows for quick checks of incoming {@link Map} types without the
     * performance penalty of a full decode. After the method returns the contexts of the encoded
     * Map in the provided stream will have been consumed and the next type can be decoded.
     * <p>
     * Each matching key / value mapping triggers a call to the provided {@link BiConsumer} with
     * the key that triggered the match (generally a cached non-decoded value) and the decoded
     * value mapped to that key. The caller should use the consumer to trigger actions based on
     * the matches found in the mappings which avoid full decodings of large maps when only a
     * limited set of values is desired.
     *
     * @param <KeyType>
     * 		The key type is used when calling the match consumer
     * @param stream
     *      The InputStream containing the encoded Map type.
     * @param state
     * 		The {@link StreamDecoderState} used during this decode.
     * @param context
     * 		The previously created and configured {@link ScanningContext}
     * @param matchConsumer
     * 		The consumer that will be notified when a matching key is found.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    <KeyType> void scanKeys(InputStream stream, StreamDecoderState state, StreamScanningContext<KeyType> context, BiConsumer<KeyType, Object> matchConsumer) throws DecodeException;

}