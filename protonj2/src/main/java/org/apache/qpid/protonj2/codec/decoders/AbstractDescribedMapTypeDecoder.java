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
package org.apache.qpid.protonj2.codec.decoders;

import java.io.InputStream;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.MapTypeDecoder;

/**
 * Abstract base for all List based Described Type decoders which implements the generic methods
 * common to all the implementations.
 *
 * @param <V> The type that this decoder handles.
 */
public abstract class AbstractDescribedMapTypeDecoder<V> extends AbstractDescribedTypeDecoder<V> {

    @Override
    public final void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!decoder.isNull()) {
            checkIsExpectedType(MapTypeDecoder.class, decoder);
            decoder.skipValue(buffer, state);
        }
    }

    @Override
    public final void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (!decoder.isNull()) {
            checkIsExpectedType(MapTypeDecoder.class, decoder);
            decoder.skipValue(stream, state);
        }
    }

    protected <KeyType> void scanMapEntries(ProtonBuffer buffer, DecoderState state, ScanningContext<KeyType> context, BiConsumer<KeyType, Object> matchConsumer) throws DecodeException {
        final TypeDecoder<?> typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (typeDecoder.isNull()) {
            return;
        }

        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, typeDecoder);

        try {
            mapDecoder.scanKeys(buffer, state, context, matchConsumer);
        } finally {
            context.reset();
        }
    }

    protected <KeyType> void scanMapEntries(InputStream stream, StreamDecoderState state, StreamScanningContext<KeyType> context, BiConsumer<KeyType, Object> matchConsumer) throws DecodeException {
        final StreamTypeDecoder<?> typeDecoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (typeDecoder.isNull()) {
            return;
        }

        final MapTypeDecoder mapDecoder = checkIsExpectedTypeAndCast(MapTypeDecoder.class, typeDecoder);

        try {
            mapDecoder.scanKeys(stream, state, context, matchConsumer);
        } finally {
            context.reset();
        }
    }
}
