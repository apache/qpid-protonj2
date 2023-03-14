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
import org.apache.qpid.protonj2.codec.DecodeEOFException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.List32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.List8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Released;

/**
 * Decoder of AMQP Released type values from a byte stream.
 */
public final class ReleasedTypeDecoder extends AbstractDescribedListTypeDecoder<Released> {

    private static final ListTypeDecoder SMALL_LIST_TYPE_DECODER = new List8TypeDecoder();
    private static final ListTypeDecoder LARGE_LIST_TYPE_DECODER = new List32TypeDecoder();

    @Override
    public Class<Released> getTypeClass() {
        return Released.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Released.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Released.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Released readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode;

        try {
            encodingCode = buffer.readByte();
        } catch (IndexOutOfBoundsException e) {
            throw new DecodeEOFException(e);
        }

        switch (encodingCode) {
            case EncodingCodes.LIST0:
                break;
            case EncodingCodes.LIST8:
                SMALL_LIST_TYPE_DECODER.skipValue(buffer, state);
                break;
            case EncodingCodes.LIST32:
                LARGE_LIST_TYPE_DECODER.skipValue(buffer, state);
                break;
            default:
                throw new DecodeException(
                    "Expected list encoding but got decoder for type code: " + encodingCode);
        }

        return Released.getInstance();
    }

    @Override
    public Released[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        final Released[] result = new Released[count];
        for (int i = 0; i < count; ++i) {
            decoder.skipValue(buffer, state);
            result[i] = Released.getInstance();
        }

        return result;
    }

    @Override
    public Released readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);

        return Released.getInstance();
    }

    @Override
    public Released[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        final Released[] result = new Released[count];
        for (int i = 0; i < count; ++i) {
            decoder.skipValue(stream, state);
            result[i] = Released.getInstance();
        }

        return result;
    }
}
