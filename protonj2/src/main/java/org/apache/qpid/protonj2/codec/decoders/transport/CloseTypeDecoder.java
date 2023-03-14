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
package org.apache.qpid.protonj2.codec.decoders.transport;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP Close type values from a byte stream
 */
public final class CloseTypeDecoder extends AbstractDescribedListTypeDecoder<Close> {

    private static final int MIN_CLOSE_LIST_ENTRIES = 0;
    private static final int MAX_CLOSE_LIST_ENTRIES = 1;

    @Override
    public Class<Close> getTypeClass() {
        return Close.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Close.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Close.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Close readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readClose(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Close[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Close[] result = new Close[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readClose(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Close readClose(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Close close = new Close();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        if (count < MIN_CLOSE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Close list encoding: " + count);
        } else if (count > MAX_CLOSE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Close list encoding: " + count);
        } else if (count == 1) {
            close.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
        }

        return close;
    }

    @Override
    public Close readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readClose(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Close[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Close[] result = new Close[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readClose(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Close readClose(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Close close = new Close();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        if (count < MIN_CLOSE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Close list encoding: " + count);
        } if (count > MAX_CLOSE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Close list encoding: " + count);
        } else if (count == 1) {
            close.setError(state.getDecoder().readObject(stream, state, ErrorCondition.class));
        }

        return close;
    }
}
