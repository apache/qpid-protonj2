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
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP ErrorCondition type values from a byte stream.
 */
public final class ErrorConditionTypeDecoder extends AbstractDescribedListTypeDecoder<ErrorCondition> {

    private static final int MIN_ERROR_CONDITION_LIST_ENTRIES = 1;
    private static final int MAX_ERROR_CONDITION_LIST_ENTRIES = 3;

    @Override
    public Class<ErrorCondition> getTypeClass() {
        return ErrorCondition.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return ErrorCondition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ErrorCondition.DESCRIPTOR_SYMBOL;
    }

    @Override
    public ErrorCondition readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readErrorCondition(buffer, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public ErrorCondition[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final ErrorCondition[] result = new ErrorCondition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readErrorCondition(buffer, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private ErrorCondition readErrorCondition(ProtonBuffer buffer, Decoder decoder, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_ERROR_CONDITION_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in ErrorCondition list encoding: " + count);
        }
        if (count > MAX_ERROR_CONDITION_LIST_ENTRIES) {
            throw new DecodeException("To many entries in ErrorCondition list encoding: " + count);
        }

        Symbol condition = null;
        String description = null;
        Map<Symbol, Object> info = null;

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    condition = decoder.readSymbol(buffer, state);
                    break;
                case 1:
                    description = decoder.readString(buffer, state);
                    break;
                case 2:
                    info = decoder.readMap(buffer, state);
                    break;
            }
        }

        return new ErrorCondition(condition, description, info);
    }

    @Override
    public ErrorCondition readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readErrorCondition(stream, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public ErrorCondition[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final ErrorCondition[] result = new ErrorCondition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readErrorCondition(stream, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private ErrorCondition readErrorCondition(InputStream stream, StreamDecoder decoder, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_ERROR_CONDITION_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in ErrorCondition list encoding: " + count);
        }
        if (count > MAX_ERROR_CONDITION_LIST_ENTRIES) {
            throw new DecodeException("To many entries in ErrorCondition list encoding: " + count);
        }

        Symbol condition = null;
        String description = null;
        Map<Symbol, Object> info = null;

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    condition = decoder.readSymbol(stream, state);
                    break;
                case 1:
                    description = decoder.readString(stream, state);
                    break;
                case 2:
                    info = decoder.readMap(stream, state);
                    break;
            }
        }

        return new ErrorCondition(condition, description, info);
    }
}
