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
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP ErrorCondition type values from a byte stream.
 */
public final class ErrorConditionTypeDecoder extends AbstractDescribedTypeDecoder<ErrorCondition> {

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
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readErrorCondition(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public ErrorCondition[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        ErrorCondition[] result = new ErrorCondition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readErrorCondition(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private ErrorCondition readErrorCondition(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

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
                    condition = state.getDecoder().readSymbol(buffer, state);
                    break;
                case 1:
                    description = state.getDecoder().readString(buffer, state);
                    break;
                case 2:
                    info = state.getDecoder().readMap(buffer, state);
                    break;
                default:
                    throw new DecodeException("To many entries in ErrorCondition encoding");
            }
        }

        return new ErrorCondition(condition, description, info);
    }

    @Override
    public ErrorCondition readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readErrorCondition(stream, state, (ListTypeDecoder) decoder);
    }

    @Override
    public ErrorCondition[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        ErrorCondition[] result = new ErrorCondition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readErrorCondition(stream, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private ErrorCondition readErrorCondition(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        @SuppressWarnings("unused")
        int size = listDecoder.readSize(stream);
        int count = listDecoder.readCount(stream);

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
                    condition = state.getDecoder().readSymbol(stream, state);
                    break;
                case 1:
                    description = state.getDecoder().readString(stream, state);
                    break;
                case 2:
                    info = state.getDecoder().readMap(stream, state);
                    break;
                default:
                    throw new DecodeException("To many entries in ErrorCondition encoding");
            }
        }

        return new ErrorCondition(condition, description, info);
    }
}
