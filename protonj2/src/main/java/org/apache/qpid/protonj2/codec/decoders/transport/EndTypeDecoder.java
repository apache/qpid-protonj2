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
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.End;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP End type values from a byte stream
 */
public final class EndTypeDecoder extends AbstractDescribedTypeDecoder<End> {

    private static final int MIN_END_LIST_ENTRIES = 0;
    private static final int MAX_END_LIST_ENTRIES = 1;

    @Override
    public Class<End> getTypeClass() {
        return End.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return End.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return End.DESCRIPTOR_SYMBOL;
    }

    @Override
    public End readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readEnd(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public End[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        End[] result = new End[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readEnd(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private End readEnd(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        End end = new End();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_END_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in End list encoding: " + count);
        }

        if (count > MAX_END_LIST_ENTRIES) {
            throw new DecodeException("To many entries in End list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    end.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
                    break;
                default:
                    throw new DecodeException("To many entries in End encoding");
            }
        }

        return end;
    }

    @Override
    public End readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readEnd(stream, state, (ListTypeDecoder) decoder);
    }

    @Override
    public End[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        End[] result = new End[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readEnd(stream, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private End readEnd(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        End end = new End();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(stream);
        int count = listDecoder.readCount(stream);

        if (count < MIN_END_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in End list encoding: " + count);
        }

        if (count > MAX_END_LIST_ENTRIES) {
            throw new DecodeException("To many entries in End list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    end.setError(state.getDecoder().readObject(stream, state, ErrorCondition.class));
                    break;
                default:
                    throw new DecodeException("To many entries in End encoding");
            }
        }

        return end;
    }
}
